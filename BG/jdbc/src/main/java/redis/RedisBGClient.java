package redis;

import static mongodb.ReconConfig.CREATE_SCHEMA;
import static mongodb.ReconConfig.KEY_ACTION;
import static mongodb.ReconConfig.KEY_ALPHA_POLICY;
import static mongodb.ReconConfig.KEY_BASE_ALPHA;
import static mongodb.ReconConfig.KEY_DB_FAILED;
import static mongodb.ReconConfig.KEY_NUM_AR_WORKER;
import static mongodb.ReconConfig.LOAD_DATA;
import static mongodb.ReconConfig.RUN;
import static edu.usc.bg.workloads.CoreWorkload.DATABASE_FAILURE;
import static tardis.TardisClientConfig.KEY_LIST_FRIENDS;
import static tardis.TardisClientConfig.KEY_LIST_FRIENDS_REQUEST;
import static tardis.TardisClientConfig.KEY_VIEW_PROFILE;
import static tardis.TardisClientConfig.LEASE_TIMEOUT;
import static tardis.TardisClientConfig.PAGE_SIZE_FRIEND_LIST;
import static tardis.TardisClientConfig.getEWLogMutationLeaseKeyFromUserId;
import static tardis.TardisClientConfig.getUserLogLeaseKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.bson.Document;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;
import edu.usc.bg.workloads.CoreWorkload;
import mongodb.ArrayMetrics;
import mongodb.CacheUtilities;
import mongodb.DatabaseFailureRecord;
import mongodb.MetricsHelper;
import mongodb.MetricsName;
import mongodb.MongoBGClient;
import mongodb.MongoBGClientDelegate;
import mongodb.ReconConfig;
import mongodb.RecoveryCaller;
import mongodb.RecoveryResult;
import mongodb.TimedMetrics;
import mongodb.alpha.AlphaDynamicPolicy;
import mongodb.alpha.AlphaPolicy;
import mongodb.alpha.AlphaPolicy.AlphaPolicyEnum;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;
import tardis.TardisClientConfig;

public class RedisBGClient extends DB {

	HashShardedJedis redisClient;
	RedisLease leaseClient;

	RedisRecoveryEngine recovery;
	static MongoBGClient mongoClient;
	static RedisDatabaseStateSimulator databaseStateSimulator;
	static List<RedisActiveRecoveryWorker> activeRecoveryWorker = new ArrayList<>();
	static RedisEWStatsWatcher ewStatsWatcher;
	static long start = System.currentTimeMillis();

	static final AtomicBoolean isDatabaseFailed = new AtomicBoolean(false);

	final Logger logger = Logger.getLogger(RedisBGClient.class);
	static AtomicBoolean initialized = new AtomicBoolean(false);
	static final Semaphore initializeLock = new Semaphore(1);
	static final AtomicBoolean canClean = new AtomicBoolean(false);
	static AtomicInteger threads = new AtomicInteger(0);
	static AtomicInteger viewProfileExecuted = new AtomicInteger(0);

	static final ExecutorService executor = Executors.newFixedThreadPool(120);

	public static final String ZERO_FRIEND = "t";

	final byte[] readBuffer = new byte[1024 * 5];
	final List<String> luaKeys = new ArrayList<>();
	final List<String> luaArgs = new ArrayList<>();

	@Override
	public boolean init() throws DBException {
		java.util.logging.Logger mongoLogger = java.util.logging.Logger.getLogger("org.mongodb.driver");
		mongoLogger.setLevel(java.util.logging.Level.SEVERE);

		if (getProperties().getProperty("threadcount") != null) {
			threads.set(Integer.parseInt(getProperties().getProperty("threadcount")));
		}

		if (getProperties().getProperty(ReconConfig.KEY_NUM_USER_IN_CACHE) != null) {
			TardisClientConfig.numUserInCache = Integer
					.parseInt(getProperties().getProperty(ReconConfig.KEY_NUM_USER_IN_CACHE));
		}

		if (getProperties().getProperty(ReconConfig.KEY_MONITOR_LOST_WRITE) != null) {
			TardisClientConfig.monitorLostWrite = Boolean
					.parseBoolean(getProperties().getProperty(ReconConfig.KEY_MONITOR_LOST_WRITE));
		}

		if (getProperties().getProperty(ReconConfig.KEY_TWEMCACHED_IP) == null) {
			throw new RuntimeException("twemcached Ip not specified");
		}

		if (getProperties().getProperty(MongoBGClient.KEY_MONGO_DB_IP) == null) {
			throw new RuntimeException("mongo Ip not specified");
		}

		if (getProperties().getProperty("enablelogging") != null) {
			TardisClientConfig.supportPaginatedFriends = !Boolean
					.parseBoolean(getProperties().getProperty("enablelogging"));
		}

		if (getProperties().getProperty("measurelostwritefailure") != null) {
			TardisClientConfig.measureLostWriteAfterFailure = Boolean
					.parseBoolean(getProperties().getProperty("measurelostwritefailure"));
		}

		if (getProperties().getProperty("ew") != null) {
			TardisClientConfig.NUM_EVENTUAL_WRITE_LOGS = Integer.parseInt(getProperties().getProperty("ew"));
		}

		String urls = getProperties().getProperty(ReconConfig.KEY_TWEMCACHED_IP);
		
		if (!urls.contains(",")) {
			redisClient = getShardedJedis(urls);
		} else {
			redisClient = getShardedJedis(urls.split(","));
		}
		
		leaseClient = new RedisLease(redisClient, "App");

		initStaticResources(urls.split(","));

		recovery = new RedisRecoveryEngine(redisClient, mongoClient);
		System.out.println("### initialized ");
		return true;
	}

	private HashShardedJedis getShardedJedis(String... urls) {
		return new HashShardedJedis(urls);
	}

	private void initStaticResources(String... urls) {
		System.out.println("### init");
		try {

			initializeLock.acquire();
			if (initialized.get()) {
				initializeLock.release();
				return;
			}

			loadScripts();

			System.out.println("########### version 14");

			BasicConfigurator.configure();

			ConsoleAppender console = new ConsoleAppender(); // create appender
			// configure the appender
			String PATTERN = "%d [%p|%c|%C{1}] %m%n";
			console.setLayout(new PatternLayout(PATTERN));
			console.setThreshold(Level.ERROR);
			console.activateOptions();
			// add appender to any Logger (here is root)
			Logger.getRootLogger().addAppender(console);

			Logger.getLogger(MongoBGClientDelegate.class).setLevel(Level.ERROR);
			Logger.getLogger(MongoBGClient.class).setLevel(Level.OFF);
			Logger.getLogger(RedisRecoveryEngine.class).setLevel(Level.ERROR);
			Logger.getLogger(CoreWorkload.class).setLevel(Level.INFO);
			Logger.getLogger(RedisActiveRecoveryWorker.class).setLevel(Level.ERROR);
			Logger.getLogger(RedisEWStatsWatcher.class).setLevel(Level.OFF);
			Logger.getLogger(RedisBGClient.class).setLevel(Level.ERROR);

			getProperties().forEach((k, v) -> {
				System.out.println("key " + k + " value " + v);
			});

			if (getProperties().getProperty(ReconConfig.KEY_FULL_WARM_UP) != null) {
				TardisClientConfig.fullWarmUp = Boolean
						.parseBoolean(getProperties().getProperty(ReconConfig.KEY_FULL_WARM_UP));
			}

			mongoClient = new MongoBGClientDelegate(getProperties().getProperty(MongoBGClient.KEY_MONGO_DB_IP),
					isDatabaseFailed);
			mongoClient.init();

			String intervals = getProperties().getProperty(KEY_DB_FAILED);
			if (intervals != null) {
				String[] ints = intervals.split(",");
				long[] longIntervals = new long[ints.length];
				for (int i = 0; i < ints.length; i++) {
					longIntervals[i] = Long.parseLong(ints[i]);
				}

				databaseStateSimulator = new RedisDatabaseStateSimulator(getShardedJedis(urls), isDatabaseFailed,
						longIntervals);

				for (int i = 0; i < longIntervals.length; i += 2) {
					ArrayMetrics.getInstance().add(MetricsName.METRICS_DATABASE_DOWN_TIME,
							new DatabaseFailureRecord(longIntervals[i], longIntervals[i + 1]));
				}
				executor.submit(databaseStateSimulator);
			}

			String numARWorker = getProperties().getProperty(KEY_NUM_AR_WORKER);
			String baseAlpha = getProperties().getProperty(KEY_BASE_ALPHA);
			String alphaPolicyStr = getProperties().getProperty(KEY_ALPHA_POLICY);

			if (numARWorker != null) {
				AlphaPolicy alphaPolicy = null;
				switch (AlphaPolicyEnum.fromString(alphaPolicyStr)) {
				case DYNAMIC:
					alphaPolicy = new AlphaDynamicPolicy(Integer.parseInt(baseAlpha));
					break;
				case STATIC:
					alphaPolicy = new AlphaPolicy(Integer.parseInt(baseAlpha));
					break;
				default:
					break;
				}
				for (int i = 0; i < Integer.parseInt(numARWorker); i++) {
					HashShardedJedis client = getShardedJedis(urls);
					RedisLease lease = new RedisLease(client, "AR");
					RedisRecoveryEngine recovery = new RedisRecoveryEngine(client, mongoClient);
					activeRecoveryWorker.add(new RedisActiveRecoveryWorker(isDatabaseFailed, client, lease, recovery,
							alphaPolicy, Integer.parseInt(numARWorker)));
				}
			}

			ewStatsWatcher = new RedisEWStatsWatcher(getShardedJedis(urls));

			executor.submit(ewStatsWatcher);

			activeRecoveryWorker.forEach(i -> {
				executor.submit(i);
			});

			initialized.set(true);
			initializeLock.release();
		} catch (Exception e) {
			logger.error("Initialize failed", e);
		}
	}

	public void loadScripts() {

		System.out.println("load scripts");

		RedisLuaScripts.luaScripts.forEach(v -> {
			redisClient.scriptLoad(v);
			System.out.println("loading " + v);
		});
	}

	@Override
	public void cleanup(boolean warmup) throws DBException {
		if (TardisClientConfig.fullWarmUp) {
			if (!mongoClient.getFriendList().isEmpty()) {
				mongoClient.getFriendList().forEach((id, friends) -> {
					redisClient.sadd(id, KEY_LIST_FRIENDS + id, ZERO_FRIEND);
					redisClient.sadd(id, KEY_LIST_FRIENDS + id, friends.toArray(new String[0]));
				});
				mongoClient.bulkWriteFriends();
			}
			System.out.println("inserted users");
		} else if (TardisClientConfig.numUserInCache != -1) {
			if (!mongoClient.getFriendList().isEmpty()) {
				for (int i = 0; i < TardisClientConfig.numUserInCache; i++) {
					Set<String> friends = mongoClient.getFriendList().get(i);
					redisClient.sadd(i, KEY_LIST_FRIENDS + i, ZERO_FRIEND);
					redisClient.sadd(i, KEY_LIST_FRIENDS + i, friends.toArray(new String[0]));
				}
				mongoClient.bulkWriteFriends();
			}
		} else {
			if (!mongoClient.getFriendList().isEmpty()) {
				mongoClient.bulkWriteFriends();
			}
		}

		try {
			System.out.println("### cleanup");

			// TODO: find a better way to check experiment finished
			if (getProperties().getProperty(KEY_ACTION).equals(RUN)) {

				if (viewProfileExecuted.get() >= 1000 || threads.get() == 0) {
					canClean.set(true);
				}
			}

			if (!canClean.get()) {
				return;
			}

			if (threads.decrementAndGet() > 0) {
				return;
			}

			System.out.println("####!!!! cleaned up ");

			if (getProperties().getProperty(KEY_ACTION).equals(RUN)) {
//				String metricsFile = getProperties().getProperty(ReconConfig.KEY_METRICS_FILE_NAME);
				// ideally, this should equal to 0
//				int pws = RedisRecoveryEngine.measureLostWrites(redisClient);
//				MetricsHelper.numberOfPendingWritesAttheEndOfRecoveryMode.set(pws);

//				if (databaseStateSimulator != null && TardisClientConfig.enableMetrics) {
//					MetricsHelper.writeMetrics(metricsFile, getProperties(), start, System.currentTimeMillis(),
//							databaseStateSimulator.getInvtervals()[1] * 1000, TimedMetrics.getInstance(),
//							ArrayMetrics.getInstance());
//				}

			}

			if (databaseStateSimulator != null) {
				databaseStateSimulator.shutdown();
			}
			ewStatsWatcher.shutdown();
			activeRecoveryWorker.forEach(i -> {
				i.shutdown();
			});

			executor.shutdown();

			executor.awaitTermination(10, TimeUnit.SECONDS);

			mongoClient.cleanup(warmup);
			redisClient.close();

		} catch (Exception e) {
			logger.error("clean up failed", e);
		}
		super.cleanup(warmup);
	}

	@Override
	public Properties getProperties() {
		return super.getProperties();
	}

	@Override
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values,
			boolean insertImage) {

		try {
			if (TardisClientConfig.fullWarmUp) {
				redisClient.sadd(Integer.parseInt(entityPK), KEY_LIST_FRIENDS + entityPK, ZERO_FRIEND);
				redisClient.sadd(Integer.parseInt(entityPK), KEY_LIST_FRIENDS_REQUEST + entityPK, ZERO_FRIEND);
				redisClient.set(Integer.parseInt(entityPK), (KEY_VIEW_PROFILE + entityPK).getBytes(),
						CacheUtilities.SerializeHashMap(values));
			}

			if (TardisClientConfig.numUserInCache != -1 && !TardisClientConfig.fullWarmUp) {
				if (Integer.parseInt(entityPK) < TardisClientConfig.numUserInCache) {
					redisClient.sadd(Integer.parseInt(entityPK), KEY_LIST_FRIENDS + entityPK, ZERO_FRIEND);
					redisClient.sadd(Integer.parseInt(entityPK), KEY_LIST_FRIENDS_REQUEST + entityPK, ZERO_FRIEND);
					redisClient.set(Integer.parseInt(entityPK), (KEY_VIEW_PROFILE + entityPK).getBytes(),
							CacheUtilities.SerializeHashMap(values));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return mongoClient.insertEntity(entitySet, entityPK, values, insertImage);
	}

	@Override
	public void buildIndexes(Properties props) {
		super.buildIndexes(props);
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
			boolean insertImage, boolean testMode) {
		logger.debug("view profile ru" + profileOwnerID + "ur");
		if (viewProfileExecuted.get() <= 1000) {
			viewProfileExecuted.incrementAndGet();
		}

		luaKeys.clear();
		luaKeys.add(KEY_VIEW_PROFILE + profileOwnerID);
		luaKeys.add(KEY_LIST_FRIENDS + profileOwnerID);
		luaKeys.add(KEY_LIST_FRIENDS_REQUEST + profileOwnerID);
		List<?> list = (List<?>) redisClient.evalsha(profileOwnerID, RedisLuaScripts.VIEW_PROFILE, luaKeys,
				Collections.emptyList());

		if (list.get(0) != null && (Long) list.get(1) > 0 && (Long) list.get(2) > 0) {

			result.put("userid", new ObjectByteIterator(String.valueOf(profileOwnerID).getBytes()));
			CacheUtilities.unMarshallHashMap(result, String.valueOf(list.get(0)).getBytes(), readBuffer);
			result.put("friendcount", new ObjectByteIterator(String.valueOf((Long) list.get(1) - 1).getBytes()));
			if (requesterID == profileOwnerID) {
				result.put("pendingcount", new ObjectByteIterator(String.valueOf((Long) list.get(2) - 1).getBytes()));
			}
			return 0;
		} else {
			if (TardisClientConfig.fullWarmUp) {
				logger.fatal("cache miss. existing keys ");
			}
			return recoverViewProfile(requesterID, profileOwnerID, result, insertImage, testMode);
		}

	}

	@SuppressWarnings("unchecked")
	private int recoverViewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
			boolean insertImage, boolean testMode) {
		if (isDatabaseFailed.get()) {
			return DATABASE_FAILURE;
		}

		try {

			leaseClient.acquireTillSuccess(profileOwnerID, getUserLogLeaseKey(profileOwnerID), LEASE_TIMEOUT);

			if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.READ, profileOwnerID, luaKeys))) {
				// view the user's profile.
				try {
					Document doc = mongoClient.viewProfile(requesterID, profileOwnerID, result);
					List<String> friends = doc.get(MongoBGClient.KEY_FRIEND, List.class);
					List<String> pendingFriends = doc.get(MongoBGClient.KEY_PENDING, List.class);

					result.put("friendcount", new ObjectByteIterator(String.valueOf(friends.size()).getBytes()));
					if (requesterID == profileOwnerID) {
						result.put("pendingcount",
								new ObjectByteIterator(String.valueOf(pendingFriends.size()).getBytes()));
					}

					Pipeline pipe = redisClient.getJedis(profileOwnerID).pipelined();

					if (friends == null || friends.isEmpty()) {
						pipe.sadd(KEY_LIST_FRIENDS + profileOwnerID, ZERO_FRIEND);
					} else {
						friends.add(ZERO_FRIEND);
						pipe.sadd(KEY_LIST_FRIENDS + profileOwnerID, friends.toArray(new String[0]));
					}

					if (requesterID == profileOwnerID) {
						if (pendingFriends == null || pendingFriends.isEmpty()) {
							pipe.sadd(KEY_LIST_FRIENDS_REQUEST + profileOwnerID, ZERO_FRIEND);
						} else {
							pendingFriends.add(ZERO_FRIEND);
							pipe.sadd(KEY_LIST_FRIENDS_REQUEST + profileOwnerID, pendingFriends.toArray(new String[0]));
						}
					}
					pipe.set((KEY_VIEW_PROFILE + profileOwnerID).getBytes(), CacheUtilities.SerializeHashMap(result));
					pipe.sync();
				} catch (Exception e) {
					e.printStackTrace();
					return DATABASE_FAILURE;
				}

			} else {
				return DATABASE_FAILURE;
			}
		} catch (Exception e) {
			logger.error("Recover user profile failed " + profileOwnerID, e);
		} finally {
			leaseClient.releaseLease(profileOwnerID, getUserLogLeaseKey(profileOwnerID), luaKeys);
		}
		return 0;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int listFriends(int requesterID, int profileOwnerID, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> results, boolean insertImage, boolean testMode) {

		if (viewProfileExecuted.get() <= 1000) {
			viewProfileExecuted.incrementAndGet();
		}

		String key = KEY_LIST_FRIENDS + profileOwnerID;

		luaKeys.clear();
		luaKeys.add(key);
		luaKeys.add(String.valueOf(PAGE_SIZE_FRIEND_LIST));
		luaKeys.add(KEY_VIEW_PROFILE);
		luaKeys.add(KEY_LIST_FRIENDS);
		
		luaArgs.clear();
		luaArgs.add(String.valueOf(profileOwnerID));
		luaArgs.add(String.valueOf(redisClient.size()));

		List<List<?>> friendsList = (List<List<?>>) redisClient.evalsha(profileOwnerID, RedisLuaScripts.LIST_FRIENDS,
				luaKeys, luaArgs);

		List<String> queryProfileFriends = new ArrayList<>();
		
		if (friendsList != null) {
			// cache hit on friends list.
			// local friends. 
			for (int i = 0; i < friendsList.get(0).size(); i++) {
				if (friendsList.get(1).get(i) == null || (Long) friendsList.get(2).get(i) == 0) {
					queryProfileFriends.add(String.valueOf(friendsList.get(0).get(i)));
				} else {
					HashMap<String, ByteIterator> result = new HashMap<>();
					result.put("userid", new ObjectByteIterator(String.valueOf(friendsList.get(0).get(i)).getBytes()));
					CacheUtilities.unMarshallHashMap(result, String.valueOf(friendsList.get(1).get(i)).getBytes(),
							readBuffer);
					result.put("friendcount",
							new ObjectByteIterator(String.valueOf((Long) friendsList.get(2).get(i) - 1).getBytes()));
					results.add(result);
				}
			}
			
			if (friendsList.get(0).size() < PAGE_SIZE_FRIEND_LIST) {
				for (int i = 0; i < friendsList.get(3).size(); i++) {
					queryProfileFriends.add(String.valueOf(friendsList.get(3).get(i)));
				}
			}

			if (queryProfileFriends.isEmpty()) {
				return 1;
			} else if (TardisClientConfig.fullWarmUp && redisClient.size() == 1) {
				logger.fatal("missing friends !!!!!!" + profileOwnerID);
			}
		}

		logger.debug("list ru" + profileOwnerID + "ur " + results.size());

		logger.debug("list friends ru" + profileOwnerID + "ur missed " + queryProfileFriends);

		if (friendsList == null) {
			// cache miss on friend list.
			if (TardisClientConfig.fullWarmUp) {
				logger.fatal("BUG: cache miss list friends ru" + profileOwnerID);
			}
			// cache miss
			if (isDatabaseFailed.get()) {
				return DATABASE_FAILURE;
			} else {
				try {
					leaseClient.acquireTillSuccess(profileOwnerID, getUserLogLeaseKey(profileOwnerID), LEASE_TIMEOUT);

					if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.READ, profileOwnerID, luaKeys))) {
						queryProfileFriends = mongoClient.listFriends(profileOwnerID);

						logger.debug("list friends ru" + profileOwnerID + "ur db " + queryProfileFriends);

						if (queryProfileFriends == null) {
							return DATABASE_FAILURE;
						} else {
							queryProfileFriends.add(ZERO_FRIEND);
							redisClient.sadd(profileOwnerID, key, queryProfileFriends.toArray(new String[0]));
							queryProfileFriends.remove(ZERO_FRIEND);
						}
					} else {
						return DATABASE_FAILURE;
					}
				} catch (Exception e) {
					logger.error("list friends failed " + profileOwnerID, e);
				} finally {
					leaseClient.releaseLease(profileOwnerID, getUserLogLeaseKey(profileOwnerID), luaKeys);
				}
			}
		}

		int neededFriends = queryProfileFriends.size();
		if (TardisClientConfig.supportPaginatedFriends) {
			neededFriends = Integer.min(PAGE_SIZE_FRIEND_LIST - results.size(), queryProfileFriends.size());
		}

		if (viewFriendsProfile(profileOwnerID, queryProfileFriends, results, neededFriends) == DATABASE_FAILURE) {
			return DATABASE_FAILURE;
		}
		
		return 0;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {

		if (viewProfileExecuted.get() <= 1000) {
			viewProfileExecuted.incrementAndGet();
		}

		String key = KEY_LIST_FRIENDS_REQUEST + profileOwnerID;

		luaKeys.clear();
		luaKeys.add(key);
		luaKeys.add(String.valueOf(PAGE_SIZE_FRIEND_LIST));
		luaKeys.add(KEY_VIEW_PROFILE);
		luaKeys.add(KEY_LIST_FRIENDS);
		
		luaArgs.clear();
		luaArgs.add(String.valueOf(profileOwnerID));
		luaArgs.add(String.valueOf(redisClient.size()));

		List<List<?>> friendsList = (List<List<?>>) redisClient.evalsha(profileOwnerID, RedisLuaScripts.LIST_FRIENDS,
				luaKeys, luaArgs);

		List<String> queryProfileFriends = new ArrayList<>();

		if (friendsList != null) {
			// cache hit on friends list.
			// local friends. 
			for (int i = 0; i < friendsList.get(0).size(); i++) {
				if (friendsList.get(1).get(i) == null || (Long) friendsList.get(2).get(i) == 0) {
					queryProfileFriends.add(String.valueOf(friendsList.get(0).get(i)));
				} else {
					HashMap<String, ByteIterator> result = new HashMap<>();
					result.put("userid", new ObjectByteIterator(String.valueOf(friendsList.get(0).get(i)).getBytes()));
					CacheUtilities.unMarshallHashMap(result, String.valueOf(friendsList.get(1).get(i)).getBytes(),
							readBuffer);
					result.put("friendcount",
							new ObjectByteIterator(String.valueOf((Long) friendsList.get(2).get(i) - 1).getBytes()));
					results.add(result);
				}
			}
			
			if (friendsList.get(0).size() < PAGE_SIZE_FRIEND_LIST && !friendsList.get(3).isEmpty()) {
				for (int i = 0; i < friendsList.get(3).size(); i++) {
					queryProfileFriends.add(String.valueOf(friendsList.get(3).get(i)));
				}
			}

			if (queryProfileFriends.isEmpty()) {
				return 1;
			} else if (TardisClientConfig.fullWarmUp && redisClient.size() == 1) {
				logger.fatal("missing friends !!!!!!" + profileOwnerID);
			}

			
		}

		logger.debug("list ru" + profileOwnerID + "ur " + results.size());

		logger.debug("list friends ru" + profileOwnerID + "ur missed " + queryProfileFriends);

		if (friendsList == null) {
			// cache miss on friend list.
			if (TardisClientConfig.fullWarmUp) {
				logger.fatal("BUG: cache miss list friends ru" + profileOwnerID);
			}
			// cache miss
			if (isDatabaseFailed.get()) {
				return DATABASE_FAILURE;
			} else {
				try {
					leaseClient.acquireTillSuccess(profileOwnerID, getUserLogLeaseKey(profileOwnerID), LEASE_TIMEOUT);

					if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.READ, profileOwnerID, luaKeys))) {
						queryProfileFriends = mongoClient.listPendingFriends(profileOwnerID);

						logger.debug("list friends request ru" + profileOwnerID + "ur db " + queryProfileFriends);

						if (queryProfileFriends == null) {
							return DATABASE_FAILURE;
						} else {
							queryProfileFriends.add(ZERO_FRIEND);
							redisClient.sadd(profileOwnerID, key, queryProfileFriends.toArray(new String[0]));
							queryProfileFriends.remove(ZERO_FRIEND);
						}
					} else {
						return DATABASE_FAILURE;
					}
				} catch (Exception e) {
					logger.error("list friends request failed " + profileOwnerID, e);
				} finally {
					leaseClient.releaseLease(profileOwnerID, getUserLogLeaseKey(profileOwnerID), luaKeys);
				}
			}
		}

		int neededFriends = queryProfileFriends.size();
		if (TardisClientConfig.supportPaginatedFriends) {
			neededFriends = Integer.min(PAGE_SIZE_FRIEND_LIST - results.size(), queryProfileFriends.size());
		}

		if (viewFriendsProfile(profileOwnerID, queryProfileFriends, results, neededFriends) == DATABASE_FAILURE) {
			return DATABASE_FAILURE;
		}
		return 0;
	}

	private int viewFriendsProfile(int profileOwnerId, Collection<String> friends,
			Vector<HashMap<String, ByteIterator>> results, int neededFriends) {

		if (friends.size() == 0 || neededFriends == 0) {
			return 0;
		}

		Map<Integer, List<String>> serverFriendsMap = new HashMap<>();
		int profileOwnerServer = redisClient.getServerIndex(profileOwnerId);
		friends.forEach(friend -> {
			int friendServer = redisClient.getServerIndex(Integer.parseInt(friend));
			List<String> fs = serverFriendsMap.get(friendServer);
			if (fs == null) {
				fs = new ArrayList<>();
				serverFriendsMap.put(friendServer, fs);
			}
			fs.add(friend);
		});

		// first favor retrieve friends on the same server of the profile owner.
		// then go to others if necessary
		List<String> profileOwnerFriends = serverFriendsMap.remove(profileOwnerServer);
		if (profileOwnerFriends != null) {
			for (String friend : profileOwnerFriends) {
				HashMap<String, ByteIterator> result = new HashMap<>();
				if (TardisClientConfig.fullWarmUp) {
					logger.fatal("BUG: cache miss list users profile " + friend);
				}

				if (recoverViewProfile(-1, Integer.parseInt(String.valueOf(friend)), result, false,
						false) == DATABASE_FAILURE) {
					return DATABASE_FAILURE;
				}
				results.add(result);
				neededFriends--;
				if (TardisClientConfig.supportPaginatedFriends) {
					if (neededFriends == 0) {
						return 0;
					}
				}
			}
		}

		List<Entry<Integer, List<String>>> sortedList = serverFriendsMap.entrySet().stream()
				.sorted(Comparator.comparingInt(e -> -1 * e.getValue().size())).collect(Collectors.toList());
		
		if (redisClient.size() == 1 && !sortedList.isEmpty()) {
			logger.fatal("BUG: single server, but has remote friends!!! " + profileOwnerId);
		}
		
		for (Entry<Integer, List<String>> entry : sortedList) {
			
			Map<String, Response<?>> profiles = new HashMap<String, Response<?>>();
			List<String> friendsServer = entry.getValue();

			if (friendsServer.size() > neededFriends) {
				friendsServer = friendsServer.stream().limit(neededFriends).collect(Collectors.toList());
			}

			Pipeline pipe = redisClient.getJedisByServerId(entry.getKey()).pipelined();
			friendsServer.forEach(i -> {
				profiles.put(KEY_VIEW_PROFILE + i, pipe.get((KEY_VIEW_PROFILE + i).getBytes()));
				profiles.put(KEY_LIST_FRIENDS + i, pipe.scard(KEY_LIST_FRIENDS + i));
			});
			pipe.sync();

			for (String friend : friendsServer) {
				HashMap<String, ByteIterator> result = new HashMap<>();
				if (profiles.get(KEY_VIEW_PROFILE + friend).get() != null
						&& (Long) profiles.get(KEY_LIST_FRIENDS + friend).get() != 0) {
					result.put("userid", new ObjectByteIterator(String.valueOf(friend).getBytes()));
					CacheUtilities.unMarshallHashMap(result, (byte[]) profiles.get(KEY_VIEW_PROFILE + friend).get(),
							readBuffer);
					result.put("friendcount", new ObjectByteIterator(
							String.valueOf((Long) profiles.get(KEY_LIST_FRIENDS + friend).get() - 1).getBytes()));
				} else {
					if (TardisClientConfig.fullWarmUp) {
						logger.fatal("BUG: cache miss list users profile " + friend);
					}

					if (recoverViewProfile(-1, Integer.parseInt(String.valueOf(friend)), result, false,
							false) == DATABASE_FAILURE) {
						return DATABASE_FAILURE;
					}
				}
				results.add(result);
				neededFriends -= 1;
			}

		}
		return 0;
	}

	private int acceptFriendInviter(int inviterID, int inviteeID) {
		boolean updateInviterFailed = false;
		boolean cacheUpdateFailed = false;
		boolean firstTimeDirty = false;

		try {

			leaseClient.acquireTillSuccess(inviterID, getUserLogLeaseKey(inviterID), LEASE_TIMEOUT);

			luaKeys.clear();
			luaKeys.add(KEY_LIST_FRIENDS + inviterID);
			luaKeys.add(String.valueOf(inviteeID));

			if ((long) redisClient.evalsha(inviterID, RedisLuaScripts.SADD_IF_EXIST, luaKeys,
					Collections.emptyList()) == 0) {
				cacheUpdateFailed = true;
			}

			if (!isDatabaseFailed.get()) {
				if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, inviterID, luaKeys))) {
					int ret = mongoClient.acceptFriendInviter(inviterID, inviteeID);
					if (ret == DATABASE_FAILURE) {
						updateInviterFailed = true;
					}
				} else {
					updateInviterFailed = true;
				}
			} else {
				updateInviterFailed = true;
			}

			if (updateInviterFailed) {
				if (cacheUpdateFailed || TardisClientConfig.monitorLostWrite) {
					firstTimeDirty = !recovery.appendFriendActionToLog(inviterID, inviteeID, true, luaKeys);
				} else {
					firstTimeDirty = recovery.addDirtyFlag(inviterID, luaKeys);
				}
			}
			logger.debug("update database " + updateInviterFailed + " update cache " + cacheUpdateFailed);

		} catch (Exception e) {
			logger.error("Accept friend, update inviter failed " + inviterID + ", " + inviteeID, e);
		} finally {
			leaseClient.releaseLease(inviterID, getUserLogLeaseKey(inviterID), luaKeys);
			if (updateInviterFailed) {
				insertToEWList(firstTimeDirty, inviterID);
			}
		}
		return 0;
	}

	private int acceptFriendInvitee(int inviterID, int inviteeID) {
		boolean updateInviteeFailed = false;
		boolean cacheUpdateFailed = false;
		boolean firstTimeDirty = false;
		try {
			leaseClient.acquireTillSuccess(inviteeID, getUserLogLeaseKey(inviteeID), LEASE_TIMEOUT);

			luaKeys.clear();
			luaKeys.add(KEY_LIST_FRIENDS + inviteeID);
			luaKeys.add(String.valueOf(inviterID));
			luaKeys.add(KEY_LIST_FRIENDS_REQUEST + inviteeID);
			luaKeys.add(String.valueOf(inviterID));

			if ((long) redisClient.evalsha(inviteeID, RedisLuaScripts.ACCEPT_FRIEND_INVITEE, luaKeys,
					Collections.emptyList()) == 0) {
				cacheUpdateFailed = true;
			}

			if (!isDatabaseFailed.get()) {
				if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, inviteeID, luaKeys))) {
					int ret = mongoClient.acceptFriendInvitee(inviterID, inviteeID);
					if (ret == DATABASE_FAILURE) {
						updateInviteeFailed = true;
					}
				} else {
					updateInviteeFailed = true;
				}
			} else {
				updateInviteeFailed = true;
			}

			if (updateInviteeFailed) {
				if (cacheUpdateFailed || TardisClientConfig.monitorLostWrite) {
					// if append success, it means key exists. So it is not the
					// first time.
					firstTimeDirty = !recovery.appendFriendActionToLog(inviteeID, inviterID, true, luaKeys);
					recovery.appendPendingFriendActionToLog(inviteeID, inviterID, false, luaKeys);
				} else {
					// if add success, it means it is the first time.
					firstTimeDirty = recovery.addDirtyFlag(inviteeID, luaKeys);
				}
			}

			logger.debug("update database " + updateInviteeFailed + " update cache " + cacheUpdateFailed);
		} catch (Exception e) {
			logger.error("Accept friend, update invitee failed " + inviterID + ", " + inviteeID, e);
		} finally {
			leaseClient.releaseLease(inviteeID, getUserLogLeaseKey(inviteeID), luaKeys);
			if (updateInviteeFailed) {
				insertToEWList(firstTimeDirty, inviteeID);
			}
		}
		return 0;
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {

		logger.debug("accept friend wu" + inviterID + "uw wu" + inviteeID + "uw");

		acceptFriendInviter(inviterID, inviteeID);
		acceptFriendInvitee(inviterID, inviteeID);
		return 0;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		boolean updateInviteeFailed = false;
		boolean cacheUpdateFailed = false;
		boolean firstTimeDirty = false;

		logger.debug("reject friend wu" + inviterID + "uw wu" + inviteeID + "uw");

		try {
			leaseClient.acquireTillSuccess(inviteeID, getUserLogLeaseKey(inviteeID), LEASE_TIMEOUT);

			luaKeys.clear();
			luaKeys.add(KEY_LIST_FRIENDS_REQUEST + inviteeID);
			luaKeys.add(String.valueOf(inviterID));

			if ((long) redisClient.evalsha(inviteeID, RedisLuaScripts.SREM_IF_EXISTS, luaKeys, luaArgs) == 0) {
				cacheUpdateFailed = true;
			}

			if (!isDatabaseFailed.get()) {
				// database is up
				if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, inviteeID, luaKeys))) {
					int ret = mongoClient.rejectFriend(inviterID, inviteeID);
					if (ret == DATABASE_FAILURE) {
						updateInviteeFailed = true;
					}
				} else {
					updateInviteeFailed = true;
				}
			} else {
				updateInviteeFailed = true;
			}

			if (updateInviteeFailed) {
				if (cacheUpdateFailed || TardisClientConfig.monitorLostWrite) {
					firstTimeDirty = !recovery.appendPendingFriendActionToLog(inviteeID, inviterID, false, luaKeys);
				} else {
					firstTimeDirty = recovery.addDirtyFlag(inviteeID, luaKeys);
				}
			}
			logger.debug("update database " + updateInviteeFailed + " update cache " + cacheUpdateFailed);
		} catch (Exception e) {
			logger.error("Reject friend failed " + inviterID + ", " + inviteeID, e);
		} finally {
			leaseClient.releaseLease(inviteeID, getUserLogLeaseKey(inviteeID), luaKeys);
			if (updateInviteeFailed) {
				insertToEWList(firstTimeDirty, inviteeID);
			}
		}
		return 0;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {

		logger.debug("invite friend wu" + inviterID + "uw wu" + inviteeID + "uw");

		boolean updateInviteeFailed = false;
		boolean cacheUpdateFailed = false;
		boolean firstTimeDirty = false;

		try {
			leaseClient.acquireTillSuccess(inviteeID, getUserLogLeaseKey(inviteeID), LEASE_TIMEOUT);

			luaKeys.clear();
			luaKeys.add(KEY_LIST_FRIENDS_REQUEST + inviteeID);
			luaKeys.add(String.valueOf(inviterID));

			if ((long) redisClient.evalsha(inviteeID, RedisLuaScripts.SADD_IF_EXIST, luaKeys,
					Collections.emptyList()) == 0) {
				cacheUpdateFailed = true;
			}

			if (!isDatabaseFailed.get()) {
				// database is up
				if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, inviteeID, luaKeys))) {

					int ret = mongoClient.inviteFriend(inviterID, inviteeID);

					if (ret == DATABASE_FAILURE) {
						updateInviteeFailed = true;
					}
				} else {
					updateInviteeFailed = true;
				}
			} else {
				updateInviteeFailed = true;
			}

			if (updateInviteeFailed) {
				if (cacheUpdateFailed || TardisClientConfig.monitorLostWrite) {
					firstTimeDirty = !recovery.appendPendingFriendActionToLog(inviteeID, inviterID, true, luaKeys);
				} else {
					firstTimeDirty = recovery.addDirtyFlag(inviteeID, luaKeys);
				}
			}

			logger.debug("update database " + updateInviteeFailed + " update cache " + cacheUpdateFailed);
		} catch (Exception e) {
			logger.error("Invite friend failed " + inviterID + ", " + inviteeID, e);
		} finally {
			leaseClient.releaseLease(inviteeID, getUserLogLeaseKey(inviteeID), luaKeys);
			if (updateInviteeFailed) {
				insertToEWList(firstTimeDirty, inviteeID);
			}

		}
		return 0;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	@Override
	public int getCreatedResources(int creatorID, Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID,
			Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		return 0;
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID, int manipulationID) {
		return 0;
	}

	private int thawFriendInviter(int inviterID, int inviteeID) {
		boolean updateInviterFailed = false;
		boolean cacheUpdateFailed = false;
		boolean firstTimeDirty = false;

		try {
			leaseClient.acquireTillSuccess(inviterID, getUserLogLeaseKey(inviterID), LEASE_TIMEOUT);
			ShardedJedis jedis;
			luaKeys.clear();
			luaKeys.add(KEY_LIST_FRIENDS + inviterID);
			luaKeys.add(String.valueOf(inviteeID));

			if ((long) redisClient.evalsha(inviterID, RedisLuaScripts.SREM_IF_EXISTS, luaKeys,
					Collections.emptyList()) == 0) {
				cacheUpdateFailed = true;
			}

			if (!isDatabaseFailed.get()) {
				if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, inviterID, luaKeys))) {
					int ret = mongoClient.thawFriendInviter(inviterID, inviteeID);
					if (ret == DATABASE_FAILURE) {
						updateInviterFailed = true;
					}
				} else {
					updateInviterFailed = true;
				}
			} else {
				updateInviterFailed = true;
			}

			if (updateInviterFailed) {
				if (cacheUpdateFailed || TardisClientConfig.monitorLostWrite) {
					firstTimeDirty = !recovery.appendFriendActionToLog(inviterID, inviteeID, false, luaKeys);
				} else {
					firstTimeDirty = recovery.addDirtyFlag(inviterID, luaKeys);
				}
			}

			logger.debug("update database " + updateInviterFailed + " update cache " + cacheUpdateFailed);
		} catch (Exception e) {
			logger.error("Thaw friend, update inviter failed " + inviterID + ", " + inviteeID, e);
		} finally {
			leaseClient.releaseLease(inviterID, getUserLogLeaseKey(inviterID), luaKeys);
			if (updateInviterFailed) {
				insertToEWList(firstTimeDirty, inviterID);
			}
		}
		return updateInviterFailed ? DATABASE_FAILURE : 0;
	}

	private void insertToEWList(boolean firstTimeDirty, int userId) {
		if (firstTimeDirty) {
			leaseClient.acquireTillSuccess(TardisClientConfig.getEWId(userId), 
			    getEWLogMutationLeaseKeyFromUserId(userId), LEASE_TIMEOUT);
			recovery.insertUserToPartitionLog(userId);
			leaseClient.releaseLease(TardisClientConfig.getEWId(userId), 
			    getEWLogMutationLeaseKeyFromUserId(userId), luaKeys);
		}
	}

	private int thawFriendInvitee(int inviterID, int inviteeID) {
		boolean updateInviteeFailed = false;
		boolean cacheUpdateFailed = false;
		boolean firstTimeDirty = false;
		try {
			leaseClient.acquireTillSuccess(inviteeID, getUserLogLeaseKey(inviteeID), LEASE_TIMEOUT);

			luaKeys.clear();
			luaKeys.add(KEY_LIST_FRIENDS + inviteeID);
			luaKeys.add(String.valueOf(inviterID));

			if ((long) redisClient.evalsha(inviteeID, RedisLuaScripts.SREM_IF_EXISTS, luaKeys,
					Collections.emptyList()) == 0) {
				cacheUpdateFailed = true;
			}

			if (!isDatabaseFailed.get()) {
				if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, inviteeID, luaKeys))) {
					int ret = mongoClient.thawFriendInvitee(inviterID, inviteeID);
					if (ret == DATABASE_FAILURE) {
						updateInviteeFailed = true;
					}
				} else {
					updateInviteeFailed = true;
				}
			} else {
				updateInviteeFailed = true;
			}

			if (updateInviteeFailed) {
				if (cacheUpdateFailed || TardisClientConfig.monitorLostWrite) {
					firstTimeDirty = !recovery.appendFriendActionToLog(inviteeID, inviterID, false, luaKeys);
				} else {
					firstTimeDirty = recovery.addDirtyFlag(inviteeID, luaKeys);
				}
			}

			logger.debug("update database " + updateInviteeFailed + " update cache " + cacheUpdateFailed);
		} catch (Exception e) {
			logger.error("Thaw friend, update invitee failed " + inviterID + ", " + inviteeID, e);
		} finally {
			leaseClient.releaseLease(inviteeID, getUserLogLeaseKey(inviteeID), luaKeys);
			if (updateInviteeFailed) {
				insertToEWList(firstTimeDirty, inviteeID);
			}
		}
		return updateInviteeFailed ? DATABASE_FAILURE : 0;
	}

	@Override
	public int thawFriendship(int inviterID, int inviteeID) {

		logger.debug("thaw friend wu" + inviterID + "uw wu" + inviteeID + "uw");

		thawFriendInviter(inviterID, inviteeID);
		thawFriendInvitee(inviterID, inviteeID);

		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		// TODO: find a better way to check when load finished
		if (getProperties().getProperty(KEY_ACTION).equals(LOAD_DATA)) {
			canClean.set(true);
		}
		return mongoClient.getInitialStats();
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		return mongoClient.CreateFriendship(friendid1, friendid2);
	}

	@Override
	public void createSchema(Properties props) {
		mongoClient.createSchema(props);
		if (getProperties().getProperty(KEY_ACTION).equals(CREATE_SCHEMA)) {
			canClean.set(true);
		}
	}

	@Override
	public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
		return 0;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
		return 0;
	}

	public static void main(String[] args) {

		long sum = 0;

		for (int i = 0; i < 1000; i++) {
			long s = System.currentTimeMillis();
			char[] c = new char[708066];
			Arrays.fill(c, '0');
			// RandomStringUtils.randomAscii(708066);
			sum += (System.currentTimeMillis() - s);
		}

		System.out.println(sum / 1000);

	}

}
