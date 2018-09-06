package mongodb;

import static mongodb.ReconConfig.CREATE_SCHEMA;
import static mongodb.ReconConfig.KEY_ACTION;
import static mongodb.ReconConfig.KEY_ALPHA_POLICY;
import static mongodb.ReconConfig.KEY_BASE_ALPHA;
import static mongodb.ReconConfig.KEY_NUM_AR_WORKER;
import static mongodb.ReconConfig.LOAD_DATA;
import static mongodb.ReconConfig.RUN;

import static edu.usc.bg.workloads.CoreWorkload.DATABASE_FAILURE;
import static tardis.TardisClientConfig.KEY_FRIEND_COUNT;
import static tardis.TardisClientConfig.KEY_LIST_FRIENDS;
import static tardis.TardisClientConfig.KEY_LIST_FRIENDS_REQUEST;
import static tardis.TardisClientConfig.KEY_PENDING_FRIEND_COUNT;
import static tardis.TardisClientConfig.KEY_VIEW_PROFILE;
import static tardis.TardisClientConfig.LEASE_TIMEOUT;
import static tardis.TardisClientConfig.PAGE_SIZE_FRIEND_LIST;
import static tardis.TardisClientConfig.getEWLogMutationLeaseKeyFromUserId;
import static tardis.TardisClientConfig.getUserLogLeaseKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.MemcachedLease;
import com.meetup.memcached.SockIOPool;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;
import edu.usc.bg.workloads.CoreWorkload;
import mongodb.alpha.AlphaDynamicPolicy;
import mongodb.alpha.AlphaPolicy;
import mongodb.alpha.AlphaPolicy.AlphaPolicyEnum;
import tardis.TardisClientConfig;

public class MemcachedMongoBGClient extends DB {
	MemcachedClient memcachedClient;
	MemcachedLease leaseClient;
	RecoveryEngine recovery;
	SockIOPool pool;
	static MongoBGClient mongoClient;
	static DatabaseStateSimulator databaseStateSimulator;
	static List<ActiveRecoveryWorker> activeRecoveryWorker = new ArrayList<>();
	static EWStatsWatcher ewStatsWatcher;
//	static SlabStatsWatcher slabStatsWatcher;
	static long start = System.currentTimeMillis();

	static final AtomicBoolean isDatabaseFailed = new AtomicBoolean(false);
	static boolean writeBack = false;
	static final AtomicBoolean isWarmUp = new AtomicBoolean(true);

	final Logger logger = Logger.getLogger(MemcachedMongoBGClient.class);

	static AtomicBoolean initialized = new AtomicBoolean(false);
	static final Semaphore initializeLock = new Semaphore(1);
	static final AtomicBoolean canClean = new AtomicBoolean(false);


	public static final boolean enableMetrics = true;

	static boolean asyncGet = false;
	static final boolean lockRead = false;

	/**
	 * A hacky way to make sure we can clean up resources when experiments
	 * finished.
	 */
	static AtomicInteger threads = new AtomicInteger(0);
	/**
	 * Set to false if validation is ran
	 */

	static final ExecutorService executor = Executors.newFixedThreadPool(120);

	final byte[] readBuffer = new byte[1024 * 5];
	static AtomicInteger viewProfileExecuted = new AtomicInteger(0);
	static AtomicInteger sockPoolNum = new AtomicInteger();
	int profileSize = -1;

	boolean noCachePut = false;

	boolean onlyLocalLease = false;


	static boolean measureLostWriteAfterFailure = false;

	public static boolean journaled = false;


	static boolean useCAMP = false;

	@Override
	public boolean init() throws DBException {
		java.util.logging.Logger mongoLogger = java.util.logging.Logger.getLogger("org.mongodb.driver");
		mongoLogger.setLevel(java.util.logging.Level.SEVERE);

		if (getProperties().getProperty("threadcount") != null) {
			threads.set(Integer.parseInt(getProperties().getProperty("threadcount")));
		}

		if (getProperties().getProperty("camp") != null) {
			useCAMP = Boolean.parseBoolean(getProperties().getProperty("camp"));
		}
		
		if (getProperties().getProperty("writeback") != null) {
			writeBack = Boolean.parseBoolean(getProperties().getProperty("writeback"));
		}
		
		if (getProperties().getProperty("warmup") != null) {
			isWarmUp.set(false);
		}

		if (getProperties().getProperty(ReconConfig.KEY_NUM_USER_IN_CACHE) != null) {
			TardisClientConfig.numUserInCache = Integer.parseInt(getProperties().getProperty(ReconConfig.KEY_NUM_USER_IN_CACHE));
		}

		if (getProperties().getProperty(ReconConfig.KEY_MONITOR_LOST_WRITE) != null) {
			TardisClientConfig.monitorLostWrite = Boolean.parseBoolean(getProperties().getProperty(ReconConfig.KEY_MONITOR_LOST_WRITE));
		}

		if (getProperties().getProperty(ReconConfig.KEY_TWEMCACHED_IP) == null) {
			throw new RuntimeException("twemcached Ip not specified");
		}

		if (getProperties().getProperty(MongoBGClient.KEY_MONGO_DB_IP) == null) {
			throw new RuntimeException("mongo Ip not specified");
		}

		if (getProperties().getProperty("enablelogging") != null) {
			TardisClientConfig.supportPaginatedFriends = !Boolean.parseBoolean(getProperties().getProperty("enablelogging"));
		}
		
    if (getProperties().getProperty("limitfriends") != null) {
      TardisClientConfig.PAGE_SIZE_FRIEND_LIST = Integer.parseInt(getProperties().getProperty("limitfriends"));
    }

		if (getProperties().getProperty("asyncget") != null) {
			asyncGet = Boolean.parseBoolean(getProperties().getProperty("asyncget"));
		}

		if (getProperties().getProperty("profilesize") != null) {
			profileSize = Integer.parseInt(getProperties().getProperty("profilesize"));
		}

		if (getProperties().getProperty("nocacheput") != null) {
			noCachePut = Boolean.parseBoolean(getProperties().getProperty("nocacheput"));
		}

		if (getProperties().getProperty("onlylocallease") != null) {
			onlyLocalLease = Boolean.parseBoolean(getProperties().getProperty("onlylocallease"));
		}

		if (getProperties().getProperty("measurelostwritefailure") != null) {
			TardisClientConfig.measureLostWriteAfterFailure = Boolean.parseBoolean(getProperties().getProperty("measurelostwritefailure"));
		}

		if (getProperties().getProperty("ew") != null) {
			TardisClientConfig.NUM_EVENTUAL_WRITE_LOGS = Integer.parseInt(getProperties().getProperty("ew"));
		}

		String[] serverlist = getProperties().getProperty(ReconConfig.KEY_TWEMCACHED_IP).split(",");
		
		System.out.println("Server list: "+Arrays.toString(serverlist));

		initStaticResources(serverlist);

		memcachedClient = getMemcachedClient(serverlist, "BG" + sockPoolNum.incrementAndGet());
		leaseClient = new MemcachedLease(1, memcachedClient, onlyLocalLease);
		
		recovery = new RecoveryEngine(memcachedClient, mongoClient);
		if (getProperties().getProperty("lazyrecovery") != null) {
			recovery.setLazyRecovery(Boolean.parseBoolean(getProperties().getProperty("lazyrecovery")));
		}

		System.out.println("### initialized ");

		return true;
	}

	private void initStaticResources(String[] serverlist) {
		System.out.println("### init");
		try {

			initializeLock.acquire();
			if (initialized.get()) {
				initializeLock.release();
				return;
			}

			System.out.println("########### version 13");

			ConsoleAppender console = new ConsoleAppender(); // create appender
			// configure the appender
			String PATTERN = "%d [%p|%c|%C{1}] %m%n";
			console.setLayout(new PatternLayout(PATTERN));
			console.setThreshold(Level.ERROR);
			console.activateOptions();
			// add appender to any Logger (here is root)
			Logger.getRootLogger().addAppender(console);

			Logger.getLogger(MongoBGClientDelegate.class).setLevel(Level.ERROR);
			Logger.getLogger(MemcachedSetHelper.class).setLevel(Level.ERROR);
			Logger.getLogger(MemcachedClient.class).setLevel(Level.ERROR);
			Logger.getLogger(SockIOPool.class).setLevel(Level.OFF);
			Logger.getLogger(SockIOPool.SockIO.class).setLevel(Level.OFF);
			Logger.getLogger(MemcachedMongoBGClient.class).setLevel(Level.DEBUG);
			Logger.getLogger(MemcachedLease.class).setLevel(Level.DEBUG);
			Logger.getLogger(MongoBGClient.class).setLevel(Level.OFF);
			Logger.getLogger(RecoveryEngine.class).setLevel(Level.ERROR);
			Logger.getLogger(CoreWorkload.class).setLevel(Level.INFO);
			Logger.getLogger(ActiveRecoveryWorker.class).setLevel(Level.ERROR);
			Logger.getLogger(EWStatsWatcher.class).setLevel(Level.OFF);

			getProperties().forEach((k, v) -> {
				System.out.println("key " + k + " value " + v);
			});

			if (getProperties().getProperty(ReconConfig.KEY_FULL_WARM_UP) != null) {
				TardisClientConfig.fullWarmUp = Boolean.parseBoolean(getProperties().getProperty(ReconConfig.KEY_FULL_WARM_UP));
			}

			mongoClient = new MongoBGClientDelegate(getProperties().getProperty(MongoBGClient.KEY_MONGO_DB_IP),
					isDatabaseFailed);
			mongoClient.init();

			if (writeBack == false) {
				String intervals = getProperties().getProperty(
				    TardisClientConfig.KEY_DB_FAILED);
				if (intervals != null) {
					String[] ints = intervals.split(",");
					long[] longIntervals = new long[ints.length];
					for (int i = 0; i < ints.length; i++) {
						longIntervals[i] = Long.parseLong(ints[i]);
					}
	
					databaseStateSimulator = new DatabaseStateSimulator(getMemcachedClient(serverlist, getPoolName()),
							isDatabaseFailed, longIntervals);
	
					for (int i = 0; i < longIntervals.length; i += 2) {
						ArrayMetrics.getInstance().add(MetricsName.METRICS_DATABASE_DOWN_TIME,
								new DatabaseFailureRecord(longIntervals[i], longIntervals[i + 1]));
					}
					executor.submit(databaseStateSimulator);
				}
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
					MemcachedClient client = getMemcachedClient(serverlist, getPoolName());
					MemcachedLease leaseClient = new MemcachedLease(1, client, onlyLocalLease);
					RecoveryEngine recovery = new RecoveryEngine(client, mongoClient);
					activeRecoveryWorker.add(new ActiveRecoveryWorker(isDatabaseFailed, client, leaseClient, recovery,
							alphaPolicy, Integer.parseInt(numARWorker)));
				}
			}

//			ewStatsWatcher = new EWStatsWatcher(getMemcachedClient(serverlist, getPoolName()));
//			slabStatsWatcher = new SlabStatsWatcher(getMemcachedClient(serverlist, getPoolName()));

//			executor.submit(ewStatsWatcher);
//			executor.submit(slabStatsWatcher);

			activeRecoveryWorker.forEach(i -> {
				executor.submit(i);
			});
			
			System.out.println("Started all AR workers. Num ARs = "+activeRecoveryWorker.size());

			initialized.set(true);
			initializeLock.release();
		} catch (Exception e) {
			logger.error("Initialize failed", e);
		}
	}

	public static String getPoolName() {
		return "BG" + sockPoolNum.incrementAndGet();
	}

	public static MemcachedClient getMemcachedClient(String[] serverlist, String poolName) {
		SockIOPool pool = SockIOPool.getInstance(poolName);
		System.out.println(poolName);
		if (!pool.isInitialized()) {
			pool.setServers(serverlist);

			pool.setInitConn(2);
			pool.setMinConn(1);
			pool.setMaxConn(5);
			pool.setMaintSleep(0);

			pool.setNagle(false);
			pool.initialize();
		}

		// get client instance
		return new MemcachedClientDelegate(poolName);
	}

	@Override
	public void cleanup(boolean warmup) throws DBException {
		int numUsers = Integer.parseInt(getProperties().getProperty("usercount"));

		int numFriendsPerUser = Integer.parseInt(getProperties().getProperty("friendcountperuser"));
//		int numFriendsPerUser = 100;
		
		int max = 0;
		if (TardisClientConfig.fullWarmUp) {
			max = numUsers;
		} else if (TardisClientConfig.numUserInCache > 0) {
			max = TardisClientConfig.numUserInCache > numUsers ? numUsers : TardisClientConfig.numUserInCache;
		}
		
		// load cache
		for (int i = 0; i < max; i++) {
			Set<String> friends = new HashSet<>();
			for (int j = 1; j <= numFriendsPerUser / 2; j++) {
				int id = i+j;
				if (id >= numUsers) id -= numUsers;
				friends.add(String.valueOf(id));
		
				id = i-j;
				if (id < 0) id += numUsers;
				friends.add(String.valueOf(id));
			}
		
			MemcachedSetHelper.set(memcachedClient, KEY_LIST_FRIENDS + i, null, friends);
			try {
				memcachedClient.set(KEY_FRIEND_COUNT + i, String.valueOf(friends.size()));
			} catch (Exception e) {
				logger.error("set failed", e);
			}
		}

//		if (TardisClientConfig.fullWarmUp) {
//			if (!mongoClient.getFriendList().isEmpty()) {
//				mongoClient.getFriendList().forEach((id, friends) -> {
//					MemcachedSetHelper.set(memcachedClient, KEY_LIST_FRIENDS + id, id, friends);
//					try {
//						memcachedClient.set(KEY_FRIEND_COUNT + id, String.valueOf(friends.size()), id);
//					} catch (Exception e) {
//						logger.error("set failed", e);
//					}
//				});
//				mongoClient.bulkWriteFriends();
//			}
//			System.out.println("inserted users");
//		} else if (TardisClientConfig.numUserInCache != -1) {
//			if (!mongoClient.getFriendList().isEmpty()) {
//				for (int i = 0; i < TardisClientConfig.numUserInCache; i++) {
//					Set<String> friends = mongoClient.getFriendList().get(i);
//					MemcachedSetHelper.set(memcachedClient, KEY_LIST_FRIENDS + i, i, friends);
//					try {
//						memcachedClient.set(KEY_FRIEND_COUNT + i, String.valueOf(friends.size()), i);
//					} catch (Exception e) {
//						logger.error("set failed", e);
//					}
//				}
//			}
//			
//			System.out.println("Finish load cache.");
//		}
		
		if (!warmup && MongoBGClient.createFriendship.get() == true) {
			mongoClient.setProperties(getProperties());
			mongoClient.bulkWriteFriends();
		}

		try {
			System.out.println("### cleanup");

			System.out.println("Number of back-offs " + MemcachedLease.getBackoff().get());
			System.out.println("Number of socket backoffs " + MemcachedClient.GET_SOCK_FAILED.get());

			// TODO: find a better way to check experiment finished
			if (!warmup && getProperties().getProperty(KEY_ACTION).equals(RUN)) {
				if (viewProfileExecuted.get() >= 1000 || threads.get() == 0) {
					canClean.set(true);
				}
			}

			if (processed.get() >= 100000) {
				canClean.set(true);
			}

			if (!canClean.get()) {
				return;
			}

			if (threads.decrementAndGet() > 0) {
				return;
			}

			System.out.println("####!!!! cleaned up ");

			// System.out.println("#############Load time " +
			// (System.currentTimeMillis() - startTime));

			if (getProperties().getProperty(KEY_ACTION).equals(RUN)) {
				String metricsFile = getProperties().getProperty(ReconConfig.KEY_METRICS_FILE_NAME);
				// ideally, this should equal to 0
				int pws = RecoveryEngine.measureLostWrites(memcachedClient);
				MetricsHelper.numberOfPendingWritesAttheEndOfRecoveryMode.set(pws);


				if (enableMetrics) {
					MetricsHelper.writeMetrics(metricsFile, getProperties(), start, System.currentTimeMillis(), 
					    TimedMetrics.getInstance(),ArrayMetrics.getInstance());

//				if (databaseStateSimulator != null && TardisClientConfig.enableMetrics) {
//					MetricsHelper.writeMetrics(metricsFile, getProperties(), start, System.currentTimeMillis(),
//							databaseStateSimulator.getInvtervals()[1] * 1000, TimedMetrics.getInstance(),
//
//							ArrayMetrics.getInstance());
				}
			}

			if (databaseStateSimulator != null) {
				databaseStateSimulator.shutdown();
			}
//			ewStatsWatcher.shutdown();
//			slabStatsWatcher.shutdown();
			activeRecoveryWorker.forEach(i -> {
				i.shutdown();
			});

			executor.shutdown();

			executor.awaitTermination(10, TimeUnit.SECONDS);

			mongoClient.cleanup(warmup);
			memcachedClient.cleanup();
			// this.pool.shutDown();

		} catch (Exception e) {
			logger.error("clean up failed", e);
		}
		
		// set isWarmUp to false at the end of warm up phase so the failure may start.
		if (warmup) {
			isWarmUp.compareAndSet(true, false);
		}
		
		super.cleanup(warmup);
	}

	@Override
	public Properties getProperties() {
		return super.getProperties();
	}

	int mem = 0;

	private Random r = new Random(100000);
	private AtomicBoolean firstTime = new AtomicBoolean(false);
	private long startTime = 0;
	private AtomicInteger processed = new AtomicInteger(0);

	@Override
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values,
			boolean insertImage) {

		// if (firstTime.compareAndSet(false, true)) {
		// startTime = System.currentTimeMillis();
		// }
		//
		// int sizeOfValue = r.nextInt(899996) + 4;
		//
		// char[] c = new char[sizeOfValue];
		// Arrays.fill(c, '0');
		// long s = System.currentTimeMillis();
		// try {
		// memcachedClient.set(entityPK, new String(c));
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// long e = System.currentTimeMillis();
		// double meanInsert = (e - startTime) / (processed.incrementAndGet());
		// System.out.println("inserted " + entityPK + " size " + sizeOfValue +
		// " meanload " + meanInsert + " this time " + (e - s));
		// return 0;

		try {
			if (profileSize != -1) {
				values = new HashMap<>();
				String uid = "";
				Random r = new Random();
				for (int i = 0; i < profileSize; i++) {
					uid += (char) (r.nextInt(26) + 'a');
				}
				values.put("uid", new ObjectByteIterator(uid.getBytes()));
			}
			if (TardisClientConfig.fullWarmUp) {
				memcachedClient.set(KEY_FRIEND_COUNT + entityPK, "0", Integer.parseInt(entityPK));
				memcachedClient.set(KEY_PENDING_FRIEND_COUNT + entityPK, "0", Integer.parseInt(entityPK));
				memcachedClient.set(KEY_VIEW_PROFILE + entityPK, 
				    CacheUtilities.SerializeHashMap(values), Integer.parseInt(entityPK));
				memcachedClient.set(KEY_LIST_FRIENDS + entityPK,
						MemcachedSetHelper.convertString(Collections.emptyList()),
						Integer.parseInt(entityPK));
				memcachedClient.set(KEY_LIST_FRIENDS_REQUEST + entityPK,
						MemcachedSetHelper.convertString(Collections.emptyList()),
						Integer.parseInt(entityPK));
			}

			if (TardisClientConfig.numUserInCache != -1 && !TardisClientConfig.fullWarmUp) {
				if (Integer.parseInt(entityPK) < TardisClientConfig.numUserInCache) {
					memcachedClient.set(KEY_FRIEND_COUNT + entityPK, "0", Integer.parseInt(entityPK));
					memcachedClient.set(KEY_PENDING_FRIEND_COUNT + entityPK, "0", Integer.parseInt(entityPK));
					memcachedClient.set(KEY_VIEW_PROFILE + entityPK, CacheUtilities.SerializeHashMap(values), Integer.parseInt(entityPK));
					memcachedClient.set(KEY_LIST_FRIENDS + entityPK,
							MemcachedSetHelper.convertString(Collections.emptyList()), Integer.parseInt(entityPK));
					memcachedClient.set(KEY_LIST_FRIENDS_REQUEST + entityPK,
							MemcachedSetHelper.convertString(Collections.emptyList()), Integer.parseInt(entityPK));
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

		try {
			if (lockRead) {
				leaseClient.acquireTillSuccess(getUserLogLeaseKey(profileOwnerID), 
				    profileOwnerID, LEASE_TIMEOUT);
			}

//			Map<String, Object> profile = memcachedClient.getMulti(new String[] { KEY_VIEW_PROFILE + profileOwnerID,
//					KEY_FRIEND_COUNT + profileOwnerID, KEY_PENDING_FRIEND_COUNT + profileOwnerID }, asyncGet);
			String[] keys = new String[] {
          KEY_VIEW_PROFILE + profileOwnerID,
          KEY_FRIEND_COUNT + profileOwnerID, 
          KEY_PENDING_FRIEND_COUNT + profileOwnerID };
			
			Object[] objs = memcachedClient.getMultiArray(keys, new Integer[] {profileOwnerID, profileOwnerID,profileOwnerID});
			
			if (objs[0] != null && objs[1] != null && objs[2] != null) {
			    result.put("userid", new ObjectByteIterator(String.valueOf(profileOwnerID).getBytes()));
			 
				CacheUtilities.unMarshallHashMap(result, (byte[]) objs[0],
						readBuffer);

				result.put("friendcount", new ObjectByteIterator(
						String.valueOf(objs[1]).trim().getBytes()));
				if (requesterID == profileOwnerID) {
					result.put("pendingcount", new ObjectByteIterator(
							String.valueOf(objs[2]).getBytes()));
				}
				return 0;
			} else {
				if (TardisClientConfig.fullWarmUp) {
					logger.fatal("cache miss. existing keys " + Arrays.toString(keys));
				}
				return recoverViewProfile(requesterID, profileOwnerID, result, insertImage, testMode, lockRead);
			}
		} finally {
			if (lockRead) {
				leaseClient.releaseLease(getUserLogLeaseKey(profileOwnerID), profileOwnerID);
			}
		}
	}

	private int recoverViewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
			boolean insertImage, boolean testMode, boolean alreadyLocked) {
		if (isDatabaseFailed.get()) {
			return DATABASE_FAILURE;
		}

		try {
			if (!alreadyLocked) {
				leaseClient.acquireTillSuccess(getUserLogLeaseKey(profileOwnerID), 
				    profileOwnerID, LEASE_TIMEOUT);
			}

			if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.READ, profileOwnerID))) {
				// view the user's profile.
				int ret = mongoClient.viewProfile(profileOwnerID, profileOwnerID, result, insertImage, testMode);
				if (ret == DATABASE_FAILURE) {
					return DATABASE_FAILURE;
				} else {
					if (!noCachePut) {
						memcachedClient.set(KEY_FRIEND_COUNT + profileOwnerID,
								new String(result.get("friendcount").toArray()), profileOwnerID);
						memcachedClient.set(KEY_PENDING_FRIEND_COUNT + profileOwnerID,
								new String(result.get("pendingcount").toArray()), profileOwnerID);
						memcachedClient.set(KEY_VIEW_PROFILE + profileOwnerID, 
						    CacheUtilities.SerializeHashMap(result), profileOwnerID);
					}
				}
				if (requesterID != profileOwnerID) {
					result.remove("pendingcount");
				}
			} else {
				return DATABASE_FAILURE;
			}
		} catch (Exception e) {
			logger.error("Recover user profile failed " + profileOwnerID, e);
		} finally {
			if (!alreadyLocked) {
				leaseClient.releaseLease(getUserLogLeaseKey(profileOwnerID), 
				    profileOwnerID);
			}
		}
		return 0;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> results, boolean insertImage, boolean testMode) {

		if (viewProfileExecuted.get() <= 1000) {
			viewProfileExecuted.incrementAndGet();
		}

		if (lockRead) {
			leaseClient.acquireTillSuccess(
			    getUserLogLeaseKey(profileOwnerID), profileOwnerID, LEASE_TIMEOUT);
		}

		String key = KEY_LIST_FRIENDS + profileOwnerID;
		Collection<String> friends = MemcachedSetHelper.convertSet(
		    memcachedClient.get(key, profileOwnerID, true));

		logger.debug("list friends ru" + profileOwnerID + "ur cached " + friends);
		try {
			if (friends == null) {
				if (TardisClientConfig.fullWarmUp) {
					logger.fatal("BUG: cache miss list friends ru" + profileOwnerID);
				}
				// cache miss
				if (isDatabaseFailed.get()) {
					return DATABASE_FAILURE;
				} else {
					try {
						if (!lockRead) {
							leaseClient.acquireTillSuccess(
							    getUserLogLeaseKey(profileOwnerID), 
							    profileOwnerID, LEASE_TIMEOUT);
						}

						if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.READ, profileOwnerID))) {

							friends = mongoClient.listFriends(profileOwnerID);

							logger.debug("list friends ru" + profileOwnerID + "ur db " + friends);

							if (friends == null) {
								return DATABASE_FAILURE;
							} else {
								if (!noCachePut) {
									memcachedClient.set(key, 
									    MemcachedSetHelper.convertString(friends), profileOwnerID);
								}
							}
						} else {
							return DATABASE_FAILURE;
						}
					} catch (Exception e) {
						logger.error("list friends failed " + profileOwnerID, e);
					} finally {
						if (!lockRead) {
							leaseClient.releaseLease(getUserLogLeaseKey(profileOwnerID), profileOwnerID);
						}
					}
				}
			}
		} finally {
			if (lockRead) {
				leaseClient.releaseLease(getUserLogLeaseKey(profileOwnerID), profileOwnerID);
			}
		}
		if (viewFriendsProfile(friends, results) == DATABASE_FAILURE) {
			return DATABASE_FAILURE;
		}
		return 0;
	}

	@Override
	public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {

		if (viewProfileExecuted.get() <= 1000) {
			viewProfileExecuted.incrementAndGet();
		}

		if (lockRead) {
			leaseClient.acquireTillSuccess(getUserLogLeaseKey(profileOwnerID), 
			    profileOwnerID, LEASE_TIMEOUT);
		}

		String key = KEY_LIST_FRIENDS_REQUEST + profileOwnerID;
		Collection<String> friends = MemcachedSetHelper.convertSet(memcachedClient.get(key, profileOwnerID, true));

		logger.debug("list pend friends ru" + profileOwnerID + "ur cached " + friends);

		try {
			if (friends == null) {
				if (TardisClientConfig.fullWarmUp) {
					logger.fatal("BUG: cache miss list pend friends ru" + profileOwnerID);
				}

				// cache miss
				if (isDatabaseFailed.get()) {
					return DATABASE_FAILURE;
				} else {
					try {
						if (!lockRead) {
							leaseClient.acquireTillSuccess(
							    getUserLogLeaseKey(profileOwnerID), 
							    profileOwnerID, LEASE_TIMEOUT);
						}
						if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.READ, profileOwnerID))) {
							friends = mongoClient.listPendingFriends(profileOwnerID);

							logger.debug("list pend friends ru" + profileOwnerID + "ur db " + friends);

							if (friends == null) {
								return DATABASE_FAILURE;
							} else {
								if (!noCachePut) {
									memcachedClient.set(key, 
									    MemcachedSetHelper.convertString(friends), profileOwnerID);
								}
							}
						} else {
							return DATABASE_FAILURE;
						}
					} catch (Exception e) {
						logger.error("List Friend Request failed " + profileOwnerID, e);
					} finally {
						if (!lockRead) {
							leaseClient.releaseLease(
							    getUserLogLeaseKey(profileOwnerID), profileOwnerID);
						}
					}
				}
			}
		} finally {
			if (lockRead) {
				leaseClient.releaseLease(getUserLogLeaseKey(profileOwnerID), profileOwnerID);
			}
		}
		if (viewFriendsProfile(friends, results) == DATABASE_FAILURE) {
			return DATABASE_FAILURE;
		}
		return 0;
	}

	private int viewFriendsProfile(Collection<String> friends, Vector<HashMap<String, ByteIterator>> results) {

		if (friends.size() == 0) {
			return 0;
		}

//		List<String> keys = new ArrayList<>();
		Collection<String> paginatedFriends = friends;

		if (TardisClientConfig.supportPaginatedFriends) {
			paginatedFriends = friends.stream().limit(PAGE_SIZE_FRIEND_LIST).collect(Collectors.toList());
		}

//		paginatedFriends.forEach(i -> {
//			keys.add(KEY_VIEW_PROFILE + i);
//			keys.add(KEY_FRIEND_COUNT + i);
//		});

//		Map<String, Object> profiles = memcachedClient.getMulti(keys.toArray(new String[0]), asyncGet);
		
    String[] keys = new String[paginatedFriends.size()*2];
    Integer[] hashCodes = new Integer[keys.length];
    
    String[] fs = paginatedFriends.toArray(new String[0]);
    for (int i = 0 ; i < fs.length; i++) {
      keys[i*2] = KEY_VIEW_PROFILE+fs[i];
      keys[i*2+1] = KEY_FRIEND_COUNT+fs[i];
      
      hashCodes[i*2] = Integer.parseInt(fs[i]);
      hashCodes[i*2+1] = Integer.parseInt(fs[i]);
    }
    
    Object[] objs = memcachedClient.getMultiArray(keys, hashCodes);

		for (int i = 0; i < fs.length; i++) {
			HashMap<String, ByteIterator> result = new HashMap<>();
			if (objs[i*2] != null && objs[i*2+1] != null) {
				result.put("userid", new ObjectByteIterator(fs[i].getBytes()));
				CacheUtilities.unMarshallHashMap(result, (byte[]) objs[i*2], readBuffer);
				result.put("friendcount",
						new ObjectByteIterator(String.valueOf(objs[i*2+1]).getBytes()));
			} else {
				if (TardisClientConfig.fullWarmUp) {
					logger.fatal("BUG: cache miss list users profile " + fs[i]);
				}

				if (recoverViewProfile(-1, Integer.parseInt(fs[i]), result, false, false,
						false) == DATABASE_FAILURE) {
					return DATABASE_FAILURE;
				}
			}
			results.add(result);
		}
		return 0;
	}

	private int acceptFriendInviter(int inviterID, int inviteeID) {
		boolean updateInviterFailed = false;
		boolean cacheUpdateFailed = false;
		boolean firstTimeDirty = false;

		try {
			leaseClient.acquireTillSuccess(getUserLogLeaseKey(inviterID), 
			    inviterID, LEASE_TIMEOUT);
			if (memcachedClient.incr(KEY_FRIEND_COUNT + inviterID, 1L, inviterID) == -1) {
				cacheUpdateFailed = true;
			}
			if (!MemcachedSetHelper.addToSet(memcachedClient, 
			    KEY_LIST_FRIENDS + inviterID, inviterID, String.valueOf(inviteeID),
					false)) {
				cacheUpdateFailed = true;
			}

			if (!writeBack && !isDatabaseFailed.get()) {
				if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, inviterID))) {
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
					firstTimeDirty = !recovery.appendFriendActionToLog(inviterID, inviteeID, true);
				} else {
					firstTimeDirty = recovery.addDirtyFlag(inviterID);
				}
			}
			logger.debug("update database " + updateInviterFailed + " update cache " + cacheUpdateFailed);

		} catch (Exception e) {
			logger.error("Accept friend, update inviter failed " + inviterID + ", " + inviteeID, e);
		} finally {
			leaseClient.releaseLease(getUserLogLeaseKey(inviterID), inviterID);
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
			leaseClient.acquireTillSuccess(getUserLogLeaseKey(inviteeID), 
			    inviteeID, LEASE_TIMEOUT);

			if (memcachedClient.incr(KEY_FRIEND_COUNT + inviteeID, 1L, inviteeID) == -1) {
				cacheUpdateFailed = true;
			}
			if (!MemcachedSetHelper.addToSet(memcachedClient, 
			    KEY_LIST_FRIENDS + inviteeID, inviteeID, String.valueOf(inviterID),
					false)) {
				cacheUpdateFailed = true;
			}
			if (memcachedClient.decr(KEY_PENDING_FRIEND_COUNT + inviteeID, 1L, inviteeID) == -1) {
				cacheUpdateFailed = true;
			}
			if (!MemcachedSetHelper.removeFromSet(memcachedClient, KEY_LIST_FRIENDS_REQUEST + inviteeID, inviteeID,
					String.valueOf(inviterID))) {
				cacheUpdateFailed = true;
			}

			if (!writeBack && !isDatabaseFailed.get()) {
				if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, inviteeID))) {
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
					firstTimeDirty = !recovery.appendFriendActionToLog(inviteeID, inviterID, true);
					recovery.appendPendingFriendActionToLog(inviteeID, inviterID, false);
				} else {
					// if add success, it means it is the first time.
					firstTimeDirty = recovery.addDirtyFlag(inviteeID);
				}
			}

			logger.debug("update database " + updateInviteeFailed + " update cache " + cacheUpdateFailed);
		} catch (Exception e) {
			logger.error("Accept friend, update invitee failed " + inviterID + ", " + inviteeID, e);
		} finally {
			leaseClient.releaseLease(getUserLogLeaseKey(inviteeID), inviteeID);
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
			leaseClient.acquireTillSuccess(getUserLogLeaseKey(inviteeID), 
			    inviteeID, LEASE_TIMEOUT);

			if (memcachedClient.decr(KEY_PENDING_FRIEND_COUNT + inviteeID, 1L, inviteeID) == -1) {
				cacheUpdateFailed = true;
			}
			if (!MemcachedSetHelper.removeFromSet(memcachedClient, 
			    KEY_LIST_FRIENDS_REQUEST + inviteeID, inviteeID, 
					String.valueOf(inviterID))) {
				cacheUpdateFailed = true;
			}

			if (!writeBack && !isDatabaseFailed.get()) {
				// database is up
				if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, inviteeID))) {
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
					firstTimeDirty = !recovery.appendPendingFriendActionToLog(inviteeID, inviterID, false);
				} else {
					firstTimeDirty = recovery.addDirtyFlag(inviteeID);
				}
			}
			logger.debug("update database " + updateInviteeFailed + " update cache " + cacheUpdateFailed);
		} catch (Exception e) {
			logger.error("Reject friend failed " + inviterID + ", " + inviteeID, e);
		} finally {
			leaseClient.releaseLease(getUserLogLeaseKey(inviteeID), inviteeID);
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
			leaseClient.acquireTillSuccess(getUserLogLeaseKey(inviteeID), 
			    inviteeID, LEASE_TIMEOUT);

			if (memcachedClient.incr(KEY_PENDING_FRIEND_COUNT + inviteeID, 1L, 
			    inviteeID) == -1) {
				cacheUpdateFailed = true;
			}
			
			if (!MemcachedSetHelper.addToSet(memcachedClient, 
			    KEY_LIST_FRIENDS_REQUEST + inviteeID, inviteeID,
					String.valueOf(inviterID), false)) {
				cacheUpdateFailed = true;
			}

			if (!writeBack && !isDatabaseFailed.get()) {
				// database is up
				if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, inviteeID))) {

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
					firstTimeDirty = !recovery.appendPendingFriendActionToLog(inviteeID, inviterID, true);
				} else {
					firstTimeDirty = recovery.addDirtyFlag(inviteeID);
				}
			}

			logger.debug("update database " + updateInviteeFailed + " update cache " + cacheUpdateFailed);
		} catch (Exception e) {
			logger.error("Invite friend failed " + inviterID + ", " + inviteeID, e);
		} finally {
			leaseClient.releaseLease(getUserLogLeaseKey(inviteeID), inviteeID);
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
			leaseClient.acquireTillSuccess(getUserLogLeaseKey(inviterID), 
			    inviterID, LEASE_TIMEOUT);

			if (memcachedClient.decr(KEY_FRIEND_COUNT + inviterID, 1L, inviterID) == -1) {
				cacheUpdateFailed = true;
			}
			if (!MemcachedSetHelper.removeFromSet(memcachedClient, KEY_LIST_FRIENDS + inviterID, inviterID, 
					String.valueOf(inviteeID))) {
				cacheUpdateFailed = true;
			}

			if (!writeBack && !isDatabaseFailed.get()) {
				if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, inviterID))) {
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
					firstTimeDirty = !recovery.appendFriendActionToLog(inviterID, inviteeID, false);
				} else {
					firstTimeDirty = recovery.addDirtyFlag(inviterID);
				}
			}

			logger.debug("update database " + updateInviterFailed + " update cache " + cacheUpdateFailed);
		} catch (Exception e) {
			logger.error("Thaw friend, update inviter failed " + inviterID + ", " + inviteeID, e);
		} finally {
			leaseClient.releaseLease(getUserLogLeaseKey(inviterID), inviterID);
			if (updateInviterFailed) {
				insertToEWList(firstTimeDirty, inviterID);
			}
		}
		return updateInviterFailed ? DATABASE_FAILURE : 0;
	}

	private void insertToEWList(boolean firstTimeDirty, int userId) {
		if (firstTimeDirty) {
			leaseClient.acquireTillSuccess(
			    getEWLogMutationLeaseKeyFromUserId(userId), 
			    TardisClientConfig.getEWId(userId), LEASE_TIMEOUT);
			recovery.insertUserToPartitionLog(userId);
			leaseClient.releaseLease(getEWLogMutationLeaseKeyFromUserId(userId), 
			    TardisClientConfig.getEWId(userId));
		}
	}

	private int thawFriendInvitee(int inviterID, int inviteeID) {
		boolean updateInviteeFailed = false;
		boolean cacheUpdateFailed = false;
		boolean firstTimeDirty = false;
		try {
			leaseClient.acquireTillSuccess(getUserLogLeaseKey(inviteeID), 
			    inviteeID, LEASE_TIMEOUT);

			if (memcachedClient.decr(KEY_FRIEND_COUNT + inviteeID, 1L, inviteeID) == -1) {
				cacheUpdateFailed = true;
			}
			if (!MemcachedSetHelper.removeFromSet(memcachedClient, KEY_LIST_FRIENDS + inviteeID, inviteeID, 
					String.valueOf(inviterID))) {
				cacheUpdateFailed = true;
			}
			if (!writeBack && !isDatabaseFailed.get()) {
				if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, inviteeID))) {
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
					firstTimeDirty = !recovery.appendFriendActionToLog(inviteeID, inviterID, false);
				} else {
					firstTimeDirty = recovery.addDirtyFlag(inviteeID);
				}
			}

			logger.debug("update database " + updateInviteeFailed + " update cache " + cacheUpdateFailed);
		} catch (Exception e) {
			logger.error("Thaw friend, update invitee failed " + inviterID + ", " + inviteeID, e);
		} finally {
			leaseClient.releaseLease(getUserLogLeaseKey(inviteeID), inviteeID);
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
		// return new HashMap<>();
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
