package mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import tardis.TardisClientConfig;

import com.meetup.memcached.CLValue;
import com.meetup.memcached.IQException;
import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;
import static tardis.TardisClientConfig.KEY_FRIEND_COUNT;
import static tardis.TardisClientConfig.KEY_LIST_FRIENDS;
import static tardis.TardisClientConfig.KEY_PENDING_FRIEND_COUNT;
import static tardis.TardisClientConfig.KEY_LIST_FRIENDS_REQUEST;
import static tardis.TardisClientConfig.KEY_VIEW_PROFILE;
import static tardis.TardisClientConfig.PAGE_SIZE_FRIEND_LIST;
import static tardis.TardisClientConfig.TARDIS_MODE;
import static tardis.TardisClientConfig.WRITE_SET_PROP;
import static mongodb.RecoveryCaller.READ;
import static edu.usc.bg.workloads.CoreWorkload.DATABASE_FAILURE;

public abstract class MemMongoClient extends DB {
	public static final int SUCCESS = 0;
	public static final int FAIL = -1;

	public static final int ACT_VIEW_PROFILE = 1;
	public static final int ACT_LIST_FRIENDS = 2;
	public static final int ACT_PENDING_FRIENDS = 3;

	public static final int ACT_INV_FRIEND = 4;
	public static final int ACT_REJ_FRIEND = 5;
	public static final int ACT_ACP_FRIEND_INVITER = 6;
	public static final int ACT_ACP_FRIEND_INVITEE = 7;
	public static final int ACT_THW_FRIEND = 8;

	private static long start = System.currentTimeMillis();

	public static final int ACT_AR = 0;		// for active recovery (background worker thread)

	public static final int Q_LEASE_BACKOFF_TIME = 50;

	public static boolean READ_RECOVER = false;
	public static boolean WRITE_RECOVER = false; 
	public static boolean READ_RECOVER_ALWAYS = true;
	public static boolean WRITE_SET = false;

	MemcachedClient mc;
	static MongoBGClientDelegate client = null;

	public static final AtomicLong cacheHits = new AtomicLong(0);
	public static final AtomicLong cacheMisses = new AtomicLong(0);
	public static final AtomicLong numReads = new AtomicLong(0);
	public static final AtomicLong numSessRetriesInWrites = new AtomicLong(0);
	public static final AtomicLong numSessRetriesInReads = new AtomicLong(0);

	public static final AtomicLong numUpdatesOnReads = new AtomicLong(0);
	public static final AtomicLong numUpdatesOnWrites = new AtomicLong(0);
	public static final AtomicLong numUpdatesOnARs = new AtomicLong(0);

	public static final AtomicLong numSessRetriesInARs = new AtomicLong(0);

	public static final AtomicLong totalLFRoundtripsToCache = new AtomicLong(0);
	public static final AtomicLong totalLFs = new AtomicLong(0);
	public static final AtomicLong numOfUpdatesInLFs = new AtomicLong(0);
	public static final AtomicLong queryDB_VF = new AtomicLong(0);
	public static final AtomicLong queryDB_LF = new AtomicLong(0);
	public static final AtomicLong queryDB_PF = new AtomicLong(0);

	public Random rand = new Random();

	private static EWStatsWatcher ewStatsWatcher;

	byte[] read_buffer = new byte[1024*5];
	private static String[] serverlist;
	private static boolean fullWarmUp = true;
	private static int numUserInCache = -1;
	private static boolean loadCache = false;

	private static Semaphore initLock = new Semaphore(1);
	private static boolean isInit = false;
	private static DatabaseStateSimulator dbStateSimulator;

	static CLActiveRecoveryWorker[] arWorkers = null;

	private static ExecutorService executor = null;

	public static final AtomicBoolean isDatabaseFailed = new AtomicBoolean(false);
	public static final AtomicInteger dirtyDocs = new AtomicInteger(0);

	public static final boolean LIMIT_FRIENDS = true;
	private static boolean SIMPLE_LIST_FRIENDS = true;

	public static int NUM_ACTIVE_RECOVERY_WORKERS = 0;

	private static AtomicInteger threads = new AtomicInteger(0);

	protected static boolean writeBack = false;
	private static final boolean disjoint = true;
	private static int machineid;

	public MemMongoClient() {
		Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
		mongoLogger.setLevel(Level.SEVERE); 
	}

	private void resetStats() {
		numReads.set(0);
		cacheHits.set(0);
		cacheMisses.set(0);
		totalLFs.set(0);
		totalLFRoundtripsToCache.set(0);
		numOfUpdatesInLFs.set(0);
		queryDB_VF.set(0);
		queryDB_LF.set(0);
		queryDB_PF.set(0);
	}

	@Override
	public boolean init() throws DBException {
		try {
			initLock.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		resetStats();

		if (!isInit) {
			System.out.println(String.format("MongoDB IP: %s", getProperties().getProperty("mongoip")));
			System.out.println(String.format("Twemcached IP: %s", getProperties().getProperty("twemcachedip")));

			if (getProperties().getProperty("limitfriends") != null) {
				PAGE_SIZE_FRIEND_LIST = Integer.parseInt(getProperties().getProperty("limitfriends"));
			}
			System.out.println("Limit friends = " + LIMIT_FRIENDS);
			System.out.println("Page Size Friend list = " + PAGE_SIZE_FRIEND_LIST);

			if (getProperties().getProperty(ReconConfig.KEY_FULL_WARM_UP) != null) {
				fullWarmUp = Boolean.parseBoolean(getProperties().getProperty(ReconConfig.KEY_FULL_WARM_UP));
			}
			if (getProperties().getProperty(ReconConfig.KEY_NUM_USER_IN_CACHE) != null) {
				numUserInCache = Integer.parseInt(getProperties().getProperty(ReconConfig.KEY_NUM_USER_IN_CACHE));
			}

			if (getProperties().getProperty("machineid") != null) {
				machineid = Integer.parseInt(getProperties().getProperty("machineid"));
			}

			if (getProperties().getProperty("writeback") != null) {
				writeBack = Boolean.parseBoolean(getProperties().getProperty("writeback"));				
			}

			if (!writeBack) {
				if (getProperties().getProperty(TARDIS_MODE) != null) {
					String tardisMode = getProperties().getProperty(TARDIS_MODE);
					switch (tardisMode) {
					case "tar":
						READ_RECOVER = false;
						WRITE_RECOVER = false;
						break;
					case "tard_reads":
						READ_RECOVER_ALWAYS = true;
						READ_RECOVER = true;
						WRITE_RECOVER = false;
						break;
					case "tard":
						READ_RECOVER = true;
						WRITE_RECOVER = false;
						break;
					case "dis":
						READ_RECOVER = false;
						WRITE_RECOVER = true;
						break;
					case "tardis":
					default:
						READ_RECOVER = true;
						WRITE_RECOVER = true;
						break;
					}
				} else {
					READ_RECOVER = true;
					WRITE_RECOVER = true;
				}
			}
			
			if (getProperties().getProperty(WRITE_SET_PROP) != null) {
				WRITE_SET = Boolean.parseBoolean(getProperties().getProperty(WRITE_SET_PROP));
			}			
			
			System.out.println("Machine id = "+machineid);
			System.out.println("Write Back = " + writeBack);
			System.out.println("READ_RECOVER = "+READ_RECOVER);
			System.out.println("WRITE_RECOVER = "+WRITE_RECOVER);

			if (getProperties().getProperty("simplelistfriends") != null) {
				SIMPLE_LIST_FRIENDS = 
						Boolean.parseBoolean(getProperties().getProperty("simplelistfriends"));
			}
			System.out.println("SIMPLE LIST FRIENDS = " + SIMPLE_LIST_FRIENDS);

			if (getProperties().getProperty("snapshot") != null) {
				TardisClientConfig.SNAPSHOT = Boolean.parseBoolean(getProperties().getProperty("snapshot"));
			}
			System.out.println("Snapshot = " + TardisClientConfig.SNAPSHOT);

			String[] originServerlist = getProperties().getProperty("twemcachedip").split(",");

			int numCacheServers = originServerlist.length;
			if (getProperties().getProperty("numcacheservers") != null) {
				numCacheServers = Integer.parseInt(getProperties().getProperty("numcacheservers"));
				numCacheServers = numCacheServers > originServerlist.length ?
						originServerlist.length : numCacheServers;
			}

			int cps = Integer.parseInt(getProperties().getProperty("cachesperserver"));
			serverlist = new String[numCacheServers*cps];
			for (int i = 0; i < numCacheServers; i++) {
				if (!originServerlist[i].contains(":")) {
					int port = 11211;
					for (int j = 0; j < cps; j++) {
						serverlist[i*cps+j] = originServerlist[i] + ":" + port;
						port++;
					}
				} else {
					serverlist[i] = originServerlist[i];
				}
			}

			System.out.println("Cache Servers: "+ Arrays.toString(serverlist));
			System.out.println("Num of EWs: "+ TardisClientConfig.NUM_EVENTUAL_WRITE_LOGS);

			//Integer[] weights = {1,1,1,1};

			// initialize the pool for memcache servers
			SockIOPool pool = SockIOPool.getInstance( "BG" );
			pool.setServers( serverlist );
			//pool.setWeights( weights );

			pool.setInitConn(100);
			pool.setMinConn(100);
			pool.setMaxConn( 200 );
			pool.setMaintSleep(0);
			pool.setNagle( false );
			//			pool.setHashingAlg( SockIOPool.CONSISTENT_HASH );
			pool.initialize();

			if (client == null) {
				client = new MongoBGClientDelegate(getProperties().getProperty("mongoip"), isDatabaseFailed);
				client.setProperties(getProperties());
				client.init();
			}

			if (getProperties().getProperty("numarworker") != null) {
				NUM_ACTIVE_RECOVERY_WORKERS = Integer.parseInt(getProperties().getProperty("numarworker"));
			}

			int alpha = 0;
			if (getProperties().getProperty("alpha") != null) {
				alpha = Integer.parseInt(getProperties().getProperty("alpha"));
				System.out.println("Alpha = "+alpha);
			}			

			if (NUM_ACTIVE_RECOVERY_WORKERS > 0 && arWorkers == null) {
				System.out.println("Start "+NUM_ACTIVE_RECOVERY_WORKERS+" AR workers...");
				arWorkers = new CLActiveRecoveryWorker[NUM_ACTIVE_RECOVERY_WORKERS];
				CLActiveRecoveryWorker.isRunning = true;
				for (int i = 0; i < arWorkers.length; i++) {
					arWorkers[i] = new CLActiveRecoveryWorker(client, alpha, serverlist);
					arWorkers[i].setName("Active Recovery Worker "+i);
					arWorkers[i].start();
				}			
			}

			long[] longIntervals = null;
			if (getProperties().getProperty("dbfail") != null) {
				String str = getProperties().getProperty("dbfail");
				String[] tokens = str.split(",");
				longIntervals = new long[tokens.length];
				for (int i = 0; i < tokens.length; i++) {
					longIntervals[i] = Long.parseLong(tokens[i]);
				};	
				System.out.println("DB Fail from "+longIntervals[0]+" to "+longIntervals[1]);

				for (int i = 0; i < longIntervals.length; i += 2) {
					ArrayMetrics.getInstance().add(MetricsName.METRICS_DATABASE_DOWN_TIME,
							new DatabaseFailureRecord(longIntervals[i], longIntervals[i + 1]));
				}
			}

			if (executor == null || executor.isShutdown()) {
				executor = Executors.newFixedThreadPool(5);
			}

			if (getProperties().getProperty("ewstats") != null) {
				boolean ewstats = Boolean.parseBoolean(getProperties().getProperty("ewstats"));
				if (ewstats == true) {
					int numUsers = Integer.parseInt(getProperties().getProperty("usercount"));
					ewStatsWatcher = new EWStatsWatcher(numUsers);
					executor.submit(ewStatsWatcher);
				}
			}

			if (longIntervals != null) {
				System.out.println("Start the database state simulator..."+longIntervals);				
				dbStateSimulator = new DatabaseStateSimulator(mc, isDatabaseFailed, longIntervals);
				executor.submit(dbStateSimulator);
			}
		}

		isInit  = true;

		initLock.release();

		String[] serverlist = { getProperties().getProperty(ReconConfig.KEY_TWEMCACHED_IP) + ":11211" };

		// set up cache connections
		//		mc = MemcachedMongoBGClient.getMemcachedClient(serverlist, "BG");
		mc = new MemcachedClient("BG");
		threads.incrementAndGet();

		return true;
	}

	private CLValue getILeaseAndRecover(
			MemcachedClient mc, String key, 
			int id, int action) throws DatabaseFailureException {
		CLValue val = null;

		while (true) {
			try {
				val = mc.ewget(key, getHashCode(id));
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (val == null) {
				System.out.println("Got a null value");
				System.exit(-1);
			}

			if (!val.isPending())
				break;

			boolean recover = true;

			if ((!writeBack || (writeBack && val.getValue() == null)) 
					&& READ_RECOVER && !isDatabaseFailed.get()) {

				while (true) {
					String tid = mc.generateSID();
					try {					
						CLActiveRecoveryWorker.docRecover(mc, tid, 
								id, client, null, READ, action);
						mc.ewcommit(tid, getHashCode(id), !recover);
						numUpdatesOnReads.incrementAndGet();
						break;
					} catch (DatabaseFailureException e) {
						recover = false;
						mc.ewcommit(tid, getHashCode(id), !recover);
						break;
					} catch (IQException e) {
						numSessRetriesInReads.incrementAndGet();
						try {
							mc.release(tid, getHashCode(id));
						} catch (Exception ex) {
							// TODO Auto-generated catch block
							ex.printStackTrace();
						}
					}

					try {
						Thread.sleep(Q_LEASE_BACKOFF_TIME);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//			recover = false;
			} else {
				recover = false;
			}

			if (val.getValue() != null)
				break;

			if (!recover && val.getValue() == null) {				
				throw new DatabaseFailureException();
			}
		}

		return val;
	}
	
	public static final boolean stats=true;

	@Override
	public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
			boolean insertImage, boolean testMode) {
		if (stats)
			numReads.incrementAndGet();

		String kpfcount = TardisClientConfig.KEY_PENDING_FRIEND_COUNT + profileOwnerID;
		String kcfcount = TardisClientConfig.KEY_FRIEND_COUNT + profileOwnerID;
		String kprofile = TardisClientConfig.KEY_VIEW_PROFILE + profileOwnerID;

		int numKeys = (requesterID == profileOwnerID) ? 3 : 2;
		String[] keys = new String[numKeys];
		keys[0] = kprofile;
		keys[1] = kcfcount;

		if (numKeys == 3) {
			keys[2] = kpfcount;
		}

		Integer[] hcs = new Integer[keys.length];
		for (int i = 0; i < hcs.length; i++) hcs[i] = profileOwnerID;
		Map<String, Object> map = mc.getMulti(getHashCode(profileOwnerID), false, keys);

		boolean cacheMiss = false;

		if (map.get(keys[0]) != null) {
			CacheUtilities.unMarshallHashMap(result, (byte[])map.get(keys[0]), read_buffer);
		} else {
			cacheMiss = true;
//			System.out.println("Miss key "+keys[0]);
		}

		if (map.get(keys[1]) != null) {
			result.put("friendcount", new ObjectByteIterator(((String)map.get(keys[1])).getBytes()));
		} else {
			cacheMiss = true;
//			System.out.println("Miss key "+keys[1]);
		}

		if (requesterID == profileOwnerID) {
			if (map.get(keys[2]) != null) {
				result.put("pendingcount", new ObjectByteIterator(((String)map.get(keys[2])).getBytes()));
			} else {
				cacheMiss = true;
//				System.out.println("Miss key "+keys[2]);
			}
		}

		if (!cacheMiss) {
			if (stats) cacheHits.incrementAndGet();
			return SUCCESS;
		} else {
			if (stats) cacheMisses.incrementAndGet();
		}

		CLValue vpfcount = null, vcfcount = null, vprofile = null;

		while (true) {
			while (true) {
				if (requesterID == profileOwnerID) {
					try {
						vpfcount = mc.ewget(kpfcount, getHashCode(profileOwnerID));
					} catch (IOException e) {
						e.printStackTrace();
					}					
					if (vpfcount == null) return -1;
				}

				try {
					vcfcount = mc.ewget(kcfcount, getHashCode(profileOwnerID));
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				if (vcfcount == null) return -1;

				try {
					vprofile = mc.ewget(kprofile, getHashCode(profileOwnerID));
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				if (vprofile == null) return -1;

				Boolean recover = null;
				if ((requesterID == profileOwnerID && vpfcount.isPending()) || 
						vcfcount.isPending() || vprofile.isPending()) {		// do recover
					if ((!writeBack 
							|| (writeBack && (vprofile.getValue() == null || vcfcount.getValue() == null || (requesterID == profileOwnerID && vpfcount.getValue() == null)))
							) 
							&& READ_RECOVER && !isDatabaseFailed.get()) {

						//            Set<Integer> hashCodes = new HashSet<>();
						//            hashCodes.add(profileOwnerID);

						while (true) {
							String tid = mc.generateSID();
							try {
								CLActiveRecoveryWorker.docRecover(mc, tid, 
										profileOwnerID, client, 
										null, READ, ACT_VIEW_PROFILE);
								recover = true;

								mc.ewcommit(tid, getHashCode(profileOwnerID), !recover);
								numUpdatesOnReads.incrementAndGet();
								break;
							} catch (DatabaseFailureException e) {
								recover = false;
								mc.ewcommit(tid, getHashCode(profileOwnerID), !recover);
								break;
							} catch (IQException e) { 
								numSessRetriesInReads.incrementAndGet();
								try {
									mc.release(tid, getHashCode(profileOwnerID));
								} catch (Exception ex) {
									// TODO Auto-generated catch block
									ex.printStackTrace();
								}
							}

							try {
								Thread.sleep(Q_LEASE_BACKOFF_TIME);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					} else {
						recover = false;
					}
				}

				if (recover == null) break;	// no recover happens

				if (!(vprofile.getValue() == null || vcfcount.getValue() == null || 
						(requesterID == profileOwnerID && vpfcount.getValue() == null))) 
					break;

				mc.releaseILeases(getHashCode(profileOwnerID));
				if (!recover)
					return DATABASE_FAILURE;				
			}

			if (vprofile.getValue() == null || vcfcount.getValue() == null || 
					(requesterID == profileOwnerID && vpfcount.getValue() == null)) {
				// query MongoDB
				if (!isDatabaseFailed.get()) {
					if (client.viewProfile(requesterID, profileOwnerID, result, 
							insertImage, testMode) == DATABASE_FAILURE) {
						mc.releaseILeases(getHashCode(profileOwnerID));						
						return DATABASE_FAILURE;
					}
				} else {
					mc.releaseILeases(getHashCode(profileOwnerID));
					return DATABASE_FAILURE;
				}

				boolean iqset = true;

				// store result to cache
				ByteIterator cf = result.get("friendcount");			
				if (vcfcount.getValue() == null) {
					try {
						mc.iqset(kcfcount, new String(cf.toArray()), 
								getHashCode(profileOwnerID));
					} catch (IOException | IQException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						iqset = false;
					}
				}

				ByteIterator pf = result.get("pendingcount");	
				if (requesterID == profileOwnerID) {	
					if (vpfcount.getValue() == null) {
						try {
							mc.iqset(kpfcount, new String(pf.toArray()),
									getHashCode(profileOwnerID));
						} catch (IOException | IQException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							iqset = false;
						}
					}
				}

				if (vprofile.getValue() == null) {
					try {
						mc.iqset(kprofile, CacheUtilities.SerializeHashMap(result),
								getHashCode(profileOwnerID));
					} catch (IOException | IQException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						iqset = false;
					}
				}

				if (iqset == false) {
					// cannot consume the value
					result.clear();
					continue;
				}
			} else {			
				CacheUtilities.unMarshallHashMap(result, (byte[])vprofile.getValue(), read_buffer);
				result.put("friendcount", new ObjectByteIterator(((String)vcfcount.getValue()).getBytes()));				
				if (requesterID == profileOwnerID) {
					result.put("pendingcount", new ObjectByteIterator(((String)vpfcount.getValue()).getBytes()));
				}
			}

			break;
		}

		return SUCCESS;
	}

	private List<String> getUserIds(MemcachedClient mc, MongoBGClient client, 
			int id, String key, int action) throws DatabaseFailureException {
		List<String> userSet = null;

		CLValue val = null;   
		while (true) {
			try {
				try {
					val = getILeaseAndRecover(mc, key, id, action);
				} catch (DatabaseFailureException e) {
					mc.releaseILeases(getHashCode(id));
					throw e;
				}

				if (val == null) return null;

				if (val.getValue() == null) {
					cacheMisses.incrementAndGet();

					// get doc and then set the value in the cache.
					List<String> fs = null;

					if (!isDatabaseFailed.get()) {
						if (key.contains(TardisClientConfig.KEY_LIST_FRIENDS)) {
							fs = client.listFriends(id);
							if (fs != null) queryDB_LF.incrementAndGet();
						} else if (key.contains(TardisClientConfig.KEY_LIST_FRIENDS_REQUEST)) {
							fs = client.listPendingFriends(id);
							if (fs != null) queryDB_PF.incrementAndGet();
						}
					}

					if (fs == null) {
						mc.releaseILeases(getHashCode(id));
						throw new DatabaseFailureException();
					}

					String users = MemcachedSetHelper.convertString(fs);
					userSet = MemcachedSetHelper.convertList(users);

					try {
						mc.iqset(key, users, getHashCode(id));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {
					cacheHits.incrementAndGet();

					Object obj = val.getValue();
					userSet = MemcachedSetHelper.convertList(obj);
				}

				break;
			} catch (IQException e1) { }

			try {
				Thread.sleep(Q_LEASE_BACKOFF_TIME);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return userSet;
	}

	//	private int getMultiProfilesFromPStore(int requestId, Set<String> ids, 
	//	    Vector<HashMap<String, ByteIterator>> result) {
	//	  return SUCCESS;
	//	}

	private int getUserListNewAlgorithm(int requestId, int id, String key, 
			Vector<HashMap<String, ByteIterator>> result, int action) {
		List<String> userList = null;
		try {
			userList = getUserIds(mc, client, id, key, action);
		} catch (DatabaseFailureException e) {
			return DATABASE_FAILURE;
		}

		// construct the mapping <cache_server, friend_ids>
		Map<Integer, Set<String>> friendsMap = new HashMap<>();
		for (String user: userList) {
			int hashCode = Integer.valueOf(user) % serverlist.length;

			Set<String> set = friendsMap.get(hashCode);
			if (set == null) {
				set = new HashSet<>();
				friendsMap.put(hashCode, set);
			}
			set.add(user);
		}

		int Tprime = PAGE_SIZE_FRIEND_LIST;
		int rtt = 0;
		final int THRESHOLD = 1;

		// list of cache-miss users
		List<String> misses = new ArrayList<>();

		// list of cache-hit users
		List<String> hits = new ArrayList<>();

		Map<Integer, Set<Integer>> sizeMap = 
				new TreeMap<Integer, Set<Integer>>(new Comparator<Integer>() {

					@Override
					public int compare(Integer o1, Integer o2) {
						return o2.intValue() - o1.intValue();
					}
				});  
		List<String> multigetList = new ArrayList<>();

		while (friendsMap.size() > 0) {
			rtt++;

			sizeMap.clear();
			multigetList.clear();

			// reconstruct sizeMap <size, cache_servers> 
			for (Integer cacheHashCode: friendsMap.keySet()) {
				int size = friendsMap.get(cacheHashCode).size();
				Set<Integer> cacheSet = sizeMap.get(size);
				if (cacheSet == null) {
					cacheSet = new HashSet<>();
					sizeMap.put(size, cacheSet);
				}
				cacheSet.add(cacheHashCode);
			}

			// get servers (sorted by decreasing of number of friend ids)
			List<Integer> servers = new ArrayList<>();
			for (Map.Entry<Integer, Set<Integer>> entry: sizeMap.entrySet()) {
				servers.addAll(entry.getValue());
			}

			Set<List<Integer>> options = new HashSet<>();
			List<Integer> list = new ArrayList<>();
			List<Integer> index = new ArrayList<>();
			int i = 0;
			int min = Integer.MAX_VALUE;
			int total = 0;
			while (i < servers.size()) {
				Integer server = servers.get(i);

				list.add(server);
				index.add(i);
				total += friendsMap.get(server).size();

				if (total >= PAGE_SIZE_FRIEND_LIST) {
					// add the list as an option
					options.add(list);
					min = list.size();

					// make a copy of the list and remove the last added
					List<Integer> newList = new ArrayList<Integer>();
					for (int j = 0; j < list.size()-1; j++) {
						newList.add(list.get(j));
					}

					list = newList;
					total -= friendsMap.get(server).size();
					index.remove(index.size()-1);
				} else {
					if (list.size() > min) {
						// remove two items
						Integer removed = list.remove(list.size()-1);
						index.remove(index.size()-1);
						total -= friendsMap.get(removed).size();

						if (list.size() == 0) break;
						removed = list.remove(list.size()-1);
						total -= friendsMap.get(removed).size();
						i = index.remove(index.size()-1);
					}
				}

				i++;
			}

			if (min == Integer.MAX_VALUE) { // the current list is the best one so far
				options.add(list);
			}

			min = Integer.MAX_VALUE;
			for (List<Integer> option: options) {
				TreeMap<Integer, String> index2Ids = 
						new TreeMap<Integer, String>(new Comparator<Integer>() {

							@Override
							public int compare(Integer o1, Integer o2) {
								return o2.intValue() - o1.intValue();
							}
						});

				for (Integer cache: option) {
					Set<String> ids = friendsMap.get(cache);
					for (String friendid: ids) {
						int idx = userList.indexOf(friendid);
						index2Ids.put(idx, friendid);
					}
				}

				int cnt = 0;
				int cost = 0;
				List<String> multiget = new ArrayList<>();
				for (Map.Entry<Integer, String> entry: index2Ids.entrySet()) {
					cost += entry.getKey().intValue();
					multiget.add(entry.getValue());
					if (++cnt == PAGE_SIZE_FRIEND_LIST) {
						break;
					}
				}

				if (cost < min) {
					min = cost;
					multigetList = multiget;
				}
			}

			if (multigetList.size() == 0) {
				break;
			}

			// at this time, got Tprime users to query
			String[] keys = new String[2*multigetList.size()];
			Integer[] hashCodes = new Integer[keys.length];
			for (i = 0; i < multigetList.size();i++) {
				String userid = multigetList.get(i);
				keys[2*i] = TardisClientConfig.KEY_VIEW_PROFILE + userid;
				keys[2*i+1] = TardisClientConfig.KEY_FRIEND_COUNT + userid;
				hashCodes[2*i] = Integer.valueOf(userid);
				hashCodes[2*i+1] = Integer.valueOf(userid);
			}

			totalLFRoundtripsToCache.addAndGet(
					new HashSet<>(getHashCodes(keys).values()).size());

			Object[] objs = mc.getMultiArray(keys, hashCodes);
			for (i = 0; i < multigetList.size(); i++) {
				String user = multigetList.get(i);
				HashMap<String, ByteIterator> hm = new HashMap<>();

				Object profile = objs[2*i];
				Object friendcnt = objs[2*i+1];
				if (profile != null && friendcnt != null) {
					hits.add(user);
					CacheUtilities.unMarshallHashMap(hm, (byte[])profile, read_buffer);
					hm.put("friendcount", new ObjectByteIterator(((String)friendcnt).getBytes()));
					result.add(hm);
				} else {
					misses.add(user);
				}
			}

			Tprime = PAGE_SIZE_FRIEND_LIST - result.size(); 
			if (Tprime == 0) {
				break;
			}

			// adjust friendsMap
			for (String user: multigetList) {
				int hashCode = getHashCode(user);
				Set<String> set = friendsMap.get(hashCode);
				if (set != null) {
					set.remove(user);
					if (set.size() == 0) {
						friendsMap.remove(hashCode);
					}
				}
			}  
		}

		boolean queryPStore = false;
		if (Tprime > 0 && misses.size() > 0) {
			int min = Math.min(Tprime, misses.size());

			//      Set<String> querySet = new HashSet<>();
			for (int i = 0; i < min; i++) {
				String targetId = misses.remove(0);
				HashMap<String, ByteIterator> hm = new HashMap<>();
				int success = viewProfile(id, Integer.valueOf(targetId), hm, false, false);
				if (success == SUCCESS) {
					result.add(hm);
					hits.add(targetId);
					queryPStore = true;
				} else {
					break;
				}
			}
		}

		if (queryPStore || rtt > THRESHOLD) {          // update list friends
			for (String user: hits) {
				userList.remove(user);
				userList.add(0, user);
			}

			for (String user: misses) {
				userList.remove(user);
				userList.add(userList.size(), user);
			}

			while (true) {
				String sid = mc.generateSID();
				//        Set<Integer> hcs = new HashSet<>();
				//        hcs.add(id);
				try {
					CLValue cVal = mc.ewread(sid, key, id, true);
					if (cVal != null && cVal.getValue() != null) {
						String val = (String) cVal.getValue();
						Set<String> newSet = MemcachedSetHelper.convertSet(val);

						Set<String> removed = new HashSet<>();
						for (String item: userList) {
							if (!newSet.contains(item)) {
								removed.add(item);
							}
						}
						for (String newItem: newSet) {
							if (!userList.contains(newItem)) {
								userList.add(newItem);
							}
						}
						for (String remove: removed) {
							userList.remove(remove);
						}
					}
					String ul = MemcachedSetHelper.convertString(userList);
					mc.ewswap(sid, key, id, ul);

					mc.ewcommit(sid, id, false);

					break;
				} catch (Exception e) {
					try {
						mc.release(sid, id);
					} catch (Exception ex) {
						// TODO Auto-generated catch block
						ex.printStackTrace();
					}
				}

				try {
					Thread.sleep(Q_LEASE_BACKOFF_TIME);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			numOfUpdatesInLFs.incrementAndGet();
		}

		return SUCCESS;
	}

	private int getUserList(int requestId, int id, String key, 
			Vector<HashMap<String, ByteIterator>> result, int action) {	
		List<String> userSet = null;

		try {
			userSet = getUserIds(mc, client, id, key, action); 
		} catch (DatabaseFailureException e) {
			return DATABASE_FAILURE;
		}

		if (userSet != null && userSet.size() > 0) {	
			List<String> newUserList = new ArrayList<>();

			if (LIMIT_FRIENDS) {				
				List<Integer> intList = new LinkedList<>();
				for (int i = 0; i < serverlist.length; i++) {
					intList.add(i);
				}

				int Tprime = PAGE_SIZE_FRIEND_LIST;
				while (intList.size() > 0) {
					int x = rand.nextInt(intList.size());
					Integer cacheId = intList.remove(x);

					newUserList.addAll(userSet.stream()
							.filter(s -> (Integer.parseInt(s) % serverlist.length == cacheId.intValue()))
							.limit(Tprime)
							.collect(Collectors.toList()));
					if (newUserList.size() < Tprime) {
						Tprime -= newUserList.size();
					} else {
						break;
					}
				}
			} else {
				newUserList = userSet;
			}

//			String[] keys = new String[2*newUserList.size()];
//			Integer[] hashCodes = new Integer[keys.length];
//			for (int i = 0; i < newUserList.size();i++) {
//				keys[2*i] = TardisClientConfig.KEY_VIEW_PROFILE + newUserList.get(i);
//				keys[2*i+1] = TardisClientConfig.KEY_FRIEND_COUNT + newUserList.get(i);
//				hashCodes[2*i] = Integer.valueOf(newUserList.get(i));
//				hashCodes[2*i+1] = Integer.valueOf(newUserList.get(i));
//			}
			
			Map<Integer, Set<String>> map = new HashMap<>();
			for (int i = 0; i < newUserList.size(); i++) {
				int hc = getHashCode(newUserList.get(i));
				Set<String> set = map.get(hc);
				if (set == null) {
					set = new HashSet<>();
					map.put(hc, set);
				}
				set.add(TardisClientConfig.KEY_VIEW_PROFILE + newUserList.get(i));
				set.add(TardisClientConfig.KEY_FRIEND_COUNT + newUserList.get(i));
			}
			
			if (map.size() != 1) {
				System.out.println("Map.size() = "+map.size());
			}
			
			totalLFRoundtripsToCache.addAndGet(map.size());
			
			Map<String, Object> res = new HashMap<>();
			for (Integer hc : map.keySet()) {
				String[] ks = map.get(hc).toArray(new String[0]);
				res.putAll(mc.getMulti(hc, false, ks));
			}
			
			for (int i = 0; i < newUserList.size(); i++) {
				String user = newUserList.get(i);
				Object profile = res.get(TardisClientConfig.KEY_VIEW_PROFILE + newUserList.get(i));
				Object friendcnt = res.get(TardisClientConfig.KEY_FRIEND_COUNT + newUserList.get(i));
				HashMap<String, ByteIterator> hm = new HashMap<>();

				if (profile != null && friendcnt != null) {
					CacheUtilities.unMarshallHashMap(hm, (byte[])profile, read_buffer);
					hm.put("friendcount", new ObjectByteIterator(((String)friendcnt).getBytes()));					
				} else {
					int friendId = Integer.parseInt(user);
					int success = viewProfile(requestId, friendId, hm, false, false);
					if (success == DATABASE_FAILURE) {
						return DATABASE_FAILURE;
					} else {
						queryDB_VF.incrementAndGet();
					}
				}
				result.add(hm);
			}
		}

		return SUCCESS;		
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
		numReads.incrementAndGet();
		totalLFs.incrementAndGet();

		String key = TardisClientConfig.KEY_LIST_FRIENDS + profileOwnerID;

		if (SIMPLE_LIST_FRIENDS)
			return getUserList(requesterID, profileOwnerID, 
					key, result, ACT_LIST_FRIENDS);
		else
			return getUserListNewAlgorithm(requesterID, profileOwnerID, 
					key, result, ACT_LIST_FRIENDS);
	}

	@Override
	public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> result, boolean insertImage,
			boolean testMode) {
		numReads.incrementAndGet();
		totalLFs.incrementAndGet();

		String key = TardisClientConfig.KEY_LIST_FRIENDS_REQUEST + profileOwnerID;

		if (SIMPLE_LIST_FRIENDS)
			return getUserList(profileOwnerID, profileOwnerID, 
					key, result, ACT_PENDING_FRIENDS);
		else
			return getUserListNewAlgorithm(profileOwnerID, profileOwnerID, 
					key, result, ACT_PENDING_FRIENDS);
	}

	@Override
	public abstract int inviteFriend(int inviterID, int inviteeID);

	@Override
	public abstract int rejectFriend(int inviterID, int inviteeID);

	public abstract boolean acceptFriendInviter(int inviterID, int inviteeID);

	public abstract boolean acceptFriendInvitee(int inviterID, int inviteeID);

	@Override
	public abstract int acceptFriend(int inviterID, int inviteeID);

	public abstract boolean thawFriend(int friendid1, int friendid2);

	@Override
	public abstract int thawFriendship(int friendid1, int friendid2);

	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {
		try {
			if (fullWarmUp) {
				mc.set(KEY_VIEW_PROFILE + entityPK, CacheUtilities.SerializeHashMap(values), Integer.parseInt(entityPK));
			}

			if (numUserInCache > 0 && !fullWarmUp) {
				if (Integer.parseInt(entityPK) < numUserInCache) {
					mc.set(KEY_VIEW_PROFILE + entityPK, CacheUtilities.SerializeHashMap(values), Integer.parseInt(entityPK));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return client.insertEntity(entitySet, entityPK, values, insertImage);
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		return client.viewTopKResources(requesterID, profileOwnerID, k, result);
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		return client.getCreatedResources(creatorID, result);
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		return client.viewCommentOnResource(requesterID, profileOwnerID, resourceID, result);
	}

	@Override
	public int postCommentOnResource(int commentCreatorID,
			int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		return client.postCommentOnResource(commentCreatorID, resourceCreatorID, resourceID, values);
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		return client.delCommentOnResource(resourceCreatorID, resourceID, manipulationID);
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		return client.getInitialStats();
	}

	@Override
	public void cleanup(boolean warmup) throws DBException {
		super.cleanup(warmup);

		if (threads.decrementAndGet() == 0) {
			printStats();

			if ( dbStateSimulator != null )
				dbStateSimulator.shutdown();
			if (ewStatsWatcher != null)
				ewStatsWatcher.shutdown();
			if (executor != null)
				executor.shutdown();

			if (!TimedMetrics.getInstance().getMetrics().isEmpty()) {
				String metricsFile = getProperties().getProperty(ReconConfig.KEY_METRICS_FILE_NAME);
				MetricsHelper.writeMetrics(metricsFile, getProperties(), start, System.currentTimeMillis(),
						TimedMetrics.getInstance(), ArrayMetrics.getInstance());
			}

			if (!loadCache && getProperties().getProperty("reconaction") != null &&
					getProperties().getProperty("reconaction").equals("load")) {
				int numUsers = Integer.parseInt(getProperties().getProperty("usercount"));
				int numFriendsPerUser = Integer.parseInt(getProperties().getProperty("friendcountperuser"));

				int max = 0;
				if (fullWarmUp) {
					max = numUsers;
				} else if (numUserInCache > 0) {
					max = numUserInCache > numUsers ? numUsers : numUserInCache;
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


					MemcachedSetHelper.set(mc, KEY_LIST_FRIENDS + i, new Integer(i), friends);
					try {
						mc.set(KEY_FRIEND_COUNT + i, String.valueOf(friends.size()), i);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace(System.out);
					}

					MemcachedSetHelper.set(mc, KEY_LIST_FRIENDS_REQUEST + i, i, new HashSet<String>());
					try {
						mc.set(KEY_PENDING_FRIEND_COUNT + i, "0", i);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace(System.out);
					}
				}			
				System.out.println("Finish load cache.");
				loadCache = true;
			}

			if (arWorkers != null) {
				CLActiveRecoveryWorker.isRunning = false;
				System.out.println("Set isRunning = "+CLActiveRecoveryWorker.isRunning);
				for (CLActiveRecoveryWorker ar : arWorkers) {
					try {
						ar.join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				arWorkers = null;
			} else {
				if ( dbStateSimulator != null )
					dbStateSimulator.shutdown();
				if (ewStatsWatcher != null)
					ewStatsWatcher.shutdown();
				executor.shutdown();
			}

			if (client != null) {
				client.cleanup(warmup);
				client = null;
			}

			isInit = false;
		}
	}

	public static void printStats() {
		System.out.println("Num Updates In Reads: "+numUpdatesOnReads.get());
		System.out.println("Num Updates In Writes: "+numUpdatesOnWrites.get());
		System.out.println("Num Updates In ARs: "+numUpdatesOnARs.get());
		System.out.println("Num Sess Retries In Reads: "+numSessRetriesInReads.get());
		System.out.println("Num Sess Retries In Writes: "+numSessRetriesInWrites.get());
		System.out.println("Num Sess Retries In ARs: "+numSessRetriesInARs.get());
		System.out.println("Num Reads: "+numReads.get());
		System.out.println("Cache Hits: "+cacheHits.get());
		System.out.println("Cache Misses: "+cacheMisses.get());
		System.out.println("Total roundtrips to cache: "+ totalLFRoundtripsToCache.get());
		System.out.println("Total LFs: "+ totalLFs.get());
		System.out.println("Num updates in LFs:" + numOfUpdatesInLFs.get());

		if (totalLFs.get() != 0) {
			System.out.println("Average roundtrips to cache:" + ((double)totalLFRoundtripsToCache.get() / totalLFs.get()));
			System.out.println("Average updates in LFs:" + ((double)numOfUpdatesInLFs.get() / totalLFs.get()));
		} 

		System.out.println("queryDB_VF: "+queryDB_VF.get());
		System.out.println("queryDB_LF: "+queryDB_LF.get());
		System.out.println("queryDB_PF: "+queryDB_PF.get());
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		return client.CreateFriendship(friendid1, friendid2);
		//		return 0;
	}

	protected static int getHashCode(String key) {
		return disjoint ? machineid : Integer.parseInt(key.replaceAll("[^0-9]", "")) % serverlist.length;
	}

	protected static int getHashCode(int x) {
		return disjoint ? machineid : x;
	}

	protected static Map<String, Integer> getHashCodes(String[] keys) {
		Map<String, Integer> hashCodes = new HashMap<>();

		for (int i = 0; i < keys.length; i++) {
			hashCodes.put(keys[i], getHashCode(keys[i]));
		}

		return hashCodes;
	}

	@Override
	public void createSchema(Properties props) {
		client.createSchema(props);
	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		return client.queryPendingFriendshipIds(memberID, pendingIds);
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {
		return client.queryConfirmedFriendshipIds(memberID, confirmedIds);
	}

	public void sleepRetryWrite() {
		try {
			Thread.sleep(Q_LEASE_BACKOFF_TIME);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}