package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.MemcachedLease;
import com.meetup.memcached.SockIOPool;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import static com.yahoo.ycsb.db.TardisYCSBConfig.*;

/**
 * MongoDB binding for YCSB framework using the MongoDB Inc. <a
 * href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 * 
 * @author ypai
 * @see <a href="http://docs.mongodb.org/ecosystem/drivers/java/">MongoDB Inc.
 *      driver</a>
 */
public abstract class CADSMongoDbClient extends DB {
	// mongodb client
	protected static MongoDbClientDelegate client;
	protected MemcachedClient mc;
	
	protected static boolean READ_RECOVER = true;
	protected static boolean WRITE_RECOVER = true;
	protected static boolean READ_RECOVER_ALWAYS = false;
	protected static boolean WRITE_SET = false;

	public final static AtomicLong cacheMisses = new AtomicLong(0);
	public final static AtomicLong cacheHits = new AtomicLong(0);
	public final static AtomicLong numDocsRecovered = new AtomicLong(0);
	public final static AtomicLong numDocsRecoveredInReads = new AtomicLong(0);
	public final static AtomicLong numDocsRecoveredInUpdates = new AtomicLong(0);
	public final static AtomicLong numDocsRecoveredInARs = new AtomicLong(0);
	public final static AtomicLong numSessRetriesInWrites = new AtomicLong(0);
	public final static AtomicLong numSessRetriesInARs = new AtomicLong(0);
	public final static AtomicLong numSessRetriesInReads = new AtomicLong(0);
	public final static AtomicLong findNoBuff = new AtomicLong();

	public final static AtomicLong numLeasesGranted = new AtomicLong(0);  

	private static AtomicBoolean isInit = new AtomicBoolean(false);
	private static Semaphore initLock = new Semaphore(1);

	private static EWStatsWatcher ewStatsWatcher;

	protected byte[] read_buffer = new byte[1024*10];

	protected static int cacheMode = 0;

	private static DBSimulator dbStateSimulator = null;

	private static ExecutorService executor = null;

	private static Thread[] arWorkers  = null;

	private static int NUM_ACTIVE_RECOVERY_WORKERS = 10;

	public static AtomicInteger threadCount = new AtomicInteger(0);

	private final Logger logger = Logger.getLogger(CADSMongoDbClient.class);

	public CADSMongoDbClient() {
		java.util.logging.Logger mongoLogger = java.util.logging.Logger.getLogger( "org.mongodb.driver" );
		mongoLogger.setLevel(java.util.logging.Level.SEVERE);

		ConsoleAppender console = new ConsoleAppender(); // create appender
		// configure the appender
		String PATTERN = "%d [%p|%c|%C{1}] %m%n";
		console.setLayout(new PatternLayout(PATTERN));
		console.setThreshold(Level.ERROR);
		console.activateOptions();
		// add appender to any Logger (here is root)
		Logger.getRootLogger().addAppender(console);

		Logger.getLogger(MongoDbClientDelegate.class).setLevel(Level.ERROR);
		Logger.getLogger(MemcachedSetHelper.class).setLevel(Level.ERROR);
		Logger.getLogger(MemcachedClient.class).setLevel(Level.ERROR);
		Logger.getLogger(SockIOPool.class).setLevel(Level.ERROR);
		Logger.getLogger(SockIOPool.SockIO.class).setLevel(Level.ERROR);
		Logger.getLogger(MemcachedLease.class).setLevel(Level.ERROR);
		Logger.getLogger(MongoDbClient.class).setLevel(Level.ERROR);

		Logger.getLogger(CADSMongoDbClient.class).setLevel(Level.ERROR);
		Logger.getLogger(TardisYCSBWorker.class).setLevel(Level.ERROR);
		Logger.getLogger(CADSWbMongoDbClient.class).setLevel(Level.ERROR);
		Logger.getLogger(CADSRedleaseMongoDbClient.class).setLevel(Level.ERROR);
		Logger.getLogger(RecoveryEngine.class).setLevel(Level.ERROR); 
	}

	@Override
	public abstract Status delete(String arg0, String arg1);

	@Override
	public Status insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		// put them in cache if full warm-up
		if (TardisYCSBConfig.fullWarmUp) {
			byte[] payload = CacheUtilities.SerializeHashMap(values);
			try {
				logger.info("Insert key "+key+" to server index "+getHashCode(key));
				mc.set(key, payload, getHashCode(key));
			} catch (IOException e) {
				logger.fatal("Insert: set failed.");        
				e.printStackTrace();
			}
		}
		return client.insert(table, key, values);
	}

	@Override
	public abstract Status read(String arg0, String arg1, Set<String> arg2,
			HashMap<String, ByteIterator> arg3);

	@Override
	public abstract Status scan(String arg0, String arg1, int arg2, Set<String> arg3,
			Vector<HashMap<String, ByteIterator>> arg4);

	@Override
	public abstract Status update(String arg0, String arg1,
			HashMap<String, ByteIterator> arg2);

	@Override
	public void init() throws DBException {
		threadCount.incrementAndGet();   

		if (!isInit.get()) {
			try {
				initLock.acquire();

				if (!isInit.get()) {     
					if (getProperties().getProperty(TardisYCSBConfig.KEY_FULL_WARM_UP) != null) {
						TardisYCSBConfig.fullWarmUp = Boolean.parseBoolean(getProperties().getProperty(TardisYCSBConfig.KEY_FULL_WARM_UP));
					}
					System.out.println("Full Warmup = " + TardisYCSBConfig.fullWarmUp);

					if (getProperties().getProperty(TardisYCSBConfig.KEY_AR_CHECK_SLEEP_TIME) != null) {
						TardisYCSBConfig.RECOVERY_WORKER_BASE_TIME_BETWEEN_CHECKING_EW = 
								Integer.parseInt(getProperties().getProperty(
										TardisYCSBConfig.KEY_AR_CHECK_SLEEP_TIME));
					}
					System.out.println("AR Check Sleep Time = "+ TardisYCSBConfig.RECOVERY_WORKER_BASE_TIME_BETWEEN_CHECKING_EW);

					if (getProperties().getProperty(TardisYCSBConfig.KEY_CACHE_MODE) != null) {
						String mode = getProperties().getProperty(TardisYCSBConfig.KEY_CACHE_MODE);
						switch (mode) {
						case "nocache":
							cacheMode = CACHE_NO_CACHE;
							break;
						case "around":
							cacheMode = CACHE_WRITE_AROUND;
							break;
						case "through":
							cacheMode = CACHE_WRITE_THROUGH;
							break;
						case "back":
							cacheMode = CACHE_WRITE_BACK;
							break;
						}
					}

					System.out.println("Memcached Mode = " + cacheMode);

					if (cacheMode == CACHE_WRITE_THROUGH) {
						if (getProperties().getProperty(TardisYCSBConfig.TARDIS_MODE) != null) {
							String tardisMode = getProperties().getProperty(TardisYCSBConfig.TARDIS_MODE);
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
					
					if (getProperties().getProperty(TardisYCSBConfig.WRITE_SET) != null) {
						WRITE_SET = Boolean.parseBoolean(getProperties().getProperty(TardisYCSBConfig.WRITE_SET));
					}
					
					System.out.println("Read Recover = "+READ_RECOVER);
					System.out.println("Read recover ALWAYS = "+READ_RECOVER_ALWAYS);
					System.out.println("Write Recover = "+WRITE_RECOVER);
					System.out.println("Write Set = "+WRITE_SET);

					if (getProperties().getProperty(TardisYCSBConfig.KEY_CACHE_MEMORY) != null) {
						TardisYCSBConfig.cacheMemoryMB = Integer.parseInt(getProperties().getProperty(TardisYCSBConfig.KEY_CACHE_MEMORY));
					}
					System.out.println("Cache Memory (MB): "+TardisYCSBConfig.cacheMemoryMB);

					if (getProperties().getProperty(TardisYCSBConfig.KEY_MONITOR_SPACE_WRITES) != null) {
						TardisYCSBConfig.monitorSpaceWritesRatio = Boolean.parseBoolean(getProperties().getProperty(TardisYCSBConfig.KEY_MONITOR_SPACE_WRITES));
					}
					System.out.println("Monitor space writes: "+TardisYCSBConfig.monitorSpaceWritesRatio);

					if (getProperties().getProperty(TardisYCSBConfig.KEY_CACHE_SERVERS) != null) {
						String[] servers = 
								getProperties().
								getProperty(TardisYCSBConfig.KEY_CACHE_SERVERS).split(",");

						int numCaches = servers.length;
						if (getProperties().getProperty(TardisYCSBConfig.KEY_NUM_CACHE_SERVERS) != null) {
							numCaches = Integer.parseInt(getProperties().getProperty(TardisYCSBConfig.KEY_NUM_CACHE_SERVERS));
							if (numCaches > servers.length)
								numCaches = servers.length;
						}
						logger.info("Num caches: "+numCaches);

						cacheServers = new String[numCaches];
						for (int i = 0; i < numCaches; i++) {
							if (!servers[i].contains("11211"))
								cacheServers[i] = servers[i] + ":11211";
							else
								cacheServers[i] = servers[i];
						}
					}       

					if (getProperties().getProperty(TardisYCSBConfig.KEY_PHASE) != null) {
						TardisYCSBConfig.phase = getProperties().getProperty(TardisYCSBConfig.KEY_PHASE);
					}
					System.out.println("Phase: "+TardisYCSBConfig.phase);


					if (cacheServers != null) {
						logger.info("Cache servers: "+Arrays.toString(cacheServers));
						Integer[] weights = {1, 1, 1, 1};

						// initialize the pool for memcache servers
						SockIOPool pool = SockIOPool.getInstance(TardisYCSBConfig.BENCHMARK);
						pool.setServers(cacheServers);
						pool.setWeights(weights);
						pool.setInitConn(200);
						pool.setMinConn(200);
						pool.setMaxConn(1000);
						pool.setMaintSleep(0);
						pool.setNagle(false);
						pool.initialize();
					}

					if (client == null) {
						client = new MongoDbClientDelegate();
						client.setProperties(getProperties());
						client.init();
					}

					if (TardisYCSBConfig.phase.equals("load")) {
						// clean up the database
						client.mongoClient.dropDatabase("ycsb");
						logger.info("Database dropped.");
					}   

					if (getProperties().getProperty("numarworker") != null) {
						NUM_ACTIVE_RECOVERY_WORKERS = Integer.parseInt(getProperties().getProperty("numarworker"));
					}

					int alpha = 5;
					if (getProperties().getProperty("alpha") != null) {
						alpha = Integer.parseInt(getProperties().getProperty("alpha"));
						System.out.println("Alpha = "+alpha);
					}          

					if (NUM_ACTIVE_RECOVERY_WORKERS > 0 && arWorkers == null) {
						System.out.println("Start "+NUM_ACTIVE_RECOVERY_WORKERS+" AR workers...");
						arWorkers = new TardisYCSBWorker[NUM_ACTIVE_RECOVERY_WORKERS];
						TardisYCSBWorker.isRunning = true;
						for (int i = 0; i < arWorkers.length; i++) {
							arWorkers[i] = new TardisYCSBWorker(client, alpha, cacheServers);
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

						//            for (int i = 0; i < longIntervals.length; i += 2) {
						//              ArrayMetrics.getInstance().add(MetricsName.METRICS_DATABASE_DOWN_TIME,
						//                  new DatabaseFailureRecord(longIntervals[i], longIntervals[i + 1]));
						//            }
					}

					if (executor == null || executor.isShutdown()) {
						executor = Executors.newFixedThreadPool(5);
					}

					if (getProperties().getProperty("ewstats") != null) {
						boolean ewstats = Boolean.parseBoolean(getProperties().getProperty("ewstats"));
						if (ewstats == true) {
							int numRecs = Integer.parseInt(getProperties().getProperty("recordcount"));
							ewStatsWatcher = new EWStatsWatcher(numRecs);
							executor.submit(ewStatsWatcher);
						}
					}

					if (longIntervals != null) {
						System.out.println("Start the database state simulator..."+longIntervals);        
						dbStateSimulator = new DBSimulator(MongoDbClientDelegate.isDatabaseFailed, longIntervals);
						executor.submit(dbStateSimulator);
					}

					isInit.set(true);
					System.out.println("isDatabaseFailed = "+MongoDbClientDelegate.isDatabaseFailed.get());
				}

				initLock.release();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (mc == null) {
			mc = new MemcachedClient(TardisYCSBConfig.BENCHMARK);
		}
	}

	@Override
	public void cleanup() throws DBException {
		System.out.println("Clean up");
		super.cleanup();

		if (mc != null) {
			mc = null;
		}

		if (threadCount.decrementAndGet() == 0) {   
			printStats();

			if ( dbStateSimulator != null ) {
				dbStateSimulator.shutdown();
				dbStateSimulator = null;
			}

			if (ewStatsWatcher != null) {
				ewStatsWatcher.shutdown();
				ewStatsWatcher = null;
			}

			if (executor != null) {
				executor.shutdown();
				executor = null;
			}

			if (arWorkers != null) {
				TardisYCSBWorker.isRunning = false;
				System.out.println("Set isRunning = "+TardisYCSBWorker.isRunning);
				for (Thread ar : arWorkers) {
					try {
						ar.join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				arWorkers = null;
			} 

			SockIOPool.getInstance(TardisYCSBConfig.BENCHMARK).shutDown();

			if (client != null) {
				client.cleanup();
				client = null;
			}

			isInit.set(false);
		}
	}

	public static void printStats() {
		System.out.println("Num Cache Hit: "+cacheHits.get());
		System.out.println("Num Cache Miss: "+cacheMisses.get());
		System.out.println("Num Docs Recovered: "+numDocsRecovered.get());
		System.out.println("Num Docs Recovered in ARs: "+numDocsRecoveredInARs.get());
		System.out.println("Num Docs Recovered in Reads: "+numDocsRecoveredInReads.get());
		System.out.println("Num Docs Recovered in Writes: "+numDocsRecoveredInUpdates.get());
		System.out.println("Total lease retries: "+MemcachedLease.getBackoff().get());
		System.out.println("Average lease retries: "+ MemcachedLease.getBackoff().get() / (double) MemcachedLease.numLeasesGranted.get());
		System.out.println("Num find no buffs: "+findNoBuff.get());
	}
}