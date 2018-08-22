package com.yahoo.ycsb.db;

import static com.yahoo.ycsb.db.TardisYCSBConfig.NUM_EVENTUAL_WRITE_LOGS;
import static com.yahoo.ycsb.db.TardisYCSBConfig.STATS_EW_WORKER_TIME_BETWEEN_CHECKING_EW;
import static com.yahoo.ycsb.db.TardisYCSBConfig.getEWLogKey;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

//import ch.qos.logback.core.net.SyslogOutputStream;

public class EWStatsWatcher implements Callable<Void> {
	private final MemcachedClient memcachedClient;

	private boolean isRunning = true;

	private final Logger logger = Logger.getLogger(EWStatsWatcher.class);
	private int numRecs = 0;

	public EWStatsWatcher(int numRecs) {
		super();
		this.memcachedClient = new MemcachedClient(TardisYCSBConfig.BENCHMARK);
		this.numRecs = numRecs;
	}

	public EWStatsWatcher(MemcachedClient client) {
		this.memcachedClient = client;
	}

	@Override
	public Void call() throws Exception {

		System.out.println("Start EWStatsWatcher...");

		String[] EWs = new String[NUM_EVENTUAL_WRITE_LOGS];
		for (int i = 0; i < EWs.length; i++) {
			EWs[i] = getEWLogKey(i);
		}
		
		long start = System.currentTimeMillis();
		long back = 0;
		int loop = 0;
		while (isRunning) {
			try {
				Map<String, Object> ewList = memcachedClient.getMulti(EWs);
				
				int cnt = 0;
				for (String ew: ewList.keySet()) {
				  String val = (String) ewList.get(ew);
					Set<String> dirtyUserIds = MemcachedSetHelper.convertSet(val);
					cnt += dirtyUserIds.size();
				}
				//cnt = CADSWbMongoDbClient.teleW.size();
				CADSWbMongoDbClient.dirtyDocs.set(cnt);

				if (++loop == 10) {
					System.out.println("Remaining dirty docs: "+cnt);
					System.out.println("ARs recovered by reads: "+CADSMongoDbClient.numDocsRecoveredInReads.get());
					System.out.println("ARs recovered by updates: "+CADSMongoDbClient.numDocsRecoveredInUpdates.get());
					System.out.println("ARs recovered by ARs: "+CADSMongoDbClient.numDocsRecoveredInARs.get());
					loop = 0;
				}
				
				if (cnt >= numRecs * 0.95) {
					back = System.nanoTime();
					System.out.println("Back at " + back);
					MongoDbClientDelegate.isDatabaseFailed.set(false);
					DBSimulator.modechanged = true;
				}
				
				long ellapsedTime = System.currentTimeMillis() - start;
				if (ellapsedTime > 30000 && cnt == 0 && 
						CADSWbMongoDbClient.cacheMode == TardisYCSBConfig.CACHE_WRITE_THROUGH) {
					System.out.println("Complete recovery at "+System.nanoTime());
					Thread.sleep(30000);				
					CADSMongoDbClient.printStats();
					System.exit(0);
				}
				
				if (CADSMongoDbClient.WRITE_RECOVER == false && back != 0) {
					ellapsedTime = (System.nanoTime() - back) / 1000 / 1000 / 1000;
					if (ellapsedTime > 500) {
						CADSMongoDbClient.printStats();
						System.exit(0);
					}
				}
				
				try {
					Thread.sleep(100);
				} catch (Exception e) {
					logger.error("sleep got interrupted", e);
				}
			} catch (Exception e) {
				System.out.println("EW failed");
				e.printStackTrace();
			}
		}
		return null;
	}

	public void shutdown() {
		isRunning = false;
		logger.info("shutdown EW stats watcher");
	}

	public static void main(String[] args) {
		String[] serverlist = { "127.0.0.1:11211" };
		SockIOPool pool = SockIOPool.getInstance("BG");
		if (!pool.isInitialized()) {
			pool.setServers(serverlist);

			pool.setInitConn(100);
			pool.setMinConn(1);
			pool.setMaxConn(100);
			pool.setMaintSleep(20);

			pool.setNagle(false);
			pool.initialize();
		}

		// get client instance
		MemcachedClient mc = new MemcachedClient("BG");
		Map<String, Long> stats = (Map<String, Long>) mc.statsSlabs().values().iterator().next();
		System.out.println(stats);
		EWStatsWatcher w = new EWStatsWatcher(100000);
		Executors.newFixedThreadPool(1).submit(w);
	}
}

