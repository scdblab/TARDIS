package mongodb;

import static tardis.TardisClientConfig.NUM_EVENTUAL_WRITE_LOGS;
import static tardis.TardisClientConfig.STATS_EW_WORKER_TIME_BETWEEN_CHECKING_EW;
import static tardis.TardisClientConfig.getEWLogKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

public class EWStatsWatcher implements Callable<Void> {
	private final MemcachedClient memcachedClient;

	private boolean isRunning = true;

	private final Logger logger = Logger.getLogger(EWStatsWatcher.class);
	private int numUsers = 0;

	public EWStatsWatcher(int numUsers) {
		super();
		this.memcachedClient = new MemcachedClient("BG");
		this.numUsers = numUsers;
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

		long begin = System.currentTimeMillis();
		long back = 0;
		int loop = 0;
		while (isRunning) {

			try {
				Map<String, Object> ewList = memcachedClient.getMulti(EWs);

				long start = System.currentTimeMillis();
				Map<String, Integer> recoverRecord = new HashMap<>();
				Map<String, Integer> recoveredRecord = new HashMap<>();

				int cnt = 0;
				for (String ew: ewList.keySet()) {
					String val = (String) ewList.get(ew);
					Set<String> dirtyUserIds = MemcachedSetHelper.convertSet(val);
					cnt += dirtyUserIds.size();
					recoverRecord.put(ew, dirtyUserIds.size());
				}
				
				MemMongoClient.dirtyDocs.set(cnt);
				
				if (++loop == 10) {
					System.out.println("Remaining dirty docs: "+cnt);
					System.out.println("ARs recovered by reads: "+MemMongoClient.numUpdatesOnReads.get());
					System.out.println("ARs recovered by updates: "+MemMongoClient.numUpdatesOnWrites.get());
					System.out.println("ARs recovered by ARs: "+MemMongoClient.numUpdatesOnARs.get());
					loop = 0;
				}
				
				long end = System.currentTimeMillis();
				RecoverRecord r = new RecoverRecord(start, end, 0, recoverRecord, recoveredRecord);

				if (recoverRecord.size() > 0) {
					ArrayMetrics.getInstance().add(MetricsName.METRICS_EW_STATS, r);
				}

				if (cnt >= numUsers * 0.95 && !DatabaseStateSimulator.modechanged) {
					back = System.nanoTime();
					System.out.println("Back at " + back);
					MemMongoClient.isDatabaseFailed.set(false);
					DatabaseStateSimulator.modechanged = true;
				}
				
				long ellapsedTime = System.currentTimeMillis() - begin;
				if (ellapsedTime > 30000 && cnt == 0 && 
						MemMongoClient.writeBack == false) {
					System.out.println("Complete recovery at "+System.nanoTime());
					Thread.sleep(30000);				
					MemMongoClient.printStats();
					System.exit(0);
				}
				
				if (MemMongoClient.WRITE_RECOVER == false && back != 0) {
					ellapsedTime = (System.nanoTime() - back) / 1000 / 1000 / 1000;
					if (ellapsedTime > 500) {
						MemMongoClient.printStats();
						System.exit(0);
					}
				}

				try {
					Thread.sleep(STATS_EW_WORKER_TIME_BETWEEN_CHECKING_EW);
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
		EWStatsWatcher w = new EWStatsWatcher(100);
		Executors.newFixedThreadPool(1).submit(w);
	}
}
