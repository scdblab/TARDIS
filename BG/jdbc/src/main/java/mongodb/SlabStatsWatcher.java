package mongodb;

import static tardis.TardisClientConfig.STATS_SLAB_WORKER_TIME_BETWEEN_CHECKING_EW;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

import tardis.TardisClientConfig;

public class SlabStatsWatcher implements Callable<Void> {
	private final MemcachedClient memcachedClient;

	private boolean isRunning = true;

	private final Logger logger = Logger.getLogger(EWStatsWatcher.class);

	public SlabStatsWatcher() {
		super();
		this.memcachedClient = new MemcachedClient("BG");
	}

	public SlabStatsWatcher(MemcachedClient client) {
		this.memcachedClient = client;
	}

	@Override
	public Void call() throws Exception {

		System.out.println("Start Slab Stats Watcher...");

		if (!TardisClientConfig.enableMetrics) {
			return null;
		}
		
		while (isRunning) {
			Map<String, String> stats = (Map<String, String>) memcachedClient.statsSlabs().values().iterator().next();
			Map<String, Map<String, Long>> slabStats = processStats(stats);
			
			SlabStatsRecord r = new SlabStatsRecord(slabStats);
			ArrayMetrics.getInstance().add(MetricsName.METRICS_SLAB_STATS, r);
			try {
				Thread.sleep(STATS_SLAB_WORKER_TIME_BETWEEN_CHECKING_EW);
			} catch (Exception e) {
				logger.error("sleep got interrupted", e);
			}
		}
		return null;
	}
	
	private static Map<String, Map<String, Long>> processStats(Map<String, String> stats) {
		Map<String, Map<String, Long>> slabStats = new HashMap<>();
		stats.forEach((k, v) -> {
			String[] slabKeys = k.split(":");
			Map<String, Long> stat = slabStats.get(slabKeys[0]);
			if (stat == null) {
				stat = new HashMap<>();
				slabStats.put(slabKeys[0], stat);
			}
			stat.put(slabKeys[1], Long.parseLong(v));
		});
		return slabStats;
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
		Map<String, String> stats = (Map<String, String>) mc.statsSlabs().values().iterator().next();
		System.out.println(stats);
		
		System.out.println(processStats(stats));
		JSONObject obj = new JSONObject();
		obj.put("slabstats", processStats(stats));
		System.out.println(obj.toString());
	}
}
