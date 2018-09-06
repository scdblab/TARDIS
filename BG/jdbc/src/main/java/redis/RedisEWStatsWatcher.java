package redis;

import static tardis.TardisClientConfig.NUM_EVENTUAL_WRITE_LOGS;
import static tardis.TardisClientConfig.STATS_EW_WORKER_TIME_BETWEEN_CHECKING_EW;
import static tardis.TardisClientConfig.getEWLogKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import mongodb.*;
import tardis.TardisClientConfig;

public class RedisEWStatsWatcher implements Callable<Void> {
	private final HashShardedJedis redisClient;

	private boolean isRunning = true;

	private final Logger logger = Logger.getLogger(RedisEWStatsWatcher.class);

	public RedisEWStatsWatcher(HashShardedJedis client) {
		this.redisClient = client;
	}

	@Override
	public Void call() throws Exception {

		System.out.println("Start EWStatsWatcher...");

		List<Integer> keys = new ArrayList<>();

		for (int i = 0; i < NUM_EVENTUAL_WRITE_LOGS; i++) {
			keys.add(i);
		}

		Map<String, Integer> recoveredRecord = new HashMap<>();

		while (isRunning) {

			try {
				
				Map<String, Long> values = redisClient.mscard(keys, TardisClientConfig.KEY_EVENTUAL_WRITE_LOG);

				long start = System.currentTimeMillis();
				Map<String, Integer> recoverRecord = new HashMap<>();
				
				for (int i = 0; i < NUM_EVENTUAL_WRITE_LOGS; i++) {
					String log = getEWLogKey(keys.get(i));
					recoverRecord.put(log, values.get(log).intValue());
				}
				
				long end = System.currentTimeMillis();
				RecoverRecord r = new RecoverRecord(start, end, 0, recoverRecord, recoveredRecord);

				if (recoverRecord.size() > 0) {
					ArrayMetrics.getInstance().add(MetricsName.METRICS_EW_STATS, r);
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

}
