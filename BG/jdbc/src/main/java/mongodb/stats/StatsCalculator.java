package mongodb.stats;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import mongodb.LockManager;

public class StatsCalculator {

	private final Logger logger = Logger.getLogger(StatsCalculator.class);

	public static final class Stats {

		private final long[] buckets;
		private final long interval;

		public Stats(int numBuckets, long interval) {
			super();
			this.interval = interval;
			this.buckets = new long[numBuckets];
		}

		private long max;
		private long min;
		private long count;

		public void add(long time) {

			if (time <= 0) {
				return;
			}

			min = Math.min(min, time);
			max = Math.max(max, time);

			int index = (int) (time / this.interval);
			if (index > this.buckets.length) {
				index = this.buckets.length - 1;
			}
			this.buckets[index]++;
			count++;
		}

		public long getMax() {
			return max;
		}

		public long getMin() {
			return min;
		}

		public double getMean() {
			double mean = 0;
			double in = this.interval / 2;
			for (int i = 0; i < this.buckets.length; i++) {
				mean += in * this.buckets[i] / (double) this.count;
			}
			return mean;
		}

		public long getCount() {
			return count;
		}

		@Override
		public String toString() {
			return "Stats [max=" + max + ", min=" + min + ", count=" + count + ", mean=" + getMean() + "]";
		}

	}

	private final LockManager locks = new LockManager(1024);
	private final Map<String, Stats> stats = new HashMap<>();

	private static final StatsCalculator c = new StatsCalculator();

	public static final StatsCalculator getInstance() {
		return c;
	}

	public void add(String key, long time) {
		locks.acquire(key);
		Stats s = stats.get(key);
		if (s == null) {
			// 0.1 ms 0.1*1000 = 100 ms
			s = new Stats(1024, 100);
			stats.put(key, s);
		}
		s.add(time);
		locks.release(key);
	}

	public void clear() {
		Set<String> keys = new HashSet<>(this.stats.keySet());
		keys.forEach(k -> {
			locks.acquire(k);
			Stats stat = this.stats.remove(k);
			locks.release(k);
			logger.info(stat);
		});
	}
}
