package mongodb;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import tardis.TardisClientConfig;

public class CountMetrics {
	private final Map<String, AtomicInteger> metrics;
	private final LockManager locks;


	public CountMetrics() {
		this.locks = new LockManager(2027);
		this.metrics = new HashMap<>();
	}

	public synchronized void add(String name) {
		this.add(name, 1);
	}

	public synchronized void add(String name, int amount) {
		if (!TardisClientConfig.enableMetrics) {
			return;
		}
		
		if (amount == 0) {
			return;
		}
		
		locks.acquire(name);
		AtomicInteger value = metrics.get(name);
		if (value == null) {
			value = new AtomicInteger(amount);
			metrics.put(name, value);
		} else {
			value.addAndGet(amount);
		}
		locks.release(name);
	}

	public Map<String, AtomicInteger> getMetrics() {
		return metrics;
	}
	
//	public JSONObject getMetricsAsJson(String name) {
//		try {
//			JSONObject json = new JSONObject();
//			json.put("name", name);
//			json.put("time", ArrayMetricsRecord.toJSON(metrics.get(name)));
//			return json;
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return null;
//	}
//
//	public Map<String, TreeMap<Long, Integer>> getMetrics() {
//		return metrics;
//	}
//
//	public String toString() {
//		JSONObject obj = new JSONObject();
//		metrics.keySet().forEach(key -> {
//			try {
//				obj.put(key, getMetricsAsJson(key));
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		});
//		return obj.toString();
//	}
}
