package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.json.JSONObject;

public class TimedMetrics {
	private final Map<String, TreeMap<Long, Integer>> metrics;
	private final LockManager locks;

	private static final TimedMetrics timedMetrics = new TimedMetrics();

	public static final TimedMetrics getInstance() {
		return timedMetrics;
	}

	private TimedMetrics() {
		this.locks = new LockManager(2027);
		this.metrics = new HashMap<>();
	}

	public void add(String name) {
		this.add(name, 1);
	}

	public void add(String name, int amount) {
		if (!TardisYCSBConfig.enableMetrics) {
			return;
		}
		
		if (amount == 0) {
			return;
		}
		
		long now = System.currentTimeMillis();
		locks.acquire(name);
		TreeMap<Long, Integer> timedCount = metrics.get(name);
		if (timedCount == null) {
			timedCount = new TreeMap<>();
			timedCount.put(now, amount);
			this.metrics.put(name, timedCount);
		} else {
			Long lastKey = timedCount.lastKey();
			if (now - lastKey <= 1000) {
				timedCount.put(lastKey, timedCount.get(lastKey) + amount);
			} else {
				timedCount.put(now, amount);
			}
		}
		locks.release(name);
	}

	public int aggregate(String name) {
		TreeMap<Long, Integer> map = metrics.get(name);
		if (map == null) {
			return 0;
		}
		return map.values().stream().mapToInt(Integer::intValue).sum();
	}

	public JSONObject getMetricsAsJson(String name) {
		try {
			JSONObject json = new JSONObject();
			json.put("name", name);
			json.put("time", ArrayMetricsRecord.toJSON(metrics.get(name)));
			return json;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public Map<String, TreeMap<Long, Integer>> getMetrics() {
		return metrics;
	}

	public String toString() {
		JSONObject obj = new JSONObject();
		metrics.keySet().forEach(key -> {
			try {
				obj.put(key, getMetricsAsJson(key));
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		return obj.toString();
	}
}
