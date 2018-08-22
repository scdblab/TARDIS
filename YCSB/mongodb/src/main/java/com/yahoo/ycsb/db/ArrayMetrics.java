package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

public class ArrayMetrics {
	private final Map<String, List<ArrayMetricsRecord>> metricArray;
	private final LockManager locks;

	private static final ArrayMetrics arrayMetrics = new ArrayMetrics();

	public static final ArrayMetrics getInstance() {
		return arrayMetrics;
	}

	private ArrayMetrics() {
		this.metricArray = new HashMap<>();
		this.locks = new LockManager(2027);
	}

	public void add(String name, ArrayMetricsRecord value) {
		if (!TardisYCSBConfig.enableMetrics) {
			return;
		}
		locks.acquire(name);
		List<ArrayMetricsRecord> metrics = metricArray.get(name);
		if (metrics == null) {
			metrics = new ArrayList<>();
			metricArray.put(name, metrics);
		}
		metrics.add(value);
		locks.release(name);
	}

	public JSONArray toJSON(String name) {
		JSONArray array = new JSONArray();
		metricArray.get(name).forEach(i -> {
			array.put(i.toJSON());
		});
		return array;
	}

	public Map<String, List<ArrayMetricsRecord>> getMetricArray() {
		return metricArray;
	}

	public String toString() {
		JSONObject obj = new JSONObject();
		metricArray.forEach((k, v) -> {
			try {
				obj.put(k, toJSON(k));
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		return obj.toString();
	}

}
