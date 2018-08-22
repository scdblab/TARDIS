package com.yahoo.ycsb.db;

import java.util.concurrent.ConcurrentHashMap;

public class TardisMetrics {
	public static final ConcurrentHashMap<String, String> metrics = new ConcurrentHashMap<>();
}
