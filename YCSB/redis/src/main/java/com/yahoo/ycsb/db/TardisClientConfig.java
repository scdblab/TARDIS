package com.yahoo.ycsb.db;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author haoyuh
 *
 */
public class TardisClientConfig {

	public static final long RECOVERY_WORKER_BASE_TIME_BETWEEN_CHECKING_EW = 50;
	public static final long RECOVERY_WORKER_RANGE_TIME_BETWEEN_CHECKING_EW = 50;
	
	public static final boolean SKIP_UPDATE_MONGO = true;

	public static long RECOVERY_WORKER_SLEEP_TIME = 1000;

	public static final long STATS_EW_WORKER_TIME_BETWEEN_CHECKING_EW = 1000;
	public static final long STATS_SLAB_WORKER_TIME_BETWEEN_CHECKING_EW = 10000;

	public static int NUM_EVENTUAL_WRITE_LOGS = 211;
	
	public static boolean writeBack = false;
	public static boolean updateApplyBufferedWrites = false;
	public static boolean readApplyBufferedWrites = false;
	public static boolean ARApplyBufferedWrites = false;
	public static boolean readAlwaysApplyBufferedWrites = false;
	public static boolean writeSetValue = false;
	

	// configs related to pending writes log.
	/**
	 * the key used to make sure only one recovery worker is working on a EW log
	 */
	public static final String KEY_EVENTUAL_WRITE_LOG = "EW";
	/**
	 * All mutations to the EW log must acquire lease on this key first.
	 */
	public static final String LEASE_KEY_EVENTUAL_WRITES_LOG = "EWP";
	public static final String KEY_BUFFERED_WRITES_LOG = "U";
	/**
	 * All mutations to the user pending writes key-value pair must acquire
	 * lease on this key first.
	 */
	public static final String LEASE_KEY_BUFFERED_WRITES = "UM";

	public static final String NORMAL_KEY_PREFIX = "N";

	public static final String USER_DIRTY = "d";

	public static final int DATABASE_FAILURE = 1000;
	
	public static String metricFile = "";
	public static String leaseKey(String key) {
		return LEASE_KEY_BUFFERED_WRITES + key;
	}
	
	public static String bufferedWriteKey(String key) {
		return KEY_BUFFERED_WRITES_LOG + key;
	}
	
	public static String normalKey(String key) {
		return key;
	}
	
	public static String ewLeaseKey(int id) {
		return LEASE_KEY_EVENTUAL_WRITES_LOG + id % NUM_EVENTUAL_WRITE_LOGS;
	}
	
	public static String ewLeaseKey(String key) {
		return LEASE_KEY_EVENTUAL_WRITES_LOG + (Long.parseLong(key) % NUM_EVENTUAL_WRITE_LOGS);
	}
	
	public static String keyFromBufferedWriteKey(String bufferedWriteKey) {
		return bufferedWriteKey.substring(KEY_BUFFERED_WRITES_LOG.length());
	}
	
	public static String ewKey(int id) {
		return KEY_EVENTUAL_WRITE_LOG + id % NUM_EVENTUAL_WRITE_LOGS;
	}
	
	public static String ewKey(String key) {
		return KEY_EVENTUAL_WRITE_LOG + (Long.parseLong(key) % NUM_EVENTUAL_WRITE_LOGS);
	}
}
