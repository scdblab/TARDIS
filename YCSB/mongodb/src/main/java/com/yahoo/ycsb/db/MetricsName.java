package com.yahoo.ycsb.db;

public class MetricsName {
	public static final String METRICS_PENDING_WRITES = "PendingWrites";
	
	// Recovered pending writes (over time) by both ARs and BG threads.
	public static final String METRICS_RECOVERED_WRITES = "RecoveredWrites";
	
	// Recovered pending writes (total) by both ARs and BG threads.
	public static final String METRICS_RECOVERED_WRITES_COUNT = "RecoveredWritesCount";
	
	public static final String METRICS_RECOVERY_DURATION = "RecoveredDuration";
	
	public static final String METRICS_TOTAL_PENDING_WRITES = "TotalPendingWrites";
	public static final String METRICS_TOTAL_PENDING_WRITES_FAIL = "TotalPendingWritesSeenAfterFail";
	public static final String METRICS_TOTAL_PENDING_WRITES_RECOVERY = "TotalPendingWritesSeenAfterRecovery";
	
	// Number of docs recovered by ARs.
	public static final String METRICS_NUMBER_RECOVERED_BY_AR_WORKER = "NumberRecoveredAR";
	
	// Number of docs recovered by App.
	public static final String METRICS_NUMBER_RECOVERED_BY_APP = "NumberRecoveredApp";
	
//	public static final String METRICS_NUMBER_DIRTY_USER = "NumberDirtyUser";
//	public static final String METRICS_RECOVERY_WORKER_STATS = "RecoveryWorkerStats";
	public static final String METRICS_EW_STATS = "EWStats";
	public static final String METRICS_DATABASE_DOWN_TIME = "DatabaseCrashed";
	public static final String METRICS_READ_FAILED = "ReadFailed";
//	public static final String METRICS_READ_CACHE_HIT = "ReadCacheHit";
//	public static final String METRICS_READ_CACHE_MISS = "ReadCacheMiss";
	public static final String METRICS_DBFAIL_MEM_OVERHEAD = "MemOverhead";
	public static final String METRICS_SLAB_STATS = "SlabStats";
	public static final String METRICS_DIRTY_USER = "DirtyUsers";
	
	// Number of recovered users (documents)  by both ARs and BG threads.
	public static final String METRICS_RECOVERED_USER = "RecoveredUsers";
	public static final String METRICS_MISSED_USER = "MissedUsers";

	public static final String METRICS_BUFFERED_WRITES = "BufferedWrites";
}