package mongodb;

public class ReconConfig {
	public static final String KEY_ACTION = "reconaction";
	public static final String CREATE_SCHEMA = "schema";
	public static final String LOAD_DATA = "load";
	public static final String RUN = "run";
	
	public static final String KEY_NUM_AR_WORKER = "numarworker";
	public static final String KEY_WARM_UP_END = "reconwarmupend";
	public static final String KEY_WORKLOAD = "reconworkload";
	public static final String KEY_DB_FAILED = "dbfail";
	public static final String KEY_METRICS_FILE_NAME = "reconmetrics";
	public static final String KEY_CACHE_SIZE = "reconcachesize";
	public static final String KEY_TWEMCACHED_IP = "twemcachedip";
	public static final String KEY_FULL_WARM_UP = "reconfullwarmup";
	public static final String KEY_BASE_ALPHA = "reconbasealpha";
	public static final String KEY_ALPHA_POLICY = "reconalphapolicy";
	public static final String KEY_NUM_USER_IN_CACHE = "reconnumusercache";
	public static final String KEY_MONITOR_LOST_WRITE = "reconmlostwrite";
	
	public static final String KEY_NUM_LOST_WRIETS_IN_FAIL = "reconmlostwriteinfail";
	public static final String KEY_NUM_LOST_WRIETS_IN_RECOVERY = "reconmlostwriteinrecovery";
	public static final String KEY_NUM_LOST_WRIETS_AFTER_RECOVERY = "reconmlostwriteafterrecovery";
	
	public static final String DELIMITER = ",";
}
