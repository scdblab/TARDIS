package tardis;

/**
 * @author haoyuh
 *
 */
public class TardisClientConfig {

	public static final String KEY_VIEW_PROFILE = "V";
	public static final String KEY_LIST_FRIENDS = "LF";
	public static final String KEY_LIST_FRIENDS_REQUEST = "LP";

	public static final String KEY_FRIEND_COUNT = "f";
	public static final String KEY_PENDING_FRIEND_COUNT = "p";

	public static int PAGE_SIZE_FRIEND_LIST = 10;

	public static final int LEASE_TIMEOUT = 999999; // in ms

	public static final long RECOVERY_WORKER_BASE_TIME_BETWEEN_CHECKING_EW = 50;
	public static final long RECOVERY_WORKER_RANGE_TIME_BETWEEN_CHECKING_EW = 50;
	
	public static final long RECOVERY_WORKER_SLEEP_TIME = 1000;
	
	public static final long STATS_EW_WORKER_TIME_BETWEEN_CHECKING_EW = 1000;
	public static final long STATS_SLAB_WORKER_TIME_BETWEEN_CHECKING_EW = 10000;

	public static int NUM_EVENTUAL_WRITE_LOGS = 211;
	
	public static boolean SNAPSHOT = false;

	// configs related to pending writes log.
	/**
	 * the key used to make sure only one recovery worker is working on a EW log
	 */
	public static final String KEY_EVENTUAL_WRITE_LOG = "EW";
	/**
	 * All mutations to the EW log must acquire lease on this key first.
	 */
	public static final String LEASE_KEY_EVENTUAL_WRITES_LOG_MUTATION = "EWP";
	public static final String KEY_USER_PENDING_WRITES_LOG = "U";
	public static final String KEY_USER_SNAPSHOT = "S";
	/**
	 * All mutations to the user pending writes key-value pair must acquire
	 * lease on this key first.
	 */
	public static final String LEASE_KEY_USER_PENDING_WRITES_LOG_MUTATION = "UM";

	public static final char ACTION_PW_PENDING_FRIENDS = 'P';
	public static final char ACTION_PW_FRIENDS = 'F';
	public static final String DELIMITER = ",";
	public static final String USER_DIRTY = "d";
	
	public static final String KEY_DB_FAILED = "dbfail";
	
	public static final String TARDIS_MODE = "tardismode";
	public static final String WRITE_SET_PROP = "writeset";
	
	/**
	 * Set to false if validation is ran
	 */
	public static boolean supportPaginatedFriends = true;
	public static int numUserInCache = -1;
	public static boolean monitorLostWrite = false;
	public static final boolean enableMetrics = false;
	public static boolean fullWarmUp = true;
	public static boolean measureLostWriteAfterFailure = false;

	public static int getEWId(int userId) {
		return userId % NUM_EVENTUAL_WRITE_LOGS;
	}

	public static String getEWLogKeyFromUserId(int userId) {
		return getEWLogKey(getEWId(userId));
	}

	public static String getEWLogKey(int ewId) {
		return KEY_EVENTUAL_WRITE_LOG + ewId;
	}

//	public static String getEWLogMutationLeaseKey(int ewId) {
//		return LEASE_KEY_EVENTUAL_WRITES_LOG_MUTATION + ewId;
//	}

	public static String getEWLogMutationLeaseKey(String ewLogKey) {
		return LEASE_KEY_EVENTUAL_WRITES_LOG_MUTATION + ewLogKey.substring(KEY_EVENTUAL_WRITE_LOG.length());
	}

	public static String getEWLogMutationLeaseKeyFromUserId(int userId) {
		return LEASE_KEY_EVENTUAL_WRITES_LOG_MUTATION + getEWId(userId);
	}

	public static String getUserLogKey(int userId) {
		return KEY_USER_PENDING_WRITES_LOG + userId;
	}
	
	public static String getUserLogKey(String userId) {
		return KEY_USER_PENDING_WRITES_LOG + userId;
	}

	public static int getUserIdFromUserLogKey(String userLogKey) {
		return Integer.parseInt(userLogKey.substring(KEY_USER_PENDING_WRITES_LOG.length()));
	}
	
	public static String getUserIdStringFromUserLogKey(String userLogKey) {
		return userLogKey.substring(KEY_USER_PENDING_WRITES_LOG.length());
	}

	public static String getUserLogLeaseKey(int userId) {
		return LEASE_KEY_USER_PENDING_WRITES_LOG_MUTATION + userId;
	}

	public static String getUserLogLeaseKey(String userId) {
		return LEASE_KEY_USER_PENDING_WRITES_LOG_MUTATION + userId;
	}

  public static String getSnapshotKey(int userId) {
    return KEY_USER_SNAPSHOT + userId;
  }
}
