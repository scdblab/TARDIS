package com.yahoo.ycsb.db;

public class TardisYCSBConfig {    
  public static final int ACT_READ = 0;
  public static final int ACT_INSERT = 1;
  public static final int ACT_UPDATE = 2;
  public static final int ACT_DELETE = 3;
  public static final int ACT_SCAN = 4;
  public static final int ACT_AR = 5;
  
  public static final String USER_DIRTY = "d";
  
  public static final boolean SKIP_UPDATE_MONGO = false;
  
  public static final String KEY_EVENTUAL_WRITE_LOG = "EW";
  public static long RECOVERY_WORKER_BASE_TIME_BETWEEN_CHECKING_EW = 50;
  public static final long STATS_EW_WORKER_TIME_BETWEEN_CHECKING_EW = 1000;
  
  public static final int QLEASE_BACKOFF = 50;
  public static int NUM_EVENTUAL_WRITE_LOGS = 211;
  
  protected static boolean fullWarmUp = true;
  
  public static final String KEY_FULL_WARM_UP = "fullwarmup";
  public static final String KEY_CACHE_MODE = "cachemode";
  public static final String KEY_CACHE_SERVERS = "cacheservers";
  
  protected static final int CACHE_NO_CACHE = 0;
  protected static final int CACHE_WRITE_AROUND = 1;
  protected static final int CACHE_WRITE_THROUGH = 2;
  protected static final int CACHE_WRITE_BACK = 3;
  
  public static final String DELIMITER = ",";
  public static final String KEY_USER_PENDING_WRITES_LOG = "U";
  public static final int LEASE_TIMEOUT = 10000;
  
  public static final String BENCHMARK = "YCSB";
  
  public static final String LEASE_KEY_USER_PENDING_WRITES_LOG_MUTATION = "UM";
  private static final String LEASE_KEY_EVENTUAL_WRITES_LOG_MUTATION = "EWP";
  public static boolean monitorSpaceWritesRatio = false;
  public static final String KEY_CACHE_MEMORY = "reconcachememory";
  public static final String KEY_MONITOR_SPACE_WRITES = "monitorspacewrite";
  public static final String KEY_NUM_CACHE_SERVERS = "numcacheservers";
  public static final String KEY_PHASE = "phase";
  public static final String KEY_AR_CHECK_SLEEP_TIME = "arsleeptime";
  public static final String TARDIS_MODE = "tardismode";
public static final String WRITE_SET = "writeset";
  
  public static String[] cacheServers = null;
  
  public static int cacheMemoryMB;
  public static boolean monitorLostWrite = false;
  public static boolean enableMetrics = false;
  public static String phase = "run";
  
  public static long extractUserId(String key) {
    return Long.parseLong(key.replaceAll("[^0-9]", ""));
  }
  
  protected static void sleepLeaseRetry() {
    try {
      Thread.sleep(TardisYCSBConfig.QLEASE_BACKOFF);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public static String getUserLogKey(long userId) {
    return KEY_USER_PENDING_WRITES_LOG + userId;
  }
  
  protected static Integer getHashCode(String key) {
    long num = Long.parseLong(key.replaceAll("[^0-9]", ""));
    return getHashCode(num);
  }
  
  protected static int getUserId(String key) {
	  return Integer.parseInt(key.replaceAll("[^0-9]", ""));
  }
  
  protected static Integer getHashCode(long id) {
    return new Integer((int) (id % cacheServers.length));
  }
  
//  public static String getUserLogLeaseKey(long userId) {
//    return LEASE_KEY_USER_PENDING_WRITES_LOG_MUTATION + userId;
//  }
  
  public static String getUserLogLeaseKey(long userId) {
    return LEASE_KEY_USER_PENDING_WRITES_LOG_MUTATION + userId;
  }
  
  public static String getEWLogKey(int ewId) {
    return KEY_EVENTUAL_WRITE_LOG + ewId;
  }

  public static String getPWLogKey(long id) {
    return KEY_USER_PENDING_WRITES_LOG + id;
  } 
  
  public static int getEWId(long userId) {
    return (int)(userId % NUM_EVENTUAL_WRITE_LOGS);
  }
  
  public static String getEWLogMutationLeaseKey(String ewLogKey) {
    return LEASE_KEY_EVENTUAL_WRITES_LOG_MUTATION + ewLogKey.substring(KEY_EVENTUAL_WRITE_LOG.length());
  }

  public static String getEWLogMutationLeaseKeyFromUserId(long userId) {
    return LEASE_KEY_EVENTUAL_WRITES_LOG_MUTATION + getEWId(userId);
  }
  
  public static String getEWLogKeyFromUserId(long userId) {
    return getEWLogKey(getEWId(userId));
  }
}
