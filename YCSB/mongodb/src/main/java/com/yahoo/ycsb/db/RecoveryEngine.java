package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.meetup.memcached.MemcachedClient;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.workloads.CoreWorkload;

import static com.yahoo.ycsb.db.TardisYCSBConfig.getHashCode;
import static com.yahoo.ycsb.db.TardisYCSBConfig.getUserLogKey;
import static com.yahoo.ycsb.db.TardisYCSBConfig.USER_DIRTY;
import static com.yahoo.ycsb.db.TardisYCSBConfig.DELIMITER;

public class RecoveryEngine {

  private final MemcachedClient memcachedClient;
  private final MongoDbClient mongoClient;
  private final Logger logger = Logger.getLogger(RecoveryEngine.class);

  private boolean lazyRecovery = true;

  public RecoveryEngine(MemcachedClient client, MongoDbClient mongoClient) {
    super();
    this.memcachedClient = client;
    this.mongoClient = mongoClient;
  }

  public boolean addDirtyFlag(String key, long id) {
    try {
      if (memcachedClient.add(getUserLogKey(id), 
          USER_DIRTY.getBytes(), getHashCode(id))) {
        TimedMetrics.getInstance().add(MetricsName.METRICS_DBFAIL_MEM_OVERHEAD,
            getUserLogKey(id).length() + USER_DIRTY.length() + DELIMITER.length());
        return true;
      }
      return false;
    } catch (Exception e) {
      logger.error("add dirty flag failed for user " + key);
    }
    return false;
  }

  public void insertUserToPartitionLog(String key, long id) throws Exception {
    String ewKey = TardisYCSBConfig.getEWLogKeyFromUserId(id);
    logger.debug("Insert user to partition "+ewKey);

    int length = key.length() + DELIMITER.length();
    try {
      if (!MemcachedSetHelper.addToSetAppend(memcachedClient, ewKey, 
          TardisYCSBConfig.getEWId(id), key, true)) {
        length += ewKey.length();
      }
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Failed to insert user to partition log " + ewKey, e);
      throw e;
    } finally {
      TardisRecoveryEngine.pendingWritesUsers.add(key);
      TardisRecoveryEngine.pendingWritesMetrics.add(MetricsName.METRICS_DBFAIL_MEM_OVERHEAD, length);
    }
  }

  public RecoveryResult recover(RecoveryCaller caller, 
      String key, long userId, byte[] read_buffer) {
    if (lazyRecovery == false && 
        (caller == RecoveryCaller.READ || caller == RecoveryCaller.WRITE)) {
      return RecoveryResult.FAIL;
    }

    boolean recovered = false;
    byte[] pw = null;
    try {
      String logKey = getUserLogKey(userId);

      pw = (byte[]) memcachedClient.get(logKey, getHashCode(userId), false);
      if (pw == null) {
        logger.debug("caller" + caller + " nothing to recover u" + userId + "u");
        return RecoveryResult.CLEAN;
      }

      HashMap<String, ByteIterator> values = new HashMap<>();
      if (isDirtyFlag(pw)) {
        logger.debug("No buffered write found. Get the key from cache.");
        byte[] payload = (byte[]) memcachedClient.get(key, getHashCode(userId), false);
        if (payload != null) {
          CacheUtilities.unMarshallHashMap(values, payload, read_buffer);
        } 
//        else {
//          System.out.println("Got null value "+key);
//        }
      } else {	       
        CacheUtilities.unMarshallHashMap(values, pw, read_buffer);
      }

      // update PStore
      Status status = Status.OK;
      if (values.size() != 0) {
        status = mongoClient.update(CoreWorkload.TABLENAME_PROPERTY_DEFAULT, key, values);
        logger.debug("Update data store success key="+key);
      } 
//      else {
//        logger.fatal("Something went wrong. Expect pending writes here.");
//      }
      
      if (status != Status.OK) {
        return RecoveryResult.FAIL;
      } else {
        recovered = true;
        memcachedClient.delete(key, getHashCode(userId), null);
      }
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Encountered failure during recovery for user " + userId, e);
    } finally {
      if (recovered) {
        switch (caller) {
        case AR:
          TimedMetrics.getInstance().add(MetricsName.METRICS_NUMBER_RECOVERED_BY_AR_WORKER);
          CADSMongoDbClient.numDocsRecoveredInARs.incrementAndGet();
          break;
        case READ:
          TimedMetrics.getInstance().add(MetricsName.METRICS_NUMBER_RECOVERED_BY_APP);
          CADSMongoDbClient.numDocsRecoveredInReads.incrementAndGet();
          break;
        case WRITE:
          TimedMetrics.getInstance().add(MetricsName.METRICS_NUMBER_RECOVERED_BY_APP);
          CADSMongoDbClient.numDocsRecoveredInUpdates.incrementAndGet();
          break;
        default:
          break;
        }
        if (pw != null) {
          TardisRecoveryEngine.recoveredUsers.add(String.valueOf(userId));
        }
      }
    }
    return RecoveryResult.SUCCESS;
  }

  //	public static int measureLostWrites(MemcachedClient memcachedClient) {
  //		
  //		if (!TardisYCSBConfig.enableMetrics) {
  //			return 0;
  //		}
  //		
  //		int count = 0;
  //		try {
  //			if (TardisYCSBConfig.enableMetrics) {
  //				synchronized (TardisYCSBConfig.pendingWritesPerUserMetrics) {
  //
  //					System.out.println("Number of User has pending writes "
  //							+ TardisRecoveryEngine.pendingWritesPerUserMetrics.getMetrics().size());
  //
  //					Map<String, AtomicInteger> lostWrites = TardisRecoveryEngine.pendingWritesPerUserMetrics.getMetrics();
  //
  //					List<String> array = lostWrites.keySet().stream().map(i -> {
  //
  //						String uid = i.substring(MetricsName.METRICS_PENDING_WRITES.length());
  //						return TardisClientConfig.getUserLogKey(uid);
  //					}).collect(Collectors.toList());
  //
  //					int i = 0;
  //					while (i < array.size()) {
  //						int nextPage = Math.min(i + PAGE, array.size());
  //						String[] subList = array.subList(i, nextPage).toArray(new String[0]);
  //						Map<String, Object> pw = memcachedClient.getMulti(subList);
  //
  //						for (String key : subList) {
  //							Object value = pw.get(key);
  //							if (value != null) {
  //								int actual = MemcachedSetHelper.convertList(value).size();
  //								count += actual;
  //							}
  //						}
  //						i = nextPage;
  //					}
  //				}
  //			}
  //		} catch (Exception e) {
  //			e.printStackTrace();
  //			return 0;
  //		}
  //		return count;
  //	}

  public static boolean isDirtyFlag(byte[] pw) {
    return new String(pw).equals(USER_DIRTY);
  }

  public static String translate(char action, int user, String op) {
    return String.format("%s%s%d", action, op, user);
  }

  public void setLazyRecovery(boolean lazyRecovery) {
    this.lazyRecovery = lazyRecovery;
  }

  public boolean bufferWrites(String key, long id,
      HashMap<String, ByteIterator> values, byte[] read_buffer) throws Exception {
    String logKey = TardisYCSBConfig.getPWLogKey(id);
    boolean isFirstTime = false;
    
    Object obj = memcachedClient.get(logKey, getHashCode(id), false);    
    byte[] payload = null;
    
    if (obj != null && obj instanceof byte[]) {
      payload = (byte[]) obj;
    } else {
      if (obj != null) {
        
      }
    }
      
    HashMap<String, ByteIterator> m = new HashMap<>();
    if (payload != null) {
      logger.debug("Got the PW value key="+logKey);
      CacheUtilities.unMarshallHashMap(m, payload, read_buffer);      
    } else {
      logger.debug("Miss the PW value key="+logKey);
      isFirstTime = true;
    }
    m.putAll(values);
    payload = CacheUtilities.SerializeHashMap(m);
    try {
      if (memcachedClient.set(logKey, payload, getHashCode(id)) == false) {
        if (TardisYCSBConfig.monitorSpaceWritesRatio) {
          throw new Exception();
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return isFirstTime;
  }

}