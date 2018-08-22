package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import org.apache.log4j.Logger;
import com.meetup.memcached.CLValue;
import com.meetup.memcached.IQException;
import com.meetup.memcached.MemcachedLease;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import static com.yahoo.ycsb.db.TardisYCSBConfig.*;
import static com.yahoo.ycsb.db.MongoDbClientDelegate.isDatabaseFailed;

/**
 * MongoDB binding for YCSB framework using the MongoDB Inc. <a
 * href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 * 
 * @author ypai
 * @see <a href="http://docs.mongodb.org/ecosystem/drivers/java/">MongoDB Inc.
 *      driver</a>
 */
public class CADSRedleaseMongoDbClient extends CADSMongoDbClient {
  MemcachedLease leaseClient;
  RecoveryEngine recovery;
  
  private final Logger logger = Logger.getLogger(CADSRedleaseMongoDbClient.class);

  private static final boolean lockRead = false;
  
  public CADSRedleaseMongoDbClient() {
    super();
  }

  @Override
  public Status delete(String table, String key) {
    return client.delete(table, key);
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    return super.insert(table, key, values);
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {  
    logger.debug("Read user with key="+key+".");
    
    if (cacheMode == CACHE_NO_CACHE) {
      return client.read(table, key, fields, result);
    }
    
    if (TardisYCSBConfig.monitorSpaceWritesRatio) {
      return Status.OK;
    }
    
    long id = extractUserId(key);
    Status status = Status.OK; 
    
    try {
      if (lockRead) {
        leaseClient.acquireTillSuccess(
            getUserLogLeaseKey(id), getHashCode(id), TardisYCSBConfig.LEASE_TIMEOUT);
      }

      byte[] payload = (byte[]) mc.get(key, getHashCode(id), false);
      if (payload != null) {
        logger.debug("Cache hit key="+key);
        cacheHits.incrementAndGet();
        CacheUtilities.unMarshallHashMap(result, payload, read_buffer);
        return status;
      }
       
      if (TardisYCSBConfig.fullWarmUp) {
        logger.fatal("BUG: cache miss read key=" + key);
      }
      logger.debug("Cache miss key="+key);
      cacheMisses.incrementAndGet();
      
      if (isDatabaseFailed.get()) {
        return Status.SERVICE_UNAVAILABLE;
      }

      try {
        if (!lockRead) {
          leaseClient.acquireTillSuccess(getUserLogLeaseKey(id), 
              getHashCode(id), TardisYCSBConfig.LEASE_TIMEOUT);
        }
      
        if (cacheMode != CACHE_WRITE_BACK || !RecoveryResult.FAIL.equals(
            recovery.recover(RecoveryCaller.READ, key, id, read_buffer))) {
          status = client.read(table, key, fields, result);
          if (status != Status.OK) {
            return status;
          }
        
          payload = CacheUtilities.SerializeHashMap(result);
          mc.set(key, payload, getHashCode(id));
          logger.debug("Set key to cache successfully key="+key
              +", payload size="+payload.length);
        } else {
          return Status.SERVICE_UNAVAILABLE;
        }      
      } catch (Exception e) {
        logger.error("Read failed key= "+key, e);
      } finally {
        if (!lockRead) {
          leaseClient.releaseLease(getUserLogLeaseKey(id), getHashCode(id));
        }  
      }      
    } catch (Exception e) {
      logger.error("Read failed key= "+key, e);
    } finally {    
      if (lockRead) {
        leaseClient.releaseLease(getUserLogLeaseKey(id), getHashCode(id));
      }
    }
    
    return status;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return client.scan(table, startkey, recordcount, fields, result);
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    logger.debug("Update key="+key);
    
    if (cacheMode == CACHE_NO_CACHE) {
      return client.update(table, key, values);
    }
    
    long id = extractUserId(key);
    Status status = Status.OK;
    boolean cacheUpdateFailed = false;
    boolean updateFailed = false;
    boolean firstTimeDirty = false;
    
    try {
      if (!TardisYCSBConfig.monitorSpaceWritesRatio) {
        leaseClient.acquireTillSuccess(getUserLogLeaseKey(id), 
            getHashCode(id), TardisYCSBConfig.LEASE_TIMEOUT);
      }
      
      switch (cacheMode) {
      case CACHE_WRITE_AROUND:
        // delete key-value pair
        mc.delete(key, getHashCode(id), null);
        logger.debug("Write around delete successfully key="+key);
        break;
      case CACHE_WRITE_THROUGH:
      case CACHE_WRITE_BACK:
        // update key-value pair
        byte[] payload = (byte[]) mc.get(key, getHashCode(id), false);      
        HashMap<String, ByteIterator> m = new HashMap<String, ByteIterator>();
        if (payload != null) {
          logger.debug("Cache hit. Update cache value.");
          CacheUtilities.unMarshallHashMap(m, payload, read_buffer);    
          m.putAll(values);
          payload = CacheUtilities.SerializeHashMap(m);
          if (mc.set(key, payload, getHashCode(id)) == false) {
            if (TardisYCSBConfig.monitorSpaceWritesRatio) {
              throw new Exception();
            }
          }
        } else {
          logger.debug("Got no cache value.");
          cacheUpdateFailed = true;
        }        
        break;
      default:
        break;  
      }
      
      if ((cacheMode != CACHE_WRITE_BACK) && !isDatabaseFailed.get()) {
        // database is up
        if (!RecoveryResult.FAIL.equals(
            recovery.recover(RecoveryCaller.WRITE, key, id, read_buffer))) {
          logger.debug("Database is up. Perform update against PStore.");
          if (!TardisYCSBConfig.SKIP_UPDATE_MONGO) {
            status = client.update(table, key, values);
          } else {
            status = Status.OK;
          }
          
          if (status != Status.OK) {
            logger.debug("Update to PStore failed.");
            updateFailed = true;
          }
        } else {
          logger.debug("Recover failed.");
          updateFailed = true;
        }
      } else {
        logger.debug("Write back mode or the database still fails.");
        updateFailed = true;
      }
      
      if ((cacheMode == CACHE_WRITE_BACK) || 
          (cacheMode == CACHE_WRITE_THROUGH && updateFailed)) {
        logger.debug("Tardis buffer write");
        if (cacheUpdateFailed || TardisYCSBConfig.monitorLostWrite) {          
          firstTimeDirty = recovery.bufferWrites(key, id, values, read_buffer);
          logger.debug("firstTimeDirty="+firstTimeDirty);
        } else {
          firstTimeDirty = recovery.addDirtyFlag(key, id);
          logger.debug("firstTimeDirty="+firstTimeDirty);
        }
      }
      logger.debug("Uppdate database " + updateFailed + 
          " update cache " + cacheUpdateFailed);
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Update failed key=" + key, e);
      TardisRecoveryEngine.outputAndExit();
    } finally {
      if (!TardisYCSBConfig.monitorSpaceWritesRatio) {
        leaseClient.releaseLease(getUserLogLeaseKey(id), getHashCode(id));
      }
      if (updateFailed) {
        try {
          insertToEWList(firstTimeDirty, key, id);
        } catch (Exception e) {
          e.printStackTrace();
          TardisRecoveryEngine.outputAndExit();
        }
      }
    }
    
    TardisRecoveryEngine.successfulWriteCount.incrementAndGet();
    return status;
  }
  
  private void insertToEWList(boolean firstTimeDirty, String key, long id) throws Exception {
    if (firstTimeDirty) {
      if (!TardisYCSBConfig.monitorSpaceWritesRatio) {
        leaseClient.acquireTillSuccess(
            getEWLogMutationLeaseKeyFromUserId(id), 
            TardisYCSBConfig.getEWId(id), LEASE_TIMEOUT);
      }
      
      recovery.insertUserToPartitionLog(key, id);
      
      if (!TardisYCSBConfig.monitorSpaceWritesRatio) {
        leaseClient.releaseLease(getEWLogMutationLeaseKeyFromUserId(id), 
            TardisYCSBConfig.getEWId(id));
      }
    }
  }
  
  protected boolean addDeltaToPW(String sid, long id, HashMap<String, ByteIterator> values) throws IQException {
    CLValue val = null;
    String pw = TardisYCSBConfig.getPWLogKey(id);
    
    // must perform read-modify-write to prevent the cache size going too large
    // that exceeds memcache limit.
    val = mc.ewread(sid, pw, getHashCode(id), false);
    if (val == null) {
      logger.fatal("Get pw CLVal null");
      System.exit(-1);
    }
    
    byte[] delta;
    if (val.getValue() == null) {
      delta = CacheUtilities.SerializeHashMap(values);
    } else {      
      HashMap<String, ByteIterator> m = new HashMap<>();
      CacheUtilities.unMarshallHashMap(m, (byte[]) val.getValue(), read_buffer);
      m.putAll(values);
      delta = CacheUtilities.SerializeHashMap(m);
    }
    
    if (delta != null) {
      mc.ewswap(sid, pw, getHashCode(id), delta);
      logger.debug("Added delta to PW");
    }
    
    return (val.getValue() == null);
    
//    TimedMetrics.getInstance().add(MetricsName.METRICS_DBFAIL_MEM_OVERHEAD, str.getBytes().length);
  }
  
  private int hashKey(long id) {
    return (int)(id % TardisYCSBConfig.NUM_EVENTUAL_WRITE_LOGS);
  }

  private void addUserToEW(String key) {
    logger.debug("Add user to EW.");
    
    long id = Long.parseLong(key.replaceAll("[^0-9]", ""));
    
    String ewKey = getEWLogKey(hashKey(id));
    int ewHashCode = getHashCode(ewKey);
    
    while (true) {
      String tid = mc.generateSID();
      try {
        CLValue val = mc.ewappend(ewKey, ewHashCode, key+DELIMITER, tid);
        if ((boolean)val.getValue() == false)
          mc.ewswap(tid, ewKey, ewHashCode, DELIMITER+key+DELIMITER);
        mc.ewcommit(tid, ewHashCode, false);
        logger.debug("Add user to EW succeeded.");
        break;
      } catch (IQException e) { }

      sleepLeaseRetry();
    }
    
//    TimedMetrics.getInstance().add(MetricsName.METRICS_DBFAIL_MEM_OVERHEAD, ewKey.getBytes().length + (key+DELIMITER).getBytes().length);
//    TimedMetrics.getInstance().add(MetricsName.METRICS_DIRTY_USER);
  }
  
  @Override
  public void init() throws DBException {
    super.init();
    System.out.println("########## Version 3");
    
    leaseClient = new MemcachedLease(1, mc, false);
    recovery = new RecoveryEngine(mc, client);
  }
  
  @Override
  public void cleanup() throws DBException {
    super.cleanup();
  }
}
