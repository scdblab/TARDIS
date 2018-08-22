package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.meetup.memcached.COException;
import com.meetup.memcached.IQException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import static com.yahoo.ycsb.db.TardisYCSBConfig.sleepLeaseRetry;
import static com.yahoo.ycsb.db.TardisYCSBConfig.getHashCode;

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
public class CADSWaMongoDbClient extends CADSMongoDbClient {

  @Override
  public Status delete(String table, String key) {
    Status status = Status.OK;
    
    while (true) {
      try {
        String sid = mc.generateSID();
        mc.oqReg(sid, key, getHashCode(key));
        status = client.delete(table, key);
        mc.dCommit(sid); 
        break;
      } catch (COException e) {
        e.printStackTrace(System.out);
      }
      
      sleepLeaseRetry();
    }
    
    return status;
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    Status status = Status.OK;
    
    while (true) {
      try {
        String sid = mc.generateSID();
        mc.oqReg(sid, key, getHashCode(key));
        status = client.insert(table, key, values);
        mc.dCommit(sid); 
        break;
      } catch (COException e) {
        e.printStackTrace(System.out);
      }
      
      sleepLeaseRetry();
    }
    
    return status;
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    Status status = Status.OK;
    
    String sid;
    while (true) {
      sid = mc.generateSID();
      try {
        Object obj = mc.ciget(sid, key, false, getHashCode(key));
        if (obj != null) {
          CacheUtilities.unMarshallHashMap(result, (byte[])obj, 
              read_buffer);
          return status;
        }
        
        break;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    status = client.read(table, key, fields, result);
    
    byte[] payload = CacheUtilities.SerializeHashMap(result);
    
    if (payload != null) {
      try {
        mc.iqset(key, payload);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace(System.out);
      } catch (IQException e) {
        int err = 0;
      }
    }
    
    try {
      mc.dCommit(sid);
    } catch (COException e) {
      int err = 0;
    }
    
    return status;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, 
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return client.scan(table, startkey, recordcount, fields, result);
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    Status status = Status.OK;
    
    while (true) {
      try {
        String sid = mc.generateSID();
        mc.oqReg(sid, key, getHashCode(key));
        status = client.update(table, key, values);
        mc.dCommit(sid); 
        break;
      } catch (COException e) {
        e.printStackTrace(System.out);
      }
      
      sleepLeaseRetry();
    }
    
    return status;
  }
}
