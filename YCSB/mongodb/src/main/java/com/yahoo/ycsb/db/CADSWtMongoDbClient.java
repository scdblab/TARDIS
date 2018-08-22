package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import static com.yahoo.ycsb.db.TardisYCSBConfig.getHashCode;
import static com.yahoo.ycsb.db.TardisYCSBConfig.sleepLeaseRetry;

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
public class CADSWtMongoDbClient extends CADSMongoDbClient {

  @Override
  public Status delete(String table, String key) {
    int hashCode = getHashCode(key);
    Set<Integer> set = new HashSet<>();
    set.add(hashCode);
    
    Status status = Status.OK;
    while (true) {
      String sid = mc.generateSID();
      
      try {  
      Object val = mc.oqRead(sid, key, hashCode, false);
      status = client.delete(table, key);
      
      if (status == Status.OK) {
        mc.oqSwap(sid, key, hashCode, null);
        mc.dCommit(sid);
        break;
      }
      else
        mc.release(sid);
      } catch (Exception e) {
        try {
        mc.release(sid);
        } catch (Exception ex) {
          System.out.println("Release error");
        }
      }
      
      sleepLeaseRetry();
    }
    
    return status;
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    Status status;
    int hashCode = getHashCode(key);
    
    while (true) {
    String sid = mc.generateSID();

    }
  }

  @Override
  public Status read(String arg0, String arg1, Set<String> arg2,
      HashMap<String, ByteIterator> arg3) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Status scan(String arg0, String arg1, int arg2, Set<String> arg3,
      Vector<HashMap<String, ByteIterator>> arg4) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Status update(String arg0, String arg1,
      HashMap<String, ByteIterator> arg2) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void init() throws DBException {
    
  }
}
