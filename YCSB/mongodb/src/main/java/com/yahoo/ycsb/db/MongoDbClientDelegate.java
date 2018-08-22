package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;

public class MongoDbClientDelegate extends MongoDbClient {
  public final static AtomicBoolean isDatabaseFailed = new AtomicBoolean(false);

  @Override
  public Status delete(String table, String key) {
    if (isDatabaseFailed.get()) return Status.SERVICE_UNAVAILABLE;
    return super.delete(table, key);
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    if (isDatabaseFailed.get()) return Status.SERVICE_UNAVAILABLE;
    return super.insert(table, key, values);
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    if (isDatabaseFailed.get()) return Status.SERVICE_UNAVAILABLE;
    return super.read(table, key, fields, result);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, 
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    if (isDatabaseFailed.get()) return Status.SERVICE_UNAVAILABLE;
    return super.scan(table, startkey, recordcount, fields, result);
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    if (isDatabaseFailed.get()) return Status.SERVICE_UNAVAILABLE;
    return super.update(table, key, values);
  }

}
