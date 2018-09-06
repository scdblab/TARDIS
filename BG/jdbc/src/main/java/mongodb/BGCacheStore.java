package mongodb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.log4j.Logger;

import com.usc.dblab.cafe.CacheEntry;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.QueryResult;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;

public class BGCacheStore implements CacheStore {
  public static final char ACT_VIEW_PROFILE = 'V';
  public static final char ACT_LIST_FRIENDS = 'F';
  public static final char ACT_LIST_PENDING_FRIENDS = 'P';
  public static final char ACT_INV = 'I';
  public static final char ACT_ACP = 'A';
  public static final char ACT_REJECT = 'R';
  public static final char ACT_THAW = 'T';
  
  public static final String KEY_PRE_PROFILE = "VP";
  public static final String KEY_PRE_FRIENDS_LIST = "LF";
  public static final String KEY_PRE_PENDING_LIST = "PF";
  public static final String KEY_PRE_FRIEND_COUNT = "FC";
  public static final String KEY_PRE_PENDING_COUNT = "PC";
    
  private DB db;
  
  private final static Logger logger = Logger.getLogger(BGCacheStore.class);
  
  public BGCacheStore(DB db) {
    this.db = db;
  }

  @Override
  public Set<Change> updateCacheEntries(String dml, Set<String> keys) {
    Set<Change> changes = new HashSet<>();
    char action = dml.charAt(0);
    String[] tokens = dml.split(",");
    String inviter = tokens[1];
    String invitee = tokens[2];
    
    Change change;
    switch (action) {
    case ACT_INV:
      change = new Change(Change.TYPE_APPEND, KEY_PRE_PENDING_LIST+invitee, ",+"+inviter);
      changes.add(change);
      
      change = new Change(Change.TYPE_INCR, KEY_PRE_PENDING_COUNT+invitee, "+1");
      changes.add(change);
      return changes;
    case ACT_REJECT:
      change = new Change(Change.TYPE_RMW, KEY_PRE_PENDING_LIST+invitee, ",-"+inviter);
      changes.add(change);
      
      change = new Change(Change.TYPE_INCR, KEY_PRE_PENDING_COUNT+invitee, "-1");
      changes.add(change);
      return changes;
    case ACT_ACP:
      change = new Change(Change.TYPE_RMW, KEY_PRE_PENDING_LIST+invitee, ",-"+inviter);
      changes.add(change);
      
      change = new Change(Change.TYPE_INCR, KEY_PRE_PENDING_COUNT+invitee, "-1");
      changes.add(change);
      
      change = new Change(Change.TYPE_APPEND, KEY_PRE_FRIENDS_LIST+invitee, ",+"+inviter);
      changes.add(change);
      
      change = new Change(Change.TYPE_INCR, KEY_PRE_FRIEND_COUNT+invitee, "+1");
      changes.add(change);
      
      change = new Change(Change.TYPE_APPEND, KEY_PRE_FRIENDS_LIST+inviter, ",+"+invitee);
      changes.add(change);
      
      change = new Change(Change.TYPE_INCR, KEY_PRE_FRIEND_COUNT+inviter, "+1");
      changes.add(change);
      return changes;
    case ACT_THAW:
      change = new Change(Change.TYPE_APPEND, KEY_PRE_FRIENDS_LIST+invitee, ",-"+inviter);
      changes.add(change);
      
      change = new Change(Change.TYPE_INCR, KEY_PRE_FRIEND_COUNT+invitee, "-1");
      changes.add(change);
      
      change = new Change(Change.TYPE_APPEND, KEY_PRE_FRIENDS_LIST+inviter, ",-"+invitee);
      changes.add(change);
      
      change = new Change(Change.TYPE_INCR, KEY_PRE_FRIEND_COUNT+inviter, "-1");
      changes.add(change);
      break;
    }
    
    return changes;
  }

  @Override
  public Set<String> getReferencedKeysFromQuery(String query) {
    char action = query.charAt(0);
    Set<String> keys = new HashSet<>();
    String[] tokens = query.split(",");
    String requesterID = tokens[1];
    String profileOwnerId = tokens[2];
    
    switch (action) {
    case ACT_VIEW_PROFILE:
      keys.add(KEY_PRE_PROFILE+profileOwnerId);
      keys.add(KEY_PRE_FRIEND_COUNT+profileOwnerId);
      if (requesterID.equals(profileOwnerId))
        keys.add(KEY_PRE_PENDING_COUNT+profileOwnerId);
      break;
    case ACT_LIST_FRIENDS:
      keys.add(KEY_PRE_FRIENDS_LIST+profileOwnerId);
      break;
    case ACT_LIST_PENDING_FRIENDS:
      keys.add(KEY_PRE_PENDING_LIST+profileOwnerId);
      break;
    }
    
    return keys;
  }

  @Override
  public Set<String> getImpactedKeysFromDml(String dml) {
    char action = dml.charAt(0);
    Set<String> keys = new HashSet<>();
    String[] tokens = dml.split(",");
    String inviter = tokens[1];
    String invitee = tokens[2];
    
    switch (action) {
    case ACT_INV:
      keys.add(KEY_PRE_PENDING_LIST+invitee);
      keys.add(KEY_PRE_PENDING_COUNT+invitee);
      break;
    case ACT_REJECT:
      keys.add(KEY_PRE_PENDING_LIST+invitee);
      keys.add(KEY_PRE_PENDING_COUNT+invitee);
      break;
    case ACT_ACP:
      keys.add(KEY_PRE_PENDING_LIST+invitee);
      keys.add(KEY_PRE_PENDING_COUNT+invitee);
      keys.add(KEY_PRE_FRIENDS_LIST+inviter);
      keys.add(KEY_PRE_FRIEND_COUNT+inviter);
      keys.add(KEY_PRE_FRIENDS_LIST+invitee);
      keys.add(KEY_PRE_FRIEND_COUNT+invitee);
      break;
    case ACT_THAW:
      keys.add(KEY_PRE_FRIENDS_LIST+inviter);
      keys.add(KEY_PRE_FRIEND_COUNT+inviter);
      keys.add(KEY_PRE_FRIENDS_LIST+invitee);
      keys.add(KEY_PRE_FRIEND_COUNT+invitee);
      break;
    }
    
    return keys;
  }

  @Override
  public QueryResult queryDataStore(String query) {
    char action = query.charAt(0);
    String[] tokens = query.split(",");
    int requesterID = Integer.parseInt(tokens[1]);
    int profileOwnerID = Integer.parseInt(tokens[2]);
    
    QueryResult result = null;
    switch (action) {
    case ACT_VIEW_PROFILE:
      HashMap<String, ByteIterator> map = new HashMap<>();
      db.viewProfile(requesterID, profileOwnerID, map, false, false);
      result = new QueryResult(query, map);
      break;
    case ACT_LIST_FRIENDS:
      //Vector<HashMap<String, ByteIterator>> vector = new Vector<>();
      List<String> res = ((MongoBGClientDelegate)db).listFriends(profileOwnerID);
      result = new QueryResult(query, res);
      break;
    case ACT_LIST_PENDING_FRIENDS:
      //vector = new Vector<>();
      res = ((MongoBGClientDelegate)db).listPendingFriends(profileOwnerID);
      result = new QueryResult(query, res);
      break;
    }
    
    return result;
  }

  @Override
  public Set<CacheEntry> computeCacheEntries(String query, QueryResult result) {
    char action = query.charAt(0);
    Set<CacheEntry> entries = new HashSet<>();
    
    List<String> list;
    switch (action) {
    case ACT_VIEW_PROFILE:
      String requesterId = query.split(",")[1];
      String profileOwnerId = query.split(",")[2];
      @SuppressWarnings("unchecked") HashMap<String, ByteIterator> map = (HashMap<String, ByteIterator>)result.getValue();
      CacheEntry entry = new CacheEntry(KEY_PRE_PROFILE+profileOwnerId, map, false);
      entries.add(entry);
      
      String fc = new String(map.remove("friendcount").toArray());
      entry = new CacheEntry(KEY_PRE_FRIEND_COUNT+profileOwnerId, fc, true);
      entries.add(entry);
      
      if (requesterId.equals(profileOwnerId)) {
        String pc = new String(map.remove("pendingcount").toArray());
        entry = new CacheEntry(KEY_PRE_PENDING_COUNT+profileOwnerId, pc, true);
        entries.add(entry);
      }
      break;
    case ACT_LIST_FRIENDS:
      profileOwnerId = query.split(",")[2];
      list = (List<String>) result.getValue();
      entry = new CacheEntry(KEY_PRE_FRIENDS_LIST+profileOwnerId, 
          MemcachedSetHelper.convertString(list), true);
      entries.add(entry);
      break;
    case ACT_LIST_PENDING_FRIENDS:
      profileOwnerId = query.split(",")[2];
      list = (List<String>) result.getValue();
      entry = new CacheEntry(KEY_PRE_PENDING_LIST+profileOwnerId, 
          MemcachedSetHelper.convertString(list), true);
      entries.add(entry);
      break;
    }
    
    return entries;
  }

  @Override
  public boolean dmlDataStore(String dml) {
    char action = dml.charAt(0);
    String[] tokens = dml.split(",");
    int inviter = Integer.parseInt(tokens[1]);
    int invitee = Integer.parseInt(tokens[2]);
    
    int res = 0;
    switch (action) {
    case ACT_INV:
      res = db.inviteFriend(inviter, invitee);
      break;
    case ACT_REJECT:
      res = db.rejectFriend(inviter, invitee);
      break;
    case ACT_ACP:
      res = db.acceptFriend(inviter, invitee);
      break;
    case ACT_THAW:
      res = db.thawFriendship(inviter, invitee);
      break;
    }
    
    return (res == 0) ? true : false;
  }

  @Override
  public CacheEntry applyChange(Change change, CacheEntry cacheEntry) {
    if (!cacheEntry.getKey().equals(change.getKey())) {
      throw new RuntimeException("The change is not for this entry. Something went wrong.");
    }
    
    String key = change.getKey();
    if (key.startsWith(KEY_PRE_PROFILE)) {
      throw new NotImplementedException("Should not have change to this key "+key);
    } else if (key.startsWith(KEY_PRE_FRIEND_COUNT) || key.startsWith(KEY_PRE_PENDING_COUNT)) {
      long cnt = Long.parseLong((String)cacheEntry.getValue());
      String val = (String)change.getValue();
      cnt += Integer.parseInt(val);
      return new CacheEntry(key, cnt, true);
    } else if (key.startsWith(KEY_PRE_FRIENDS_LIST) || key.startsWith(KEY_PRE_PENDING_LIST)) {
      Set<String> set = MemcachedSetHelper.convertSet(cacheEntry.getValue());
      String val = (String)change.getValue();
      List<String> cc = MemcachedSetHelper.convertList(val);
      for (String c: cc) {
        char op = c.charAt(0);
        val = c.substring(1);
        switch (op) {
        case '+':
          set.add(val);
          break;
        case '-':
          set.remove(val);
          break;
        }
      }
      String list = MemcachedSetHelper.convertString(set);
      return new CacheEntry(key, list, true);
    }
    
    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public byte[] serialize(CacheEntry cacheEntry) {
    String key = cacheEntry.getKey();
    Object obj = cacheEntry.getValue();
    
    byte[] bytes = null;
    if (key.startsWith(KEY_PRE_PROFILE)) {
      bytes = CacheUtilities.SerializeHashMap((HashMap<String, ByteIterator>)obj);
    } else {
      bytes = ((String)obj).getBytes();
    }
    
    return bytes;
  }

  @Override
  public CacheEntry deserialize(String key, byte[] bytes, byte[] buffer) {
    Object obj = null;
    if (key.startsWith(KEY_PRE_PROFILE)) {
      HashMap<String, ByteIterator> m = new HashMap<>();
      CacheUtilities.unMarshallHashMap(m, bytes, buffer);
      obj = m;
    } else {
      // convert to string
      obj = new String(bytes);
    }
    
    return new CacheEntry(key, obj, false);
  }

  @Override
  public byte[] serialize(Change change) {
    return ((String)change.getValue()).getBytes();
  }

  @Override
  public int getHashCode(String key) {
    return Integer.parseInt(key.replaceAll("[^0-9]", ""));
  }
}
