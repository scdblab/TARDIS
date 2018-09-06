package mongodb;

import static tardis.TardisClientConfig.ACTION_PW_FRIENDS;
import static tardis.TardisClientConfig.ACTION_PW_PENDING_FRIENDS;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.WriteBack;

import edu.usc.bg.base.DB;

public class BGCacheBack implements WriteBack {
  private DB db;
  
  private final static Logger logger = Logger.getLogger(BGCacheBack.class);
  
  public static final String KEY_PRE_BUFF_WRITE = "BW";
  
  public BGCacheBack(DB db) {
    this.db = db;
  }
  
  @Override
  public Set<String> getMapping(String key) {
    int userId = Integer.parseInt(key.replaceAll("[^0-9]", ""));
    String buffKey = String.format("BW%d", userId);
    Set<String> buffKeys = new HashSet<>();
    buffKeys.add(buffKey);
    return buffKeys;
  }

  @Override
  public boolean registerMapping(String key, Set<String> buffKeys) {
    return true;
  }

  @Override
  public boolean unregisterMapping(String key) {
    return true;
  }

  @Override
  public Set<Change> bufferChanges(String dml, Set<String> buffKeys) {

    Set<Change> changes = new HashSet<>();
    char action = dml.charAt(0);
    String[] tokens = dml.split(",");
    String inviter = tokens[1];
    String invitee = tokens[2];
    
    switch (action) {
    case BGCacheStore.ACT_INV:
      Change change = new Change(Change.TYPE_APPEND, KEY_PRE_BUFF_WRITE+invitee, ",P+"+inviter);
      changes.add(change);
      break;
    case BGCacheStore.ACT_REJECT:
      change = new Change(Change.TYPE_APPEND, KEY_PRE_BUFF_WRITE+invitee,",P-"+inviter);
      changes.add(change);
      break;
    case BGCacheStore.ACT_ACP:
      change = new Change(Change.TYPE_APPEND, KEY_PRE_BUFF_WRITE+invitee, ",P-"+inviter+",F+"+inviter);
      changes.add(change);
      
      change = new Change(Change.TYPE_APPEND, KEY_PRE_BUFF_WRITE+inviter, ",F+"+invitee);
      changes.add(change);
      break;
    case BGCacheStore.ACT_THAW:
      change = new Change(Change.TYPE_APPEND, KEY_PRE_BUFF_WRITE+invitee, ",F-"+inviter);
      changes.add(change);
      
      change = new Change(Change.TYPE_APPEND, KEY_PRE_BUFF_WRITE+inviter, ",F-"+invitee);
      changes.add(change);
      break;
    }
    
    return changes;
  }

  @Override
  public boolean applyBufferedWrite(String buffKey, Object buffValue) {
    Set<String> pendingFriends = new HashSet<>();
    Set<String> friends = new HashSet<>();
    int userId = Integer.parseInt(buffKey.replaceAll("[^0-9]", ""));
    
    List<String> pendingWrites = MemcachedSetHelper.convertList(buffValue);
    
    Map<String, Integer> friendsMap = new HashMap<>();
    Map<String, Integer> pendingFriendsMap = new HashMap<>();

    Set<String> addFriends = new HashSet<>();
    Set<String> removeFriends = new HashSet<>();
    Set<String> addPendingFriends = new HashSet<>();
    Set<String> removePendingFriends = new HashSet<>();

    for (String write : pendingWrites) {
      char act = write.charAt(0);
      char op = write.charAt(1);
      String user = write.substring(2);

      switch (act) {
      case ACTION_PW_FRIENDS:
        if (op == '+') {
          increment(friendsMap, user);
        } else if (op == '-') {
          decrement(friendsMap, user);
        } else {
          logger.fatal("Unrecognized pending write action " + write);
        }
        break;
      case ACTION_PW_PENDING_FRIENDS:
        if (op == '+') {
          increment(pendingFriendsMap, user);
        } else if (op == '-') {
          decrement(pendingFriendsMap, user);
        } else {
          logger.fatal("Unrecognized pending write action " + write);
        }
        break;
      default:
        logger.fatal("Unrecognized pending write action " + write);
        break;
      }
    }

    consolidate(friendsMap, addFriends, removeFriends);
    consolidate(pendingFriendsMap, addPendingFriends, removePendingFriends);

    friends.addAll(addFriends);
    pendingFriends.addAll(addPendingFriends);
    removeFriends.forEach(f -> {
      friends.remove(f);
    });
    removePendingFriends.forEach(p -> {
      pendingFriends.remove(p);
    });
    
    int res = ((MongoBGClient)db).updateUserDocument(String.valueOf(userId), friends, pendingFriends, 
        null, null, null, null);
    
    return (res == 0) ? true : false;
  }

  @Override
  public boolean isIdempotent(Object buffValue) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Object convertToIdempotent(Object buffValue) {
    // TODO Auto-generated method stub
    return null;
  }
  
  private static void increment(Map<String, Integer> map, String key) {
    Integer value = map.get(key);
    if (value == null) {
      map.put(key, 1);
    } else {
      map.put(key, value + 1);
    }
  }

  private static void decrement(Map<String, Integer> map, String key) {
    Integer value = map.get(key);
    if (value == null) {
      map.put(key, -1);
    } else {
      map.put(key, value - 1);
    }
  }

  private static void consolidate(Map<String, Integer> map, Set<String> addResult,
      Set<String> removeResult) {
    map.forEach((k, v) -> {
      if (v < 0) {
        removeResult.add(k);
      } else if (v > 0) {
        addResult.add(k);
      }
    });
  }
}
