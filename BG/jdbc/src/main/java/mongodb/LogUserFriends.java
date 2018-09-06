package mongodb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;

public class LogUserFriends extends DB {

  @Override
  public int insertEntity(String entitySet, String entityPK,
      HashMap<String, ByteIterator> values, boolean insertImage) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int viewProfile(int requesterID, int profileOwnerID,
      HashMap<String, ByteIterator> result, boolean insertImage,
      boolean testMode) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int listFriends(int requesterID, int profileOwnerID,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
      boolean insertImage, boolean testMode) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int viewFriendReq(int profileOwnerID,
      Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
      boolean testMode) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int acceptFriend(int inviterID, int inviteeID) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int rejectFriend(int inviterID, int inviteeID) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int inviteFriend(int inviterID, int inviteeID) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int viewTopKResources(int requesterID, int profileOwnerID, int k,
      Vector<HashMap<String, ByteIterator>> result) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getCreatedResources(int creatorID,
      Vector<HashMap<String, ByteIterator>> result) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int viewCommentOnResource(int requesterID, int profileOwnerID,
      int resourceID, Vector<HashMap<String, ByteIterator>> result) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int postCommentOnResource(int commentCreatorID, int resourceCreatorID,
      int resourceID, HashMap<String, ByteIterator> values) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int delCommentOnResource(int resourceCreatorID, int resourceID,
      int manipulationID) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int thawFriendship(int friendid1, int friendid2) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HashMap<String, String> getInitialStats() {
    // TODO Auto-generated method stub
    return null;
  }
  
  Map<Integer, Set<Integer>> friendList = new HashMap<>();

  @Override
  public int CreateFriendship(int friendid1, int friendid2) {
    insert(friendid1, friendid2);
    insert(friendid2, friendid1);
    return 0;
  }
  
  private void insert(int friendid1, int friendid2) {
    Set<Integer> friends = this.friendList.get(friendid1);
    if (friends == null) {
    friends = new HashSet<>();
    this.friendList.put(friendid1, friends);
    }
    friends.add(friendid2);
  }

  @Override
  public void createSchema(Properties props) {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public void cleanup(boolean warmup) throws DBException {
    System.out.println("###clean");
    
    if (!friendList.isEmpty()) {
      
      StringBuilder sb = new StringBuilder();
      for (Integer user: friendList.keySet()) {
        sb.append(user);
        for (Integer f: friendList.get(user)) {
          sb.append(" "+f);
        }
        sb.append("\n");
      }
      
      BufferedWriter writer = null;
      try {
          //create a temporary file
          File logFile = new File("/home/hieun/Desktop/friendMap.txt");

          // This will output the full path where the file will be written to...
          System.out.println(logFile.getCanonicalPath());

          writer = new BufferedWriter(new FileWriter(logFile));
          writer.write(sb.toString());
      } catch (Exception e) {
          e.printStackTrace();
      } finally {
          try {
              // Close the writer regardless of what happens...
              writer.close();
          } catch (Exception e) {
          }
      }
      
      friendList.clear();
    }
    
    super.cleanup(warmup);
  }

  @Override
  public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int queryConfirmedFriendshipIds(int memberID,
      Vector<Integer> confirmedIds) {
    // TODO Auto-generated method stub
    return 0;
  }

}
