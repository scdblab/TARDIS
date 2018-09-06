package mongodb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import tardis.TardisClientConfig;

import com.meetup.memcached.COException;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DBException;

public class CLMongoDBClientInv extends MemMongoClient {
  public CLMongoDBClientInv() {
    Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
    mongoLogger.setLevel(Level.SEVERE); 
  }

  @Override
  public boolean init() throws DBException {
    return super.init();
  }

  @Override
  public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
      boolean insertImage, boolean testMode) {
    return super.viewProfile(requesterID, profileOwnerID, result, insertImage, testMode);
  }

  @Override
  public int listFriends(int requesterID, int profileOwnerID, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
    return super.listFriends(requesterID, profileOwnerID, 
        fields, result, insertImage, testMode);
  }

  @Override
  public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> result, boolean insertImage,
      boolean testMode) {
    return super.viewFriendReq(profileOwnerID, result, insertImage, testMode);
  }

  @Override
  public int inviteFriend(int inviterID, int inviteeID) {	
    String kjpcount = TardisClientConfig.KEY_PENDING_FRIEND_COUNT + inviteeID;
    String kjplist = TardisClientConfig.KEY_LIST_FRIENDS_REQUEST + inviteeID;

    while (true) {
      String tid = mc.generateSID();	
      Set<Integer> hashCodes = new HashSet<>();
      hashCodes.add(inviteeID);
      try {
        mc.oqReg(tid, kjpcount, inviteeID);
        mc.oqReg(tid, kjplist, inviteeID);			

        client.inviteFriend(inviterID, inviteeID);

        mc.dCommit(tid, inviteeID);			
        break;
      } catch (COException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
        try {
          mc.release(tid, hashCodes);
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

      try {
        Thread.sleep(Q_LEASE_BACKOFF_TIME);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    return SUCCESS;
  }

  @Override
  public int rejectFriend(int inviterID, int inviteeID) {		
    String kjpcount = TardisClientConfig.KEY_PENDING_FRIEND_COUNT + inviteeID;
    String kjplist = TardisClientConfig.KEY_LIST_FRIENDS_REQUEST + inviteeID;

    while (true) {
      String tid = mc.generateSID();
      Set<Integer> hashCodes = new HashSet<>();
      hashCodes.add(inviteeID);
      try {
        mc.oqReg(tid, kjpcount, inviteeID);
        mc.oqReg(tid, kjplist, inviteeID);

        client.rejectFriend(inviterID, inviteeID);

        mc.dCommit(tid, inviteeID);				
        break;
      } catch (COException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        try {
          mc.release(tid, hashCodes);
        } catch (Exception e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }

      try {
        Thread.sleep(Q_LEASE_BACKOFF_TIME);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    return SUCCESS;
  }

  public boolean acceptFriendInviter(int inviterID, int inviteeID) {
    String kcficount = TardisClientConfig.KEY_FRIEND_COUNT + inviterID;
    String kcfilist = TardisClientConfig.KEY_LIST_FRIENDS + inviterID;

    while (true) {
      String tid = mc.generateSID();
      Set<Integer> hashCodes = new HashSet<>();
      hashCodes.add(inviterID);
      try {
        mc.oqReg(tid, kcficount, inviterID);
        mc.oqReg(tid, kcfilist, inviterID);

        client.acceptFriendInviter(inviterID, inviteeID);

        mc.dCommit(tid, inviterID);
        break;
      } catch (COException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        try {
          mc.release(tid, hashCodes);
        } catch (Exception e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }

      try {
        Thread.sleep(Q_LEASE_BACKOFF_TIME);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        //				e.printStackTrace();
      }
    }

    return true;
  }

  public boolean acceptFriendInvitee(int inviterID, int inviteeID) {
    String kcfjcount = TardisClientConfig.KEY_FRIEND_COUNT + inviteeID;
    String kpfjcount = TardisClientConfig.KEY_PENDING_FRIEND_COUNT + inviteeID;
    String kcfjlist = TardisClientConfig.KEY_LIST_FRIENDS + inviteeID;
    String kpfjlist = TardisClientConfig.KEY_LIST_FRIENDS_REQUEST + inviteeID;

    while (true) {
      String tid = mc.generateSID();
      Set<Integer> hashCodes = new HashSet<>();
      hashCodes.add(inviteeID);
      try {
        mc.oqReg(tid, kpfjcount, inviteeID);
        mc.oqReg(tid, kcfjcount, inviteeID);
        mc.oqReg(tid, kpfjlist, inviteeID);
        mc.oqReg(tid, kcfjlist, inviteeID);

        client.acceptFriendInvitee(inviterID, inviteeID);

        mc.dCommit(tid, inviteeID);
        break;
      } catch (COException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        try {
          mc.release(tid, hashCodes);
        } catch (Exception e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }

      try {
        Thread.sleep(Q_LEASE_BACKOFF_TIME);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    return true;
  }

  @Override
  public int acceptFriend(int inviterID, int inviteeID) {		
    boolean dbfail1 = false, dbfail2 = false;
    dbfail1 = acceptFriendInvitee(inviterID, inviteeID);
    dbfail2 = acceptFriendInviter(inviterID, inviteeID);
    if (dbfail1 || dbfail2) {
    }
    return SUCCESS;
  }

  public boolean thawFriend(int friendid1, int friendid2) {
    String kcf1count = TardisClientConfig.KEY_FRIEND_COUNT + friendid1;
    String kcf1list = TardisClientConfig.KEY_LIST_FRIENDS + friendid1;

    while (true) {
      String tid = mc.generateSID();
      Set<Integer> hashCodes = new HashSet<>();
      hashCodes.add(friendid1);
      try {
        mc.oqReg(tid, kcf1count, friendid1);
        mc.oqReg(tid, kcf1list, friendid1);

        client.thawFriendInviter(friendid1, friendid2);

        mc.dCommit(tid, friendid1);

        break;
      } catch (COException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        try {
          mc.release(tid, hashCodes);
        } catch (Exception e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }

      try {
        Thread.sleep(Q_LEASE_BACKOFF_TIME);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    return true;
  }

  @Override
  public int thawFriendship(int friendid1, int friendid2) {
    boolean dbfail1 = false, dbfail2 = false;
    dbfail1 = thawFriend(friendid1, friendid2);
    dbfail2 = thawFriend(friendid2, friendid1);
    if (dbfail1 || dbfail2) {
    }
    return SUCCESS;
  }

  @Override
  public int insertEntity(String entitySet, String entityPK,
      HashMap<String, ByteIterator> values, boolean insertImage) {
    return super.insertEntity(entitySet, entityPK, values, insertImage);
  }

  @Override
  public int viewTopKResources(int requesterID, int profileOwnerID, int k,
      Vector<HashMap<String, ByteIterator>> result) {
    return super.viewTopKResources(requesterID, profileOwnerID, k, result);
  }

  @Override
  public int getCreatedResources(int creatorID,
      Vector<HashMap<String, ByteIterator>> result) {
    return super.getCreatedResources(creatorID, result);
  }

  @Override
  public int viewCommentOnResource(int requesterID, int profileOwnerID,
      int resourceID, Vector<HashMap<String, ByteIterator>> result) {
    return super.viewCommentOnResource(requesterID, profileOwnerID, resourceID, result);
  }

  @Override
  public int postCommentOnResource(int commentCreatorID,
      int resourceCreatorID, int resourceID,
      HashMap<String, ByteIterator> values) {
    return super.postCommentOnResource(commentCreatorID, resourceCreatorID,
        resourceID, values);
  }

  @Override
  public int delCommentOnResource(int resourceCreatorID, int resourceID,
      int manipulationID) {
    return super.delCommentOnResource(resourceCreatorID, 
        resourceID, manipulationID);
  }

  @Override
  public HashMap<String, String> getInitialStats() {
    return super.getInitialStats();
  }

  @Override
  public void cleanup(boolean warmup) throws DBException {
    super.cleanup(warmup);
  }

  @Override
  public int CreateFriendship(int friendid1, int friendid2) {
    return super.CreateFriendship(friendid1, friendid2);
  }

  @Override
  public void createSchema(Properties props) {
    super.createSchema(props);
  }

  @Override
  public int queryPendingFriendshipIds(int memberID,
      Vector<Integer> pendingIds) {
    return super.queryPendingFriendshipIds(memberID, pendingIds);
  }

  @Override
  public int queryConfirmedFriendshipIds(int memberID,
      Vector<Integer> confirmedIds) {
    return super.queryConfirmedFriendshipIds(memberID, confirmedIds);
  }
}