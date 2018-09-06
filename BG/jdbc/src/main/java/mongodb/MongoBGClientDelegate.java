package mongodb;

import static edu.usc.bg.workloads.CoreWorkload.DATABASE_FAILURE;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import edu.usc.bg.base.ByteIterator;

public class MongoBGClientDelegate extends MongoBGClient {

    private final AtomicBoolean isDatabaseFailed;
    private final Logger logger = Logger.getLogger(MongoBGClientDelegate.class);
    public static final boolean DO_ACTUAL_KILL_DB = false; 
    
    private final int MAX_RETRY = 100;
    
    public MongoBGClientDelegate() {
        super();
        this.isDatabaseFailed = new AtomicBoolean(false);
    }

    public MongoBGClientDelegate(String ipAddress, AtomicBoolean isDatabaseFailed) {
        super(ipAddress);
        this.isDatabaseFailed = isDatabaseFailed;
    }

    @Override
    public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
            boolean insertImage, boolean testMode) {
        if (isDatabaseFailed.get()) {
            return DATABASE_FAILURE;
        }
        
        for (int i = 0; i < MAX_RETRY; i++) {
            try {
                return super.viewProfile(requesterID, profileOwnerID, result, insertImage, testMode);
            } catch (Exception e) {
                if (DO_ACTUAL_KILL_DB) {
                    isDatabaseFailed.set(true);
                    break;
                }
                logger.error("View profile failed for u" + profileOwnerID + "u", e);
            }
        }
        
        return DATABASE_FAILURE;
    }

    @Override
    public List<String> listFriends(int profileOwnerID) {
        if (isDatabaseFailed.get()) {
            return null;
        }
        
        for (int i = 0; i < MAX_RETRY; i++) {
            try {
                return super.listFriends(profileOwnerID);
            } catch (Exception e) {
                if (DO_ACTUAL_KILL_DB) {
                    isDatabaseFailed.set(true);
                    break;
                }
                logger.error("List friends failed for u" + profileOwnerID + "u", e);
            }
        }
        
        return null;
    }

    @Override
    public List<String> listPendingFriends(int profileOwnerID) {
        if (isDatabaseFailed.get()) {
            return null;
        }
        
        for (int i = 0; i < MAX_RETRY; i++) {
            try {
                return super.listPendingFriends(profileOwnerID);
            } catch (Exception e) {
                if (DO_ACTUAL_KILL_DB) {
                    isDatabaseFailed.set(true);
                    break;
                }
                logger.error("list pending friends failed for u" + profileOwnerID + "u", e);
            }
        }
        
        return null;
    }

    @Override
    public int acceptFriendInviter(int inviterID, int inviteeID) {
        if (isDatabaseFailed.get()) {
            return DATABASE_FAILURE;
        }
        
        for (int i = 0; i < MAX_RETRY; i++) {
            try {
                return super.acceptFriendInviter(inviterID, inviteeID);
            } catch (Exception e) {
                if (DO_ACTUAL_KILL_DB) {
                    isDatabaseFailed.set(true);
                    break;
                }
                logger.error("Accept friend inviter failed for inviter u" + inviterID + "u invitee u" + inviteeID + "u", e);
            }
        }
        
        return DATABASE_FAILURE;
    }

    @Override
    public int updateUserDocument(String userId, Set<String> friends, Set<String> pendingFriends,
            Set<String> addFriends, Set<String> removeFriends, Set<String> addPendingFriends,
            Set<String> removePendingFriends) {
        if (isDatabaseFailed.get()) {
            return DATABASE_FAILURE;
        }
        try {
            return super.updateUserDocument(userId, friends, pendingFriends, addFriends, removeFriends,
                    addPendingFriends, removePendingFriends);
        } catch (Exception e) {
            if (DO_ACTUAL_KILL_DB) {
                isDatabaseFailed.set(true);
            }
            logger.error("Recover user document failed for u" + userId + "u", e);
        }
        return DATABASE_FAILURE;
    }

    @Override
    public int acceptFriendInvitee(int inviterID, int inviteeID) {
        if (isDatabaseFailed.get()) {
            return DATABASE_FAILURE;
        }
        
        for (int i = 0; i < MAX_RETRY; i++) {
            try {
                return super.acceptFriendInvitee(inviterID, inviteeID);
            } catch (Exception e) {
                if (DO_ACTUAL_KILL_DB) {
                    isDatabaseFailed.set(true);
                    break;
                }
                logger.error("accept friend invitee failed for inviter u" + inviterID + "u invitee u" + inviteeID + "u", e);
            }
        }
        
        return DATABASE_FAILURE;
    }

    @Override
    public int thawFriendInviter(int friendid1, int friendid2) {
        if (isDatabaseFailed.get()) {
            return DATABASE_FAILURE;
        }
        
        for (int i = 0; i < MAX_RETRY; i++) {
            try {
                return super.thawFriendInviter(friendid1, friendid2);
            } catch (Exception e) {
                if (DO_ACTUAL_KILL_DB) {
                    isDatabaseFailed.set(true);
                    break;
                }
                logger.error("thaw friend inviter failed for u" + friendid1 + "u u" + friendid2 + "u", e);
            }
        }
        
        return DATABASE_FAILURE;
    }

    @Override
    public int thawFriendInvitee(int friendid1, int friendid2) {
        if (isDatabaseFailed.get()) {
            return DATABASE_FAILURE;
        }
        
        for (int i = 0; i < MAX_RETRY; i++) {
            try {
                return super.thawFriendInvitee(friendid1, friendid2);
            } catch (Exception e) {
                if (DO_ACTUAL_KILL_DB) {
                    isDatabaseFailed.set(true);
                    break;
                }
                logger.error("thaw friend invitee failed for u" + friendid1 + "u u" + friendid2 + "u", e);
            }
        }
        
        return DATABASE_FAILURE;
    }

    @Override
    public int listFriends(int requesterID, int profileOwnerID, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
        if (isDatabaseFailed.get()) {
            return DATABASE_FAILURE;
        }
        try {
            return super.listFriends(requesterID, profileOwnerID, fields, result, insertImage, testMode);
        } catch (Exception e) {
            if (DO_ACTUAL_KILL_DB) {
                isDatabaseFailed.set(true);
            }
            logger.error("list friends failed for u" + profileOwnerID + "u", e);
        }
        return DATABASE_FAILURE;
    }

    @Override
    public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
            boolean testMode) {
        if (isDatabaseFailed.get()) {
            return DATABASE_FAILURE;
        }
        
        for (int i = 0; i < MAX_RETRY; i++) {
            try {
                return super.viewFriendReq(profileOwnerID, results, insertImage, testMode);
            } catch (Exception e) {
                if (DO_ACTUAL_KILL_DB) {
                    isDatabaseFailed.set(true);
                    break;
                }
                logger.error("view friend requests failed for u" + profileOwnerID + "u", e);
            }
        }
        
        return DATABASE_FAILURE;
    }

    @Override
    public int acceptFriend(int inviterID, int inviteeID) {
        if (isDatabaseFailed.get()) {
            return DATABASE_FAILURE;
        }
        
        for (int i = 0; i < MAX_RETRY; i++) {
            try {
                return super.acceptFriend(inviterID, inviteeID);
            } catch (Exception e) {
                if (DO_ACTUAL_KILL_DB) {
                    isDatabaseFailed.set(true);
                    break;
                }
                logger.error("accept friends failed for inviter u" + inviterID + "u invitee u" + inviteeID + "u", e);
            }
        }
        
        return DATABASE_FAILURE;
    }

    @Override
    public int rejectFriend(int inviterID, int inviteeID) {
        if (isDatabaseFailed.get()) {
            return DATABASE_FAILURE;
        }
        
        for (int i = 0; i < MAX_RETRY; i++) {
            try {
                return super.rejectFriend(inviterID, inviteeID);
            } catch (Exception e) {
                if (DO_ACTUAL_KILL_DB) {
                    isDatabaseFailed.set(true);
                }
                logger.error("reject friends failed for inviter u" + inviterID + "u invitee u" + inviteeID + "u", e);
            }
        }
        
        return DATABASE_FAILURE;
    }

    @Override
    public int inviteFriend(int inviterID, int inviteeID) {
        if (isDatabaseFailed.get()) {
            return DATABASE_FAILURE;
        }
        
        for (int i = 0; i < MAX_RETRY; i++) {
            try {
                return super.inviteFriend(inviterID, inviteeID);
            } catch (Exception e) {
                if (DO_ACTUAL_KILL_DB) {
                    isDatabaseFailed.set(true);                    
                    break;
                }
                logger.error("invite friends failed for inviter u" + inviterID + "u invitee u" + inviteeID + "u", e);
            }
        }
        
        return DATABASE_FAILURE;
    }

    @Override
    public int thawFriendship(int friendid1, int friendid2) {
        if (isDatabaseFailed.get()) {
            return DATABASE_FAILURE;
        }
        
        for (int i = 0; i < MAX_RETRY; i++) {
            try {
                return super.thawFriendInvitee(friendid1, friendid2);
            } catch (Exception e) {
                if (DO_ACTUAL_KILL_DB) {
                    isDatabaseFailed.set(true);
                    break;
                }
                logger.error("thaw friends failed for inviter u" + friendid1 + "u invitee u" + friendid2 + "u", e);
            }
        }
        
        return DATABASE_FAILURE;
    }

    @Override
    public int CreateFriendship(int friendid1, int friendid2) {
        return super.CreateFriendship(friendid1, friendid2);
    }
}
