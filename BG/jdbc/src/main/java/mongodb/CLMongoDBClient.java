package mongodb;


import static mongodb.RecoveryCaller.WRITE;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.meetup.memcached.CLValue;
import com.meetup.memcached.IQException;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DBException;

import tardis.TardisClientConfig;


public class CLMongoDBClient extends MemMongoClient {
	public CLMongoDBClient() {
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
		return super.viewFriendReq(profileOwnerID, result, 
				insertImage, testMode);
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {	
		String kjpcount = TardisClientConfig.KEY_PENDING_FRIEND_COUNT + inviteeID;
		String kjplist = TardisClientConfig.KEY_LIST_FRIENDS_REQUEST + inviteeID;
		CLValue v1 = null, v = null;

		boolean addToEW = false;
		while (true) {
			addToEW = false;
			String tid = mc.generateSID();

			try {
				v1 = mc.ewincr(tid, kjpcount, getHashCode(inviteeID));
				v = mc.ewappend(kjplist, getHashCode(inviteeID), inviterID+",", tid);

				if (v1 == null || v == null) return -1;
				Boolean recover = null;
				if (!WRITE_RECOVER) recover = false;

				if (v1.isPending() || v.isPending() || 
						(WRITE_RECOVER == false  && dirtyDocs.get() == 0)) {
					if (!writeBack && WRITE_RECOVER && !isDatabaseFailed.get()) {
						Set<String> deltas = new HashSet<>();

						if ((boolean)v.getValue() == false) {		
							String delta = TardisClientConfig.ACTION_PW_PENDING_FRIENDS + "+" + inviterID;
							deltas.add(delta);
						}

						try {
							CLActiveRecoveryWorker.docRecover(mc, tid, 
									inviteeID, client, 
									deltas, WRITE, ACT_INV_FRIEND);
							numUpdatesOnWrites.incrementAndGet();
							recover = true;
						} catch (DatabaseFailureException e) {
							recover = false;
						}
					} else {
						recover = false;
					}
				}

				if (recover == null) {	// no recover happens, update MongoDB
					if (!writeBack && client.inviteFriend(inviterID, inviteeID) == SUCCESS) {
						recover = true;
					} else {
						recover = false;
					}
				}

				if (!recover) {
					String delta = TardisClientConfig.ACTION_PW_PENDING_FRIENDS + "+" + inviterID;
					addToEW = addDeltaToPW(tid, inviteeID, delta);
				}
				mc.ewcommit(tid, getHashCode(inviteeID), !recover);				
				break;
			} catch (IQException e1) { 
				try {
					mc.release(tid, getHashCode(inviteeID));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			numSessRetriesInWrites.incrementAndGet();
			sleepRetryWrite();
		}

		if (addToEW)
			addUserToEW(inviteeID);

		return SUCCESS;
	}

	private int hashKey(int inviteeID) {
		return inviteeID % TardisClientConfig.NUM_EVENTUAL_WRITE_LOGS;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {		
		String kjpcount = TardisClientConfig.KEY_PENDING_FRIEND_COUNT + inviteeID;
		String kjplist = TardisClientConfig.KEY_LIST_FRIENDS_REQUEST + inviteeID;
		CLValue v = null, pcount = null;

		boolean addToEW = false;
		while (true) {
			String tid = mc.generateSID();
			addToEW = false;

			try {
				pcount = mc.ewdecr(tid, kjpcount, getHashCode(inviteeID));	
				v = mc.ewread(tid, kjplist, getHashCode(inviteeID), false);				

				if (pcount == null || v == null) return -1;

				Boolean recover = null;
				if (!WRITE_RECOVER) recover = false;
				
				if (pcount.isPending() || v.isPending() ||
						(WRITE_RECOVER == false  && dirtyDocs.get() == 0)) {
					if (!writeBack && WRITE_RECOVER && !isDatabaseFailed.get()) {
						Set<String> changes = new HashSet<>();
						changes.add(TardisClientConfig.ACTION_PW_PENDING_FRIENDS + "-" + inviterID);
						try {
							CLActiveRecoveryWorker.docRecover(mc, tid, 
									inviteeID, client, 
									changes, WRITE, ACT_REJ_FRIEND);
							numUpdatesOnWrites.incrementAndGet();
							recover = true;
						} catch (DatabaseFailureException e) {
							recover = false;
						}
					} else {
						recover = false;
					}
				}

				if (recover == null) {	// no recover happens, update MongoDB
					if (!writeBack && client.rejectFriend(inviterID, inviteeID) == SUCCESS) {
						recover = true;
					} else {
						recover = false;
					}
				} 

				// update r-m-w key-value pairs
				Object val = v.getValue();
				Set<String> userSet = MemcachedSetHelper.convertSet(val);
				if (userSet != null) {
					userSet.remove(String.valueOf(inviterID));
					String users = MemcachedSetHelper.convertString(userSet);
					mc.ewswap(tid, kjplist, getHashCode(inviteeID), users);					
				}		

				if (!recover) {
					String delta = TardisClientConfig.ACTION_PW_PENDING_FRIENDS + "-" + inviterID;
					addToEW = addDeltaToPW(tid, inviteeID, delta);
				}

				mc.ewcommit(tid, getHashCode(inviteeID), !recover);
				break;
			} catch (IQException e) { 
				try {
					mc.release(tid, getHashCode(inviteeID));
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			numSessRetriesInWrites.incrementAndGet();
			sleepRetryWrite();
		}

		if (addToEW)
			addUserToEW(inviteeID);

		return SUCCESS;
	}

	private boolean addDeltaToPW(String tid, int inviteeID, String delta) throws IQException {
		CLValue val = null;
		String str = delta+TardisClientConfig.DELIMITER;
		String pw = TardisClientConfig.KEY_USER_PENDING_WRITES_LOG + inviteeID;
		val = mc.ewappend(pw, getHashCode(inviteeID), str, tid);
		boolean addToEW = false;
		if ((boolean)val.getValue() == false) {
			addToEW = true;
			str = rand.nextInt() + ":" + TardisClientConfig.DELIMITER+delta+TardisClientConfig.DELIMITER;
			mc.ewswap(tid, pw, getHashCode(inviteeID), str);			
		}

		TimedMetrics.getInstance().add(MetricsName.METRICS_DBFAIL_MEM_OVERHEAD, str.getBytes().length);
		return addToEW;
	}
	
	private void addDeltaToPW_RMW(String tid, int userid, char action, int data) throws IQException {
		String pw = TardisClientConfig.KEY_USER_PENDING_WRITES_LOG + userid;
		CLValue val = mc.ewread(tid, pw, userid, true);
		if (val == null) return;
		String str = (String)val.getValue();
		Set<String> ids = MemcachedSetHelper.convertSet(str); 
		switch (action) {
		case TardisClientConfig.ACTION_PW_FRIENDS:
			ids.add(String.valueOf(data));
			break;
		case TardisClientConfig.ACTION_PW_PENDING_FRIENDS:
			ids.remove(String.valueOf(data));
			break;
		}
		str = MemcachedSetHelper.convertString(ids);
		
	}

	private void addUserToEW(int inviteeID) {
		String ewKey = TardisClientConfig.getEWLogKey(hashKey(inviteeID));

		while (true) {
			String tid = mc.generateSID();
			try {
				CLValue val = mc.ewappend(ewKey, getHashCode(ewKey), String.valueOf(inviteeID)+TardisClientConfig.DELIMITER, tid);
				if ((boolean)val.getValue() == false)
					mc.ewswap(tid, ewKey, getHashCode(ewKey), 
							TardisClientConfig.DELIMITER + String.valueOf(inviteeID) + TardisClientConfig.DELIMITER);
				mc.ewcommit(tid, getHashCode(ewKey), false);
				break;
			} catch (IQException e) { 
				try {
					mc.release(tid, getHashCode(ewKey));
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			sleepRetryWrite();
		}

		TimedMetrics.getInstance().add(MetricsName.METRICS_DBFAIL_MEM_OVERHEAD, ewKey.getBytes().length + (String.valueOf(inviteeID)+TardisClientConfig.DELIMITER).getBytes().length);
		TimedMetrics.getInstance().add(MetricsName.METRICS_DIRTY_USER);
	}

	public boolean acceptFriendInviter(int inviterID, int inviteeID) {
		String kcficount = TardisClientConfig.KEY_FRIEND_COUNT + inviterID;
		String kcfilist = TardisClientConfig.KEY_LIST_FRIENDS + inviterID;
		CLValue val1, val2 = null;

		boolean addToEW = false;
		Boolean recover = null;
		while (true) {
			String tid = mc.generateSID();

			try {
				val1 = mc.ewincr(tid, kcficount, getHashCode(inviterID));	
				val2 = mc.ewappend(kcfilist, getHashCode(inviterID), inviteeID+",", tid);

				if (val1 == null || val2 == null) {
					System.out.println(
							String.format("AcceptFriendInviter: Got null value %d %d", inviterID, inviteeID));
					System.exit(-1);
				}
				
				recover = null;
				if (!WRITE_RECOVER) recover = false;

				if (val1.isPending() || val2.isPending() ||
						(WRITE_RECOVER == false  && dirtyDocs.get() == 0)) {
					if (!writeBack && WRITE_RECOVER && !isDatabaseFailed.get()) {
						Set<String> deltas = new HashSet<>();

						if ((boolean)val2.getValue() == false) {			
							deltas.add(TardisClientConfig.ACTION_PW_FRIENDS + "+" + inviteeID);
						}

						try {
							CLActiveRecoveryWorker.docRecover(mc, tid, 
									inviterID, client, 
									deltas, WRITE, ACT_ACP_FRIEND_INVITER);
							numUpdatesOnWrites.incrementAndGet();
							recover = true;
						} catch (DatabaseFailureException e) {							
							recover = false;
						}
					} else {
						recover = false;
					}
				}

				if (recover == null) {
					// update here			
					if (!writeBack && client.acceptFriendInviter(inviterID, inviteeID) == SUCCESS) {
						recover = true;
					} else {
						recover = false;
					}
				} 

				if (!recover) {					
					String delta = TardisClientConfig.ACTION_PW_FRIENDS + "+" + inviteeID;
					addToEW = addDeltaToPW(tid, inviterID, delta);
				}

				mc.ewcommit(tid, getHashCode(inviterID), !recover);
				break;
			} catch (IQException e) { 
				try {
					mc.release(tid, getHashCode(inviterID));
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			numSessRetriesInWrites.incrementAndGet();
			sleepRetryWrite();
		}

		if (addToEW)
			addUserToEW(inviterID);

		return !(boolean)recover;
	}

	public boolean acceptFriendInvitee(int inviterID, int inviteeID) {
		String kcfjcount = TardisClientConfig.KEY_FRIEND_COUNT + inviteeID;
		String kpfjcount = TardisClientConfig.KEY_PENDING_FRIEND_COUNT + inviteeID;
		String kcfjlist = TardisClientConfig.KEY_LIST_FRIENDS + inviteeID;
		String kpfjlist = TardisClientConfig.KEY_LIST_FRIENDS_REQUEST + inviteeID;
		CLValue val1 = null, val2 = null;
		CLValue v1 = null, v2 = null;

		boolean addToEW = false;
		Boolean recover = null;
		while (true) {
			String tid = mc.generateSID();
			addToEW = false;

			try {
				v1 = mc.ewdecr(tid, kpfjcount, getHashCode(inviteeID));
				v2 = mc.ewincr(tid, kcfjcount, getHashCode(inviteeID));
				val1 = mc.ewread(tid, kpfjlist, getHashCode(inviteeID), false);
				val2 = mc.ewappend(kcfjlist, getHashCode(inviteeID), inviterID+",", tid);

				if (v1 == null || v2 == null || val1 == null || val2 == null) {
					System.out.println(
							String.format("AcceptFriendInvitee: Got null value %d %d", inviterID, inviteeID));
					System.exit(-1);
				}
				
				recover = null;
				if (!WRITE_RECOVER) recover = false;

				if (v1.isPending() || v2.isPending() || 
						val1.isPending() || val2.isPending() ||
						(WRITE_RECOVER == false  && dirtyDocs.get() == 0)) {
					if (!writeBack && WRITE_RECOVER && !isDatabaseFailed.get()) {
						Set<String> changes = new HashSet<>();
						changes.add(TardisClientConfig.ACTION_PW_PENDING_FRIENDS + "-" + inviterID);
						if ((boolean)val2.getValue() == false)
							changes.add(TardisClientConfig.ACTION_PW_FRIENDS + "+" + inviterID);

						try {						
							CLActiveRecoveryWorker.docRecover(mc, tid, 
									inviteeID, client, 
									changes, WRITE, ACT_ACP_FRIEND_INVITEE);
							numUpdatesOnWrites.incrementAndGet();
							recover = true;
						} catch (DatabaseFailureException e) {
							recover = false;
						}
					} else {
						recover = false;
					}
				}

				if (recover == null) {
					if (!writeBack && client.acceptFriendInvitee(inviterID, inviteeID) == SUCCESS) {
						recover = true;						
					} else {
						recover = false;
					}			
				}

				// update pending list
				Object obj = val1.getValue();
				Set<String> userSet = MemcachedSetHelper.convertSet(obj);
				if (userSet != null) {
					userSet.remove(String.valueOf(inviterID));
					String users = MemcachedSetHelper.convertString(userSet);
					mc.ewswap(tid, kpfjlist, getHashCode(inviteeID), users);
				}

				if (!recover) {
					String delta;
					delta = TardisClientConfig.ACTION_PW_PENDING_FRIENDS + "-" + inviterID;
					addToEW = addDeltaToPW(tid, inviteeID, delta);
					delta = TardisClientConfig.ACTION_PW_FRIENDS + "+" + inviterID;
					addToEW = addToEW | addDeltaToPW(tid, inviteeID, delta);
				}

				mc.ewcommit(tid, getHashCode(inviteeID), !recover);
				break;
			} catch (IQException e) { 
				try {
					mc.release(tid, getHashCode(inviteeID));
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			numSessRetriesInWrites.incrementAndGet();
			sleepRetryWrite();
		}

		if (addToEW)
			addUserToEW(inviteeID);

		return (boolean) !recover;
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
		CLValue v = null, val = null;

		boolean addToEW = false;
		Boolean recover = null;
		while (true) {
			String tid = mc.generateSID();
			addToEW = false;

			try {
				v = mc.ewdecr(tid, kcf1count, getHashCode(friendid1));
				val = mc.ewread(tid, kcf1list, getHashCode(friendid1), false);				

				if (v == null || val == null) {
					System.out.println(
							String.format("ThawFriend: Got null value %d %d", friendid1, friendid2));
					System.exit(-1);
				}
				
				recover = null;
				if (!WRITE_RECOVER) recover = false;

				if (v.isPending() || val.isPending() ||
						(WRITE_RECOVER == false  && dirtyDocs.get() == 0)) {
					if (!writeBack && WRITE_RECOVER && !isDatabaseFailed.get()) {
						Set<String> changes = new HashSet<>();
						changes.add(TardisClientConfig.ACTION_PW_FRIENDS + "-" + friendid2);
						try {
							CLActiveRecoveryWorker.docRecover(mc, tid, 
									friendid1, client, 
									changes, WRITE, ACT_THW_FRIEND);
							numUpdatesOnWrites.incrementAndGet();
							recover = true;
						} catch (DatabaseFailureException e) {
							recover = false;
						}
					} else {
						recover = false;
					}
				}

				if (recover == null) {
					if (!writeBack && client.thawFriendInviter(friendid1, friendid2) == SUCCESS) {		
						recover = true;
					} else {
						recover = false;
					}
				}

				Object obj = val.getValue();
				Set<String> userSet = MemcachedSetHelper.convertSet(obj);
				if (userSet != null) {
					userSet.remove(friendid2 + "");
					mc.ewswap(tid, kcf1list, getHashCode(friendid1), MemcachedSetHelper.convertString(userSet));					
				}

				if (!recover) {
					String delta = TardisClientConfig.ACTION_PW_FRIENDS + "-" + friendid2;
					addToEW = addDeltaToPW(tid, friendid1, delta);
				}

				mc.ewcommit(tid, getHashCode(friendid1), !recover);
				break;
			} catch (IQException e) { 
				try {
					mc.release(tid, getHashCode(friendid1));
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			numSessRetriesInWrites.incrementAndGet();
			sleepRetryWrite();
		}

		if (addToEW)
			addUserToEW(friendid1);

		return (boolean) !recover;
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