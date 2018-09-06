package mongodb;

import static mongodb.RecoveryCaller.AR;
import static tardis.TardisClientConfig.ACTION_PW_FRIENDS;
import static tardis.TardisClientConfig.ACTION_PW_PENDING_FRIENDS;
import static tardis.TardisClientConfig.NUM_EVENTUAL_WRITE_LOGS;
import static tardis.TardisClientConfig.RECOVERY_WORKER_BASE_TIME_BETWEEN_CHECKING_EW;
import static tardis.TardisClientConfig.getEWLogKey;
import static mongodb.MemMongoClient.getHashCode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.meetup.memcached.CLValue;
import com.meetup.memcached.IQException;
import com.meetup.memcached.MemcachedClient;

import edu.usc.bg.base.DBException;
import edu.usc.bg.workloads.CoreWorkload;
import tardis.TardisClientConfig;

public class CLActiveRecoveryWorker extends Thread {
	MemcachedClient mc;
	MongoBGClientDelegate mongoClient;
	static volatile boolean isRunning = true;
	private static AtomicInteger id = new AtomicInteger(0);
	int workerId;

	private final static Logger logger = Logger.getLogger(CLActiveRecoveryWorker.class);

	static Random rand = new Random();

	private int ALPHA = 5;
	long sleepTime = RECOVERY_WORKER_BASE_TIME_BETWEEN_CHECKING_EW;
	static final long checkSleepTime = 1000;
	private MemcachedClient[] mcs;

	public CLActiveRecoveryWorker(MongoBGClientDelegate client, int alpha, String[] serverlist) {
		ALPHA = alpha;
		mcs = new MemcachedClient[ALPHA];
		for (int i = 0; i < mcs.length; i++) {
			mcs[i] = new MemcachedClient("BG");
		}

		mc = new MemcachedClient("BG");//MemcachedMongoBGClient.getMemcachedClient(serverlist, "BG");
		mongoClient = client;
		workerId = id.incrementAndGet();
		try {
			mongoClient.init();
		} catch (DBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void  run() {
		System.out.println("Start AR worker");

		String[] EWs = new String[NUM_EVENTUAL_WRITE_LOGS];
		for (int i = 0; i < EWs.length; i++) {
			EWs[i] = getEWLogKey(i);
		}

		while (isRunning) {
			int idx = rand.nextInt(EWs.length);
			int start = idx;

			do {
				// get random EW
				Object ew = mc.get(EWs[idx], getHashCode(idx), true);
				Set<String> idset = MemcachedSetHelper.convertSet(ew);

				if (idset == null || idset.size() == 0) {
					idx = getNextIdx(idx);				
					continue;
				}

				// pick random alpha ids
				idset = (Set<String>) pickRandom(idset, ALPHA);

				Map<String, String> recoverMap = new HashMap<>();
				Map<String, MemcachedClient> mcMap = new HashMap<>();
				for (String id: idset) {
					MemcachedClient mcUid = mc;

					while (true) {
						String tid = mcUid.generateSID();
						try {

							docRecover(mcUid, tid, Integer.parseInt(id), mongoClient, null, 
									AR, CLMongoDBClient.ACT_AR);

							mcUid.ewcommit(tid, getHashCode(Integer.parseInt(id)), false);
							CLMongoDBClient.numUpdatesOnARs.incrementAndGet();
							recoverMap.put(id, tid);
							mcMap.put(tid, mcUid);
							resetTime();
							break;
						} catch (DatabaseFailureException e ) {
							try {
								mcUid.ewcommit(tid, getHashCode(Integer.parseInt(id)), true);
							} catch (Exception e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
							break;
						} catch (IQException e) {
							try {
								mcUid.release(tid, getHashCode(Integer.parseInt(id)));
							} catch (Exception e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}							
						}

						CLMongoDBClient.numSessRetriesInARs.incrementAndGet();

						// at this point, fail to recover
						// sleep exponentially and retry on the same doc
						sleepTime *= 2;
						if (sleepTime > checkSleepTime) {
							sleepTime = checkSleepTime;
						}
						sleepFor(sleepTime);
					}
				}

				// update EW
				if (recoverMap.size() > 0) {
					int numLeft = updateEW(EWs[idx], recoverMap, mcMap);
				}

				idx = getNextIdx(idx);
			} while (idx != start && isRunning);
		}

		System.out.println("AR Worker stopped.");
	}

	private void resetTime() {
		sleepTime = RECOVERY_WORKER_BASE_TIME_BETWEEN_CHECKING_EW;
	}

	public void sleepFor(long time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private int getNextIdx(int idx) {
		idx++;
		return idx < NUM_EVENTUAL_WRITE_LOGS ? idx : 0;
	}

	public static Set<?> pickRandom(Set<? extends Object> idset, int alpha) {
		if (alpha >= idset.size())
			return idset;

		BitSet bs = new BitSet(idset.size());
		int cardinality = 0;
		while(cardinality < alpha) {
			int v = rand.nextInt(alpha);
			if(!bs.get(v)) {
				bs.set(v);
				cardinality++;
			}
		}

		Set<Object> chosen = new HashSet<>();
		int i = 0;
		for (Object id: idset) {
			if (bs.get(i++))
				chosen.add(id);
		}

		return chosen;
	}

	private int updateEW(String ewKey, Map<String, String> recoverMap, 
			Map<String, MemcachedClient> mcMap) {
		CLValue val = null;
		List<String> idlist = null;
		while (true) {
			String tid = mc.generateSID();

			try {
				val = mc.ewread(tid, ewKey, getHashCode(ewKey), true);

				if (val == null)
					System.out.println("Something went wrong...ewKey="+ewKey);
				else {
					Object obj = val.getValue();
					if (obj != null) {
						if (obj != null) {
							idlist = MemcachedSetHelper.convertList(obj);
							for (String key: recoverMap.keySet())
								idlist.remove(key);
							String newVal = MemcachedSetHelper.convertString(idlist);
							mc.ewswap(tid, ewKey, getHashCode(ewKey), newVal);
						}
					}
				}
				mc.ewcommit(tid, getHashCode(ewKey), false);
				break;
			} catch (IQException e1) {
		        try {
		          mc.release(tid, getHashCode(ewKey));
		        } catch (Exception e) {
		          // TODO Auto-generated catch block
		          e.printStackTrace();
		        }
			}

			try {
				Thread.sleep(CLMongoDBClient.Q_LEASE_BACKOFF_TIME);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return idlist == null ? 0 : idlist.size();
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

	private static int snapshotRecover(MemcachedClient mc, String tid, int userId, 
			MongoBGClientDelegate mongoClient, Set<String> changes, 
			RecoveryCaller caller, int action) throws IQException, DatabaseFailureException {

		String keyPendingCount = TardisClientConfig.KEY_PENDING_FRIEND_COUNT + userId;
		String keyConfirmedCount = TardisClientConfig.KEY_FRIEND_COUNT + userId;
		String keyPendingFriend = TardisClientConfig.KEY_LIST_FRIENDS_REQUEST + userId;
		String keyConfirmedFriend = TardisClientConfig.KEY_LIST_FRIENDS + userId;

		CLValue val1 = null, val2 = null, v1= null, v2 = null;

		v1 = mc.ewread(tid, keyPendingCount, userId, true);
		v2 = mc.ewread(tid, keyConfirmedCount, userId, true);
		val1 = mc.ewread(tid, keyPendingFriend, userId, false);
		val2 = mc.ewread(tid, keyConfirmedFriend, userId, false);

		if (v1 == null || v2 == null || 
				val1 == null || val2 == null) return 0;

		if (!v1.isPending() && !v2.isPending() && 
				!val1.isPending() && !val2.isPending()) {
			return 0;
		}

		List<String> pendingWrites = new ArrayList<>();

		String snapshotVal, pwVal = null;
		String pw = TardisClientConfig.getUserLogKey(userId);
		String snapshotKey = TardisClientConfig.getSnapshotKey(userId);

		// get snapshot
		snapshotVal = (String) mc.get(snapshotKey, userId, true);
		if (snapshotVal != null) {
			System.out.println("Snapshot is not null");
			pwVal = (String) mc.get(pw, userId, true);
			if (pwVal == null || 
					(getBWId(snapshotVal) != getBWId(pwVal))
					) {
				// get current state
				String currState = mongoClient.getCurrentState(userId);
				if (!checkIdentical(currState, snapshotVal)) {
					int updateRet = doIdempotentRecover(mongoClient, snapshotVal, userId, caller);
					if (updateRet == CoreWorkload.DATABASE_FAILURE) {
						throw new DatabaseFailureException();
					}
				}
			}

			// delete the snapshot
			try {
				mc.delete(snapshotKey, userId, null);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (pwVal == null) {
			pwVal = (String) mc.get(pw, userId, true);
		}

		String uid = null;
		int updateRet = 0;

		//    Set<String> pendingFriends = MemcachedSetHelper.convertSet(val1.getValue());
		//    Set<String> friends = MemcachedSetHelper.convertSet(val2.getValue());
		Set<String> pendingFriends = null;
		Set<String> friends = null;

		if (pwVal != null) {  // save a snapshot
			uid = pwVal.substring(0, pwVal.indexOf(":")+1);
			pendingWrites.addAll(MemcachedSetHelper.convertList(pwVal));

			// snapshot does not include changes of this action
			String currState = mongoClient.getCurrentState(userId);
			snapshotVal = prepareSnapshot(currState, pendingWrites, 
					friends, pendingFriends, uid);

			try {
				mc.set(snapshotKey, snapshotVal, userId);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				mc.delete(pw, userId, null);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (changes != null) {
				pendingWrites.addAll(changes);
			}

			//      updateRet = doNONIdempotentRecover(mongoClient, userId, pendingWrites, 
			//          friends, pendingFriends);
			snapshotVal = prepareSnapshot(currState, pendingWrites, 
					friends, pendingFriends, uid);
			updateRet = doIdempotentRecover(mongoClient, snapshotVal, userId, caller);
		} else {
			if (changes != null) {
				pendingWrites.addAll(changes);
			}
			updateRet = doNONIdempotentRecover(mongoClient, userId, pendingWrites, 
					friends, pendingFriends);
		}

		if (updateRet == MemMongoClient.SUCCESS) {
			if (snapshotVal != null)
				try {
					mc.delete(snapshotKey, userId, null);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			switch (caller) {
			case AR:
				TimedMetrics.getInstance().add(MetricsName.METRICS_NUMBER_RECOVERED_BY_AR_WORKER);
				break;
			case READ:
			case WRITE:
				TimedMetrics.getInstance().add(MetricsName.METRICS_NUMBER_RECOVERED_BY_APP);
				break;
			default:
				break;
			}
		} else {      
			throw new DatabaseFailureException();
		}

		return 0;
	}

	private static int doIdempotentRecover2(MongoBGClient mongoClient, 
			List<String> pendingWrites,
			int userId, RecoveryCaller caller, 
			Set<String> friends, Set<String> pendingFriends) {    
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

		return mongoClient.updateUserDocument(String.valueOf(userId), friends, pendingFriends, 
				null, null, null, null);
	}

	private static int doIdempotentRecover(MongoBGClientDelegate mongoClient, String state, 
			int userId, RecoveryCaller caller) {
		//    System.out.println("Reach this");
		String s = state.substring(state.indexOf(":")+1);
		Set<String> friends = null, pendingFriends = null;
		String fs = s.substring(0, s.indexOf(';'));    
		if (!fs.equals(""))
			friends = new HashSet<>(Arrays.asList(fs.split(",")));
		String ps = s.substring(s.indexOf(';')+1);
		if (!ps.equals(""))
			pendingFriends = new HashSet<>(Arrays.asList(ps.split(",")));

		logger.debug("caller" + caller + "cached friends for user u" + userId + "u " + friends);
		logger.debug("caller" + caller + "cached pending friends for user u" + userId + "u " + pendingFriends);

		int updateRet = mongoClient.updateUserDocument(String.valueOf(userId), friends, pendingFriends, null,
				null, null, null);

		return updateRet;
	}

	private static String prepareSnapshot(String currState, 
			List<String> pendingWrites, 
			Set<String> friends, Set<String> pendingFriends, String uid) {
		if (friends == null) {
			String fs = currState.substring(0, currState.indexOf(';'));
			friends = new HashSet<>(Arrays.asList(fs.split(",")));
		}

		if (pendingFriends == null) {
			String ps = currState.substring(currState.indexOf(';')+1);
			pendingFriends = new HashSet<>(Arrays.asList(ps.split(",")));
		}

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
		for (String f: removeFriends) {
			//    removeFriends.forEach(f -> {
			friends.remove(f);
			//    });
		}

		for (String p: removePendingFriends) {
			//    removePendingFriends.forEach(p -> {
			pendingFriends.remove(p);
			//    });
		}

		StringBuilder sb = new StringBuilder();

		for (String f: friends)
			if (!f.equals(""))
				sb.append(f).append(",");

		if (sb.length() > 0)
			sb.setCharAt(sb.length()-1, ';');
		else
			sb.append(';');

		for (String p: pendingFriends)
			if (!p.equals(""))
				sb.append(p).append(",");

		if (sb.length() > 0 && sb.charAt(sb.length()-1) == ',')
			sb.deleteCharAt(sb.length()-1);

		sb.insert(0, uid); 
		return sb.toString();
	}

	private static boolean checkIdentical(String currState, String snapshotValStr) {
		return snapshotValStr.substring(snapshotValStr.indexOf(":")+1).equals(currState);
	}

	private static int getBWId(String str) {
		return Integer.parseInt(str.substring(0, str.indexOf(":")));
	}

	private static int normalRecover(
			MemcachedClient mc, String tid, int userId, 
			MongoBGClient mongoClient, Set<String> changes, 
			RecoveryCaller caller, int action) throws IQException, DatabaseFailureException {

		String keyPendingCount = TardisClientConfig.KEY_PENDING_FRIEND_COUNT + userId;
		String keyConfirmedCount = TardisClientConfig.KEY_FRIEND_COUNT + userId;
		String keyPendingFriend = TardisClientConfig.KEY_LIST_FRIENDS_REQUEST + userId;
		String keyConfirmedFriend = TardisClientConfig.KEY_LIST_FRIENDS + userId;

		CLValue val1 = null, val2 = null, v1= null, v2 = null;

		v1 = mc.ewread(tid, keyPendingCount, getHashCode(userId), true);
		v2 = mc.ewread(tid, keyConfirmedCount, getHashCode(userId), true);
		val1 = mc.ewread(tid, keyPendingFriend, getHashCode(userId), false);
		val2 = mc.ewread(tid, keyConfirmedFriend, getHashCode(userId), false);

		if (v1 == null || v2 == null || 
				val1 == null || val2 == null) return 0;

		if (!v1.isPending() && !v2.isPending() && 
				!val1.isPending() && !val2.isPending()) {
			return 0;
		}

		Set<String> pendingFriends = MemcachedSetHelper.convertSet(val1.getValue());
		Set<String> friends = MemcachedSetHelper.convertSet(val2.getValue());

		List<String> pendingWrites = new ArrayList<>();
		if (changes != null) { 
			pendingWrites.addAll(changes);
		}

		CLValue val = null;
		String pw = TardisClientConfig.getUserLogKey(userId);
		if (friends == null || pendingFriends == null) {      
			// get the pending changes            
			List<String> pw_val = null;
			val = mc.ewread(tid, pw, getHashCode(userId), true);
			if (val.getValue() != null) {
				pw_val = MemcachedSetHelper.convertList(val.getValue());
			}
			if (pw_val != null) {
				//        for (String s: pw_val) {
				//          pendingWrites.add(s);
				//        }
				pendingWrites.addAll(pw_val);
			}
		}

		int updateRet = doNONIdempotentRecover(mongoClient, userId, pendingWrites, 
				friends, pendingFriends);

		if (updateRet == MemMongoClient.SUCCESS) {
			if (val != null)
				mc.ewswap(tid, pw, getHashCode(userId), TardisClientConfig.DELIMITER);

			//      if (isPending) {
			switch (caller) {
			case AR:
				TimedMetrics.getInstance().add(MetricsName.METRICS_NUMBER_RECOVERED_BY_AR_WORKER);
				break;
			case READ:
			case WRITE:
				TimedMetrics.getInstance().add(MetricsName.METRICS_NUMBER_RECOVERED_BY_APP);
				break;
			default:
				break;
			}
			//      }
		} else {      
			throw new DatabaseFailureException();
		}

		return 0;
	}

	private static int doNONIdempotentRecover(MongoBGClient mongoClient, 
			int userId, List<String> pendingWrites, 
			Set<String> friends, Set<String> pendingFriends) {
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

		return mongoClient.updateUserDocument(String.valueOf(userId), friends, pendingFriends,
				addFriends, removeFriends, addPendingFriends, removePendingFriends);
	}

	/**
	 * Recover the document, there is no change to key-value pairs.
	 * @param mc
	 * @param tid
	 * @param userId
	 * @return
	 */
	public static int docRecover(
			MemcachedClient mc, String tid, int userId, 
			MongoBGClientDelegate mongoClient, Set<String> changes, 
			RecoveryCaller caller, int action) throws IQException, DatabaseFailureException {
		if (TardisClientConfig.SNAPSHOT) {
			return snapshotRecover(mc, tid, userId, mongoClient, changes, caller, action);
		} else {
			return normalRecover(mc, tid, userId, mongoClient, changes, caller, action);
		}
	}
}
