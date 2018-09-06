package mongodb;

import static tardis.TardisClientConfig.ACTION_PW_FRIENDS;
import static tardis.TardisClientConfig.ACTION_PW_PENDING_FRIENDS;
import static edu.usc.bg.workloads.CoreWorkload.DATABASE_FAILURE;
import static tardis.TardisClientConfig.DELIMITER;
import static tardis.TardisClientConfig.KEY_LIST_FRIENDS;
import static tardis.TardisClientConfig.KEY_LIST_FRIENDS_REQUEST;
import static tardis.TardisClientConfig.USER_DIRTY;
import static tardis.TardisClientConfig.getEWLogKeyFromUserId;
import static tardis.TardisClientConfig.getUserLogKey;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.meetup.memcached.MemcachedClient;

import tardis.TardisClientConfig;
import tardis.TardisRecoveryEngine;

public class RecoveryEngine {

	private final MemcachedClient memcachedClient;
	private final MongoBGClient mongoClient;
	private final Logger logger = Logger.getLogger(RecoveryEngine.class);
	
	private boolean lazyRecovery = true;

	private static final int PAGE = 100;

	public RecoveryEngine(MemcachedClient client, MongoBGClient mongoClient) {
		super();
		this.memcachedClient = client;
		this.mongoClient = mongoClient;
	}

	public boolean appendFriendActionToLog(int user1, int user2, boolean add) {
		String key = getUserLogKey(user1);
		String value = translate(ACTION_PW_FRIENDS, user2, add ? "+" : "-");
		int length = value.length() + DELIMITER.length();
		logger.debug("put " + key + " " + value);
		try {
			if (MemcachedSetHelper.addToSetAppend(memcachedClient, key, user1, value, true)) {
				return true;
			}
			length += key.length();
			return false;
		} catch (Exception e) {
			logger.error("Failed to append friend action to pending log " + user1 + ", " + user2, e);
		} finally {
			if (TardisClientConfig.enableMetrics) {
				TardisRecoveryEngine.pendingWritesMetrics.add(MetricsName.METRICS_DBFAIL_MEM_OVERHEAD, length);
				TardisRecoveryEngine.pendingWritesMetrics.add(MetricsName.METRICS_PENDING_WRITES);
				synchronized (TardisRecoveryEngine.pendingWritesPerUserMetrics) {
					TardisRecoveryEngine.pendingWritesPerUserMetrics.add(MetricsName.METRICS_PENDING_WRITES + user1);
				}
			}
		}
		return false;
	}

	public boolean appendPendingFriendActionToLog(int user1, int user2, boolean add) {
		String key = getUserLogKey(user1);
		String value = translate(ACTION_PW_PENDING_FRIENDS, user2, add ? "+" : "-");
		logger.debug("put " + key + " " + value);
		int length = value.length() + DELIMITER.length();
		try {
			if (MemcachedSetHelper.addToSetAppend(memcachedClient, key, user1, value, true)) {
				return true;
			}
			length += key.length();
			return false;
		} catch (Exception e) {
			logger.error("Failed to append pending friend action to pending log " + user1 + ", " + user2, e);
		} finally {

			if (TardisClientConfig.enableMetrics) {
				TardisRecoveryEngine.pendingWritesMetrics.add(MetricsName.METRICS_DBFAIL_MEM_OVERHEAD, length);
				TardisRecoveryEngine.pendingWritesMetrics.add(MetricsName.METRICS_PENDING_WRITES);
				synchronized (TardisRecoveryEngine.pendingWritesPerUserMetrics) {
					TardisRecoveryEngine.pendingWritesPerUserMetrics.add(MetricsName.METRICS_PENDING_WRITES + user1);
				}
			}
		}
		return false;
	}

	public boolean addDirtyFlag(int user) {
		try {
			if (memcachedClient.add(getUserLogKey(user), USER_DIRTY + DELIMITER, user)) {
				TimedMetrics.getInstance().add(MetricsName.METRICS_DBFAIL_MEM_OVERHEAD,
						getUserLogKey(user).length() + USER_DIRTY.length() + DELIMITER.length());
				return true;
			}
			return false;
		} catch (Exception e) {
			logger.error("add dirty flag failed for user " + user);
		}
		return false;
	}

	public void insertUserToPartitionLog(int user) {
		String key = getEWLogKeyFromUserId(user);
		String value = String.valueOf(user);
		int length = value.length() + DELIMITER.length();
		try {
			if (!MemcachedSetHelper.addToSetAppend(memcachedClient, key, TardisClientConfig.getEWId(user), value, true)) {
				length += key.length();
			}
		} catch (Exception e) {
			logger.error("Failed to insert user to partition log " + user, e);
		} finally {
			TardisRecoveryEngine.pendingWritesUsers.add(String.valueOf(user));
			TardisRecoveryEngine.pendingWritesMetrics.add(MetricsName.METRICS_DBFAIL_MEM_OVERHEAD, length);
		}
	}

	public RecoveryResult recover(RecoveryCaller caller, int userId) {
		if (lazyRecovery == false && 
				(caller == RecoveryCaller.READ || caller == RecoveryCaller.WRITE)) {
			return RecoveryResult.FAIL;
		}		
		
		boolean recovered = false;
		List<String> pendingWrites = null;

		try {
			String logKey = getUserLogKey(userId);

			Object pw = memcachedClient.get(logKey, userId, true);

			if (pw == null) {
				logger.debug("caller" + caller + " nothing to recover u" + userId + "u");
				return RecoveryResult.CLEAN;
			}

			pendingWrites = MemcachedSetHelper.convertList(pw);

			Object[] cachedValue = memcachedClient.getMultiArray(
			    new String[] { KEY_LIST_FRIENDS + userId, KEY_LIST_FRIENDS_REQUEST + userId }, 
			    new Integer[] {userId, userId});
//			Map<String, Object> cachedValue = memcachedClient
//					.getMulti(new String[] { KEY_LIST_FRIENDS + userId, KEY_LIST_FRIENDS_REQUEST + userId });

			// favor cached value than pending writes
//			Set<String> friends = MemcachedSetHelper.convertSet(cachedValue.get(KEY_LIST_FRIENDS + userId));
			Set<String> friends = MemcachedSetHelper.convertSet(cachedValue[0]);
			Set<String> pendingFriends = MemcachedSetHelper.convertSet(cachedValue[1]);
			logger.debug("caller" + caller + "pw writes for user u" + userId + "u " + pw);
			logger.debug("caller" + caller + "cached friends for user u" + userId + "u " + friends);
			logger.debug("caller" + caller + "cached pending friends for user u" + userId + "u " + pendingFriends);

			int updateRet = 0;

			if (friends == null || pendingFriends == null) {
				// merge pending writes
				Map<String, Integer> friendsMap = new HashMap<>();
				Map<String, Integer> pendingFriendsMap = new HashMap<>();

				Set<String> addFriends = new HashSet<>();
				Set<String> removeFriends = new HashSet<>();
				Set<String> addPendingFriends = new HashSet<>();
				Set<String> removePendingFriends = new HashSet<>();

				for (String write : pendingWrites) {

					if (USER_DIRTY.equals(write)) {
						continue;
					}

					char action = write.charAt(0);
					char op = write.charAt(1);
					String user = write.substring(2);

					switch (action) {
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

				updateRet = this.mongoClient.updateUserDocument(String.valueOf(userId), friends, pendingFriends,
						addFriends, removeFriends, addPendingFriends, removePendingFriends);
			} else {
				updateRet = this.mongoClient.updateUserDocument(String.valueOf(userId), friends, pendingFriends, null,
						null, null, null);
			}

			if (updateRet == DATABASE_FAILURE) {
				return RecoveryResult.FAIL;
			} else {
				recovered = true;
				memcachedClient.delete(logKey, userId, null);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Encountered failure during recovery for user " + userId, e);
		} finally {
			if (recovered) {
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
				if (pendingWrites != null) {
					TardisRecoveryEngine.recoveredUsers.add(String.valueOf(userId));
					TardisRecoveryEngine.pendingWritesMetrics.add(MetricsName.METRICS_RECOVERED_WRITES_COUNT, pendingWrites.size());
					TimedMetrics.getInstance().add(MetricsName.METRICS_RECOVERED_WRITES, pendingWrites.size());
				}
			}
		}
		return RecoveryResult.SUCCESS;
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

	private static void consolidate(Map<String, Integer> map, Set<String> addResult, Set<String> removeResult) {
		map.forEach((k, v) -> {
			if (v < 0) {
				removeResult.add(k);
			} else if (v > 0) {
				addResult.add(k);
			}
		});
	}

	public static int measureLostWrites(MemcachedClient memcachedClient) {
		
		if (!TardisClientConfig.enableMetrics) {
			return 0;
		}
		
		int count = 0;
		try {
			if (TardisClientConfig.enableMetrics) {
				synchronized (TardisRecoveryEngine.pendingWritesPerUserMetrics) {

					System.out.println("Number of User has pending writes "
							+ TardisRecoveryEngine.pendingWritesPerUserMetrics.getMetrics().size());

					Map<String, AtomicInteger> lostWrites = TardisRecoveryEngine.pendingWritesPerUserMetrics.getMetrics();

					List<String> array = lostWrites.keySet().stream().map(i -> {

						String uid = i.substring(MetricsName.METRICS_PENDING_WRITES.length());
						return TardisClientConfig.getUserLogKey(uid);
					}).collect(Collectors.toList());

					int i = 0;
					while (i < array.size()) {
						int nextPage = Math.min(i + PAGE, array.size());
						String[] subList = array.subList(i, nextPage).toArray(new String[0]);
						Map<String, Object> pw = memcachedClient.getMulti(subList);

						for (String key : subList) {
							Object value = pw.get(key);
							if (value != null) {
								int actual = MemcachedSetHelper.convertList(value).size();
								count += actual;
							}
						}
						i = nextPage;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
		return count;
	}

	public static String translate(char action, int user, String op) {
		return String.format("%s%s%d", action, op, user);
	}

	public static void main(String[] args) {
		Map<String, Integer> m = new HashMap<>();
		m.put("a", 1);
		m.put("b", 1);
		m.put("c", 1);
		m.forEach((k, v) -> {
			m.remove(k);
		});
	}

	public void setLazyRecovery(boolean lazyRecovery) {
		this.lazyRecovery = lazyRecovery;
	}

}