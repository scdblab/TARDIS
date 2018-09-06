package redis;

import static tardis.TardisClientConfig.ACTION_PW_FRIENDS;
import static tardis.TardisClientConfig.ACTION_PW_PENDING_FRIENDS;
import static edu.usc.bg.workloads.CoreWorkload.DATABASE_FAILURE;
import static tardis.TardisClientConfig.DELIMITER;
import static tardis.TardisClientConfig.KEY_LIST_FRIENDS;
import static tardis.TardisClientConfig.KEY_LIST_FRIENDS_REQUEST;
import static tardis.TardisClientConfig.USER_DIRTY;
import static tardis.TardisClientConfig.getEWLogKeyFromUserId;
import static tardis.TardisClientConfig.getUserLogKey;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import mongodb.*;
import mongodb.RecoveryResult;
import redis.clients.jedis.Jedis;
import tardis.TardisClientConfig;
import tardis.TardisRecoveryEngine;

public class RedisRecoveryEngine {

	private final HashShardedJedis redisClient;
	private final MongoBGClient mongoClient;
	private final Logger logger = Logger.getLogger(RedisRecoveryEngine.class);

	private static final int PAGE = 100;

	public RedisRecoveryEngine(HashShardedJedis client, MongoBGClient mongoClient) {
		super();
		this.redisClient = client;
		this.mongoClient = mongoClient;
	}

	/**
	 * @param user1
	 * @param user2
	 * @param add
	 * @param luaKeys
	 * @return true if log already exists
	 */
	public boolean appendFriendActionToLog(int user1, int user2, boolean add, List<String> luaKeys) {
		String key = getUserLogKey(user1);
		String value = translate(ACTION_PW_FRIENDS, user2, add ? "+" : "-");
		int length = value.length() + DELIMITER.length();
		logger.debug("put " + key + " " + value);
		try {

			luaKeys.clear();
			luaKeys.add(key);
			luaKeys.add(value);

			if ((long) redisClient.evalsha(user1, RedisLuaScripts.LIST_APPEND_FIRST_TIME_CHECK, luaKeys,
					Collections.emptyList()) == 1) {
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

	/**
	 * @param user1
	 * @param user2
	 * @param add
	 * @param luaKeys
	 * @return true if log already exists.
	 */
	public boolean appendPendingFriendActionToLog(int user1, int user2, boolean add, List<String> luaKeys) {
		String key = getUserLogKey(user1);
		String value = translate(ACTION_PW_PENDING_FRIENDS, user2, add ? "+" : "-");
		logger.debug("put " + key + " " + value);
		int length = value.length() + DELIMITER.length();
		try {
			luaKeys.clear();
			luaKeys.add(key);
			luaKeys.add(value);

			if ((long) redisClient.evalsha(user1, RedisLuaScripts.LIST_APPEND_FIRST_TIME_CHECK, luaKeys,
					Collections.emptyList()) == 1) {
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

	public boolean addDirtyFlag(int user, List<String> luaKeys) {

		luaKeys.clear();
		luaKeys.add(getUserLogKey(user));
		luaKeys.add(USER_DIRTY);

		try {
			if ((long) redisClient.evalsha(user, RedisLuaScripts.RPUSH_IF_NON_EXIST, luaKeys,
					Collections.emptyList()) == 1) {
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
			redisClient.sadd(TardisClientConfig.getEWId(user), key, value);
		} catch (Exception e) {
			logger.error("Failed to insert user to partition log " + user, e);
		} finally {
			TardisRecoveryEngine.pendingWritesUsers.add(String.valueOf(user));
			TardisRecoveryEngine.pendingWritesMetrics.add(MetricsName.METRICS_DBFAIL_MEM_OVERHEAD, length);
		}
	}

	public static Set<String> convertListToSet(List<String> list) {
		if (list == null || list.isEmpty() || list.size() == 1) {
			return null;
		}
		Set<String> set = new HashSet<>(list);
		set.remove(RedisBGClient.ZERO_FRIEND);
		return set;
	}

	public RecoveryResult recover(RecoveryCaller caller, int userId, List<String> luaKeys) {
		boolean recovered = false;
		List<String> pendingWrites = null;

		try {
			String logKey = getUserLogKey(userId);

			pendingWrites = redisClient.lrange(userId, logKey, 0, -1);

			if (pendingWrites == null || pendingWrites.isEmpty()) {
				logger.debug("caller" + caller + " nothing to recover u" + userId + "u");
				return RecoveryResult.CLEAN;
			}

			luaKeys.clear();
			luaKeys.add(KEY_LIST_FRIENDS + userId);
			luaKeys.add(KEY_LIST_FRIENDS_REQUEST + userId);

			List<List<String>> cachedValue = (List<List<String>>) redisClient
					.evalsha(userId, RedisLuaScripts.TWO_SET_GET, luaKeys, Collections.emptyList());

			// favor cached value than pending writes
			Set<String> friends = convertListToSet(cachedValue.get(0));
			Set<String> pendingFriends = convertListToSet(cachedValue.get(1));
			logger.debug("caller" + caller + "pw writes for user u" + userId + "u " + pendingWrites);
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
				redisClient.del(userId, logKey);
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

	@SuppressWarnings("unchecked")
	public static int measureLostWrites(Jedis memcachedClient) {

		if (!TardisClientConfig.enableMetrics) {
			return 0;
		}

		int count = 0;
		try {
			if (TardisClientConfig.enableMetrics) {
				synchronized (TardisRecoveryEngine.pendingWritesPerUserMetrics) {

					System.out.println(
							"Number of User has pending writes " + TardisRecoveryEngine.pendingWritesPerUserMetrics.getMetrics().size());

					Map<String, AtomicInteger> lostWrites = TardisRecoveryEngine.pendingWritesPerUserMetrics.getMetrics();

					List<String> array = lostWrites.keySet().stream().map(i -> {
						String uid = i.substring(MetricsName.METRICS_PENDING_WRITES.length());
						return TardisClientConfig.getUserLogKey(uid);
					}).collect(Collectors.toList());

					int i = 0;
					while (i < array.size()) {
						int nextPage = Math.min(i + PAGE, array.size());

						List<Long> pw = (List<Long>) memcachedClient.evalsha(RedisLuaScripts.MULTI_LIST_LLEN,
								array.subList(i, nextPage), new ArrayList<>());

						for (Long key : pw) {
							if (key != null) {
								count += key.intValue();
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

}