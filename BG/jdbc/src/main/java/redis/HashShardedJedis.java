package redis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;

/**
 * 
 * sharded jedis based on user id
 * 
 * @author haoyuh
 *
 */
public class HashShardedJedis {
	List<Jedis> clients = new ArrayList<>();

	public HashShardedJedis(String... urls) {
		for (String url : urls) {
			String[] ip = url.split(":");
			Jedis client = new Jedis(ip[0], Integer.parseInt(ip[1]));
			clients.add(client);
		}
	}

	private static final Map<String, Map<Integer, String>> scriptNameServerIndexSHAMap = new HashMap<>();

	public String scriptLoad(String script) {
		Map<Integer, String> map = new HashMap<>();
		for (int i = 0; i < clients.size(); i++) {
			String sha = clients.get(i).scriptLoad(script);
			map.put(i, sha);
		}
		scriptNameServerIndexSHAMap.put(script, map);
		return null;
	}

	public int getServerIndex(int id) {
		if (clients.size() == 1) {
			return 0;
		}
		return id % clients.size();
	}

	public Long setnx(int id, String key, String value) {
		return clients.get(getServerIndex(id)).setnx(key, value);
	}

	public Object evalsha(int id, String scriptName, List<String> keys, List<String> args) {
		int serverIndex = getServerIndex(id);
		return clients.get(serverIndex).evalsha(scriptNameServerIndexSHAMap.get(scriptName).get(serverIndex), keys,
				args);
	}

	public String set(int id, byte[] key, byte[] value) {
		return clients.get(getServerIndex(id)).set(key, value);
	}

	public Boolean exists(int id, String key) {
		return clients.get(getServerIndex(id)).exists(key);
	}

	public Long del(int id, String key) {
		return clients.get(getServerIndex(id)).del(key);
	}

	public List<String> lrange(int id, String key, long start, long end) {
		return clients.get(getServerIndex(id)).lrange(key, start, end);
	}

	public Long sadd(int id, String key, String... members) {
		return clients.get(getServerIndex(id)).sadd(key, members);
	}

	public Set<String> smembers(int id, String key) {
		return clients.get(getServerIndex(id)).smembers(key);
	}

	public Long srem(int id, String key, String... members) {
		return clients.get(getServerIndex(id)).srem(key, members);
	}

	@SuppressWarnings("unchecked")
	public Map<String, Long> mscard(List<Integer> ids, String prefix) {
		Map<Integer, List<String>> serverKeysMap = new HashMap<>();
		Map<String, Long> result = new HashMap<>();

		ids.stream().forEach(i -> {
			int serverId = getServerIndex(i);
			List<String> list = serverKeysMap.get(serverId);
			if (list == null) {
				list = new ArrayList<>();
				serverKeysMap.put(serverId, list);
			}
			list.add(prefix + i);
		});

		serverKeysMap.forEach((serverId, keys) -> {
			Jedis client = clients.get(serverId);
			List<Long> response = (List<Long>) client.evalsha(
					scriptNameServerIndexSHAMap.get(RedisLuaScripts.MULTI_SET_SCARD).get(serverId), keys,
					Collections.emptyList());
			for (int i = 0; i < keys.size(); i++) {
				result.put(keys.get(i), response.get(i));
			}
		});
		return result;
	}

	@SuppressWarnings("unchecked")
	public Map<String, Long> mllen(int id1, String prefix1, Collection<Integer> ids, String prefix2,
			List<String> id1Result) {
		Map<Integer, List<String>> serverKeysMap = new HashMap<>();
		Map<String, Long> result = new HashMap<>();

		ids.stream().forEach(i -> {
			int serverId = getServerIndex(i);
			List<String> list = serverKeysMap.get(serverId);
			if (list == null) {
				list = new ArrayList<>();
				serverKeysMap.put(serverId, list);
			}
			list.add(prefix2 + i);
		});

		// execute multi-scard2 on id1
		int server1 = getServerIndex(id1);
		List<String> server1Args = serverKeysMap.get(server1);
		List<String> server1Keys = new ArrayList<>();
		server1Keys.add(prefix1 + id1);
		if (server1Args == null) {
			server1Args = Collections.emptyList();
		}

		Jedis client = clients.get(server1);
		List<List<?>> response = (List<List<?>>) client.evalsha(
				scriptNameServerIndexSHAMap.get(RedisLuaScripts.MULTI_LIST_LLEN_2).get(server1), server1Keys,
				server1Args);
		id1Result.addAll((List<String>) response.get(0));

		for (int i = 0; i < server1Args.size(); i++) {
			result.put(server1Args.get(i), (Long) response.get(1).get(i));
		}

		serverKeysMap.forEach((serverId, keys) -> {
			if (serverId == server1) {
				return;
			}
			Jedis client2 = clients.get(serverId);
			List<Long> response2 = (List<Long>) client2.evalsha(
					scriptNameServerIndexSHAMap.get(RedisLuaScripts.MULTI_LIST_LLEN).get(serverId), keys,
					Collections.emptyList());
			for (int i = 0; i < keys.size(); i++) {
				result.put(keys.get(i), response2.get(i));
			}
		});
		return result;
	}

	public Jedis getJedis(int id) {
		return clients.get(getServerIndex(id));
	}

	public Jedis getJedisByServerId(int serverId) {
		return clients.get(serverId);
	}

	public void close() {
		for (Jedis client : clients) {
			client.close();
		}
	}

	public int size() {
		return clients.size();
	}

	public static void main(String[] args) {
		HashShardedJedis client = new HashShardedJedis("127.0.0.1:11211");
		client.getJedis(0).flushAll();

		client.scriptLoad(RedisLuaScripts.MULTI_LIST_LLEN_2);
		client.scriptLoad(RedisLuaScripts.MULTI_LIST_LLEN);

		client.getJedis(0).sadd("EW0", "1", "2", "3");
		client.getJedis(0).rpush("U1", "1", "2", "3");
		List<Integer> ids = new ArrayList<>();
		ids.add(1);
		ids.add(2);
		ids.add(3);
		List<String> id1Result = new ArrayList<>();

		System.out.println(client.mllen(0, "EW", ids, "U", id1Result));;
		System.out.println(id1Result);
		client.close();

	}
}
