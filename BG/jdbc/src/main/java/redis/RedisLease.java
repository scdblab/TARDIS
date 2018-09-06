package redis;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

public class RedisLease {

	private static final long JITTER_BASE = 30;
	private static final int JITTER_RANGE = 20;

	private final Random random;
	private final HashShardedJedis redisClient;
	private final int clientId;
	private final String caller;
	private static final AtomicInteger global_id = new AtomicInteger();

	private final Logger logger = Logger.getLogger(RedisLease.class);

	public RedisLease(HashShardedJedis redisClient, String caller) {
		super();
		this.redisClient = redisClient;
		this.clientId = global_id.incrementAndGet();
		this.random = new Random();
		this.caller = caller;
	}

	public void acquireTillSuccess(int id, String key, int timeout) {
		int tries = 0;
		while (!acquireLease(id, key, timeout)) {
			try {
				Thread.sleep(JITTER_BASE + random.nextInt(JITTER_RANGE));
			} catch (Exception e) {
				logger.error("acquire leases failed on key " + key, e);
			}
			if (tries >= 10) {
				logger.error("acquire lease on key failed for > 10 times " + key);
			}
			tries++;
		}
	}
	
	public boolean acquireLease(int id, String key, int timeout) {
		return redisClient.setnx(id, key, caller + String.valueOf(this.clientId)) == 1;
	}

	public void releaseLease(int id, String key, List<String> keys) {
		keys.clear();
		keys.add(key);
		keys.add(caller + String.valueOf(this.clientId));
		if ((long) redisClient.evalsha(id, RedisLuaScripts.RELEASE_LEASE, keys, Collections.emptyList()) == 0) {
			logger.error("release lease failed on key " + key);
		}
	}

//	public static void main(String[] args) {
//		Jedis jedis = new Jedis();
//		RedisLease lease = new RedisLease(jedis);
//		RedisLuaScripts.luaSHA.put(RedisLuaScripts.RELEASE_LEASE, jedis.scriptLoad(RedisLuaScripts.RELEASE_LEASE));
//		
//		System.out.println(lease.acquireLease("t", 1000));
//		System.out.println(lease.acquireLease("t", 1000));
//		lease.releaseLease("t", new ArrayList<>());
//		System.out.println(lease.acquireLease("t", 1000));
//		jedis.close();
//	}
	
}
