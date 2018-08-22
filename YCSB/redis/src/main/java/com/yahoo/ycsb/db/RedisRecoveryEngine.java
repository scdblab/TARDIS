package com.yahoo.ycsb.db;

import static com.yahoo.ycsb.db.TardisClientConfig.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.yahoo.ycsb.Status;

public class RedisRecoveryEngine {
	private final HashShardedJedis redis;
	private final MongoDbClient mongo;
	public static final ConcurrentHashMap<String, String> dirtyDocumentIds = new ConcurrentHashMap<>();

	public RedisRecoveryEngine(HashShardedJedis redis, MongoDbClient mongo) {
		super();
		this.redis = redis;
		this.mongo = mongo;
	}

	public RecoveryResult recover(RecoveryCaller caller, String key) {
		int id = redis.getKeyServerIndex(key);

		String bufferedWriteKey = bufferedWriteKey(key);
		String normalKey = normalKey(key);

		if (!redis.exists(id, bufferedWriteKey)) {
			return RecoveryResult.CLEAN;
		}
		
		// buffered writes exist.
		switch (caller) {
		case AR:
			if (!TardisClientConfig.ARApplyBufferedWrites) {
				return RecoveryResult.FAIL;
			}
			break;
		case READ:
			if (!TardisClientConfig.readApplyBufferedWrites) {
				return RecoveryResult.SKIP;
			}
			break;
		case WRITE:
			if (!TardisClientConfig.updateApplyBufferedWrites) {
				return RecoveryResult.SKIP;
			}
			break;
		default:
			break;
		}

		Map<String, String> bufferedWrites = redis.hgetAll(id, normalKey);

		if (bufferedWrites.isEmpty()) {
			bufferedWrites = redis.hgetAll(id, bufferedWriteKey);
		}

		if (!Status.OK.equals(mongo.update(normalKey(key), bufferedWrites))) {
			return RecoveryResult.FAIL;
		}
		redis.del(id, bufferedWriteKey);
		dirtyDocumentIds.remove(key);

		if (dirtyDocumentIds.isEmpty()) {
			long start = Long.parseLong(TardisMetrics.metrics.get("recover-start"));
			long now = System.currentTimeMillis();
			TardisMetrics.metrics.put("recover-end", String.valueOf(now));
			TardisMetrics.metrics.put("recover-duration", String.valueOf(now - start));
		}

		return RecoveryResult.SUCCESS;
	}

	public void addDirtyDocumentId(String key) {
		dirtyDocumentIds.put(key, "");
	}
	
	public int dirtyDocs() {
		return dirtyDocumentIds.size();
	}

}
