package redis;

import static tardis.TardisClientConfig.LEASE_TIMEOUT;
import static tardis.TardisClientConfig.NUM_EVENTUAL_WRITE_LOGS;
import static tardis.TardisClientConfig.getEWLogMutationLeaseKey;
import static tardis.TardisClientConfig.getUserLogLeaseKey;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import mongodb.MongoBGClientDelegate;
import mongodb.RecoveryCaller;
import mongodb.RecoveryResult;
import mongodb.alpha.AlphaPolicy;
import mongodb.alpha.ExponentialBackoff;
import mongodb.alpha.RandomUtility;
import tardis.TardisClientConfig;

public class RedisActiveRecoveryWorker implements Callable<Void> {
	private final HashShardedJedis redisClient;
	private final RedisLease leaseClient;
	private final RedisRecoveryEngine recovery;
	private final AtomicBoolean isDatabaseFailed;
	private final int workerId;
	private final AlphaPolicy alphaPolicy;
	private final int totalNumberOfARWorker;
	private final Random r;
	private final ExponentialBackoff backoff = new ExponentialBackoff(1000, 100);

	private boolean isRunning = true;

	private static AtomicInteger GLOBAL_WORKER_ID = new AtomicInteger();
	private final Logger logger = Logger.getLogger(RedisActiveRecoveryWorker.class);

	private final List<String> luaKeys = new ArrayList<>();

	public RedisActiveRecoveryWorker(AtomicBoolean isDatabaseFailed, HashShardedJedis memcachedClient,
			RedisLease leaseClient, RedisRecoveryEngine recovery, AlphaPolicy alphaPolicy, int totalNumberOfARWorker) {
		super();
		this.alphaPolicy = alphaPolicy;
		this.isDatabaseFailed = isDatabaseFailed;
		this.redisClient = memcachedClient;
		this.leaseClient = leaseClient;
		this.recovery = recovery;
		this.workerId = GLOBAL_WORKER_ID.incrementAndGet();
		this.r = new Random(System.nanoTime());
		this.totalNumberOfARWorker = totalNumberOfARWorker;
		logger.info("created active recovery worker");
	}

	@SuppressWarnings("unchecked")
	@Override
	public Void call() throws Exception {

		while (isRunning) {

			int start = this.r.nextInt(NUM_EVENTUAL_WRITE_LOGS);
			int index = start;
			do {

				if (!isRunning) {
					break;
				}

				String ew = TardisClientConfig.getEWLogKey(index);

				Set<String> dirtyUserIds = redisClient.smembers(index, ew);

				logger.debug("worker " + workerId + " EW " + ew + ", users " + dirtyUserIds);

				if (dirtyUserIds == null || dirtyUserIds.isEmpty()) {
					index = (index + 1) % NUM_EVENTUAL_WRITE_LOGS;
					continue;
				}

				Set<Integer> recoveredUserIds = new HashSet<>();

				List<String> userIdToWorkOn = RandomUtility.randomSampling(dirtyUserIds,
						this.alphaPolicy.getAlpha(dirtyUserIds.size(), totalNumberOfARWorker));
				for (String userId : userIdToWorkOn) {

					if (!isRunning) {
						break;
					}

					String logKey = TardisClientConfig.getUserLogKey(userId);
					boolean pw = redisClient.exists(Integer.parseInt(userId), logKey);
					if (!pw) {
						recoveredUserIds.add(Integer.parseInt(userId));
						continue;
					}

					if (leaseClient.acquireLease(Integer.parseInt(userId), getUserLogLeaseKey(userId), LEASE_TIMEOUT)) {
						RecoveryResult ret = recovery.recover(RecoveryCaller.AR, Integer.parseInt(userId), luaKeys);

						if (!RecoveryResult.FAIL.equals(ret)) {
							recoveredUserIds.add(Integer.parseInt(userId));
						}

						leaseClient.releaseLease(Integer.parseInt(userId), getUserLogLeaseKey(userId), luaKeys);

						if (RecoveryResult.FAIL.equals(ret)) {
							backoff.backoff();
						} else if (RecoveryResult.SUCCESS.equals(ret)) {
							if (MongoBGClientDelegate.DO_ACTUAL_KILL_DB) {
								if (isDatabaseFailed.compareAndSet(true, false)) {
									System.out.println("### Db recovered");
								}
							}
							backoff.reset();
						}
					}
				}

				logger.debug("worker " + workerId + " recovered " + recoveredUserIds);

				if (recoveredUserIds.isEmpty()) {
					index = (index + 1) % NUM_EVENTUAL_WRITE_LOGS;
					continue;
				}

				try {
					leaseClient.acquireTillSuccess(index, getEWLogMutationLeaseKey(ew), LEASE_TIMEOUT);

					luaKeys.clear();
					luaKeys.add(ew);
					
					List<String> newEWList = new ArrayList<>();

					Map<String, Long> results = redisClient.mllen(index, TardisClientConfig.KEY_EVENTUAL_WRITE_LOG,
							recoveredUserIds, TardisClientConfig.KEY_USER_PENDING_WRITES_LOG, newEWList);

					Set<String> newEW = convertListToSet(newEWList);
					Set<String> strRecoveredUserIds = new HashSet<>();
					recoveredUserIds.forEach(i -> {
						strRecoveredUserIds.add(String.valueOf(i));
					});
					

					if (newEW != null) {
						// remove new users that are dirty from the recovered set
						results.forEach((userLog, numberOfBufferedWrites) -> {
							if (numberOfBufferedWrites > 0) {
								strRecoveredUserIds.remove(TardisClientConfig.getUserIdStringFromUserLogKey(userLog));
							}
						});
						// remove recovered users from EW list
						newEW.removeAll(strRecoveredUserIds);
						if (newEW.isEmpty()) {
							redisClient.del(index, ew);
						} else {
							redisClient.srem(index, ew, strRecoveredUserIds.toArray(new String[0]));
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					leaseClient.releaseLease(index, getEWLogMutationLeaseKey(ew), luaKeys);
				}
				index = (index + 1) % NUM_EVENTUAL_WRITE_LOGS;
			} while (index != start);
			sleep();
		}
		return null;
	}

	static final Set<String> convertListToSet(List<String> list) {
		Set<String> set = new HashSet<>(list);
		return set;
	}

	private void sleep() {
		try {
			Thread.sleep(TardisClientConfig.RECOVERY_WORKER_SLEEP_TIME);
		} catch (Exception e) {
			logger.error("sleep got interrupted", e);
		}
	}

	public void shutdown() {
		isRunning = false;
		logger.info("shutdown active recovery worker");
	}
}
