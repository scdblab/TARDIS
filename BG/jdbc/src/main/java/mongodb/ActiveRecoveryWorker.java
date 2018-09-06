package mongodb;

import static tardis.TardisClientConfig.LEASE_TIMEOUT;
import static tardis.TardisClientConfig.NUM_EVENTUAL_WRITE_LOGS;
import static tardis.TardisClientConfig.getEWLogMutationLeaseKey;
import static tardis.TardisClientConfig.getUserLogLeaseKey;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.MemcachedLease;

import mongodb.alpha.AlphaPolicy;
import mongodb.alpha.ExponentialBackoff;
import mongodb.alpha.RandomUtility;
import tardis.TardisClientConfig;

public class ActiveRecoveryWorker implements Callable<Void> {
	private final MemcachedClient memcachedClient;
	private final MemcachedLease leaseClient;
	private final RecoveryEngine recovery;
	private final AtomicBoolean isDatabaseFailed;
	private final int workerId;
	private final AlphaPolicy alphaPolicy;
	private final int totalNumberOfARWorker;
	private final Random r;
	private final ExponentialBackoff backoff = new ExponentialBackoff(1000, 100);

	private boolean isRunning = true;

	private static AtomicInteger id = new AtomicInteger();
	private final Logger logger = Logger.getLogger(ActiveRecoveryWorker.class);

	public ActiveRecoveryWorker(AtomicBoolean isDatabaseFailed, MemcachedClient memcachedClient,
			MemcachedLease leaseClient, RecoveryEngine recovery, AlphaPolicy alphaPolicy, int totalNumberOfARWorker) {
		super();
		this.alphaPolicy = alphaPolicy;
		this.isDatabaseFailed = isDatabaseFailed;
		this.memcachedClient = memcachedClient;
		this.leaseClient = leaseClient;
		this.recovery = recovery;
		this.workerId = id.incrementAndGet();
		this.r = new Random(System.nanoTime());
		this.totalNumberOfARWorker = totalNumberOfARWorker;
		logger.info("created active recovery worker");
	}

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

				Object dirtyUsers = memcachedClient.get(ew, index, true);

				logger.debug("worker " + workerId + " EW " + ew + ", users " + dirtyUsers);

				if (dirtyUsers == null) {
					index = (index + 1) % NUM_EVENTUAL_WRITE_LOGS;
					continue;
				}

				Set<String> recoveredUserIds = new HashSet<>();

				Set<String> dirtyUserIds = MemcachedSetHelper.convertSet(dirtyUsers);
				if (logger.isDebugEnabled()) {
					if (dirtyUserIds.size() != MemcachedSetHelper.convertList(dirtyUsers).size()) {
						logger.fatal("worker " + workerId + " BUG: dirty user set " + dirtyUserIds.size() + ", list "
								+ MemcachedSetHelper.convertList(dirtyUsers).size());
					}
				}

				List<String> userIdToWorkOn = RandomUtility.randomSampling(dirtyUserIds,
						this.alphaPolicy.getAlpha(dirtyUserIds.size(), totalNumberOfARWorker));
				for (String userId : userIdToWorkOn) {

					if (!isRunning) {
						break;
					}

					String logKey = TardisClientConfig.getUserLogKey(userId);
					Object pw = memcachedClient.get(logKey, Integer.parseInt(userId), true);
					if (pw == null) {
						recoveredUserIds.add(userId);
						continue;
					}

					if (leaseClient.acquireLease(getUserLogLeaseKey(userId), 
					    Integer.parseInt(userId), LEASE_TIMEOUT)) {
						RecoveryResult ret = recovery.recover(RecoveryCaller.AR, Integer.parseInt(userId));

						if (!RecoveryResult.FAIL.equals(ret)) {
							recoveredUserIds.add(userId);
						}
						leaseClient.releaseLease(
						    getUserLogLeaseKey(userId), Integer.parseInt(userId));

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

				String[] recoveredUserLogs = new String[recoveredUserIds.size()+1];
//				Set<String> recoveredUserLogs = new HashSet<>();
				Integer[] hashCodes = new Integer[recoveredUserIds.size()+1];
				
				int i = 0;
				for (String userId : recoveredUserIds) {
//					recoveredUserLogs.add(TardisClientConfig.getUserLogKey(userId));
					recoveredUserLogs[i] = TardisClientConfig.getUserLogKey(userId);
					hashCodes[i] = Integer.parseInt(userId);
					i++;
				}

				leaseClient.acquireTillSuccess(
				    getEWLogMutationLeaseKey(ew), index, LEASE_TIMEOUT);
//				recoveredUserLogs.add(ew);
				recoveredUserLogs[i] = ew;
				hashCodes[i] = index;
				
//				Map<String, Object> results = memcachedClient.getMulti(recoveredUserLogs.toArray(new String[0]));
				
				Object[] results = memcachedClient.getMultiArray(recoveredUserLogs, hashCodes);

//				Set<String> newEW = MemcachedSetHelper.convertSet(results.remove(ew));
				Set<String> newEW = MemcachedSetHelper.convertSet(results[results.length-1]);

				if (newEW != null) {
					// remove new users that are dirty from recovered set
//					results.keySet().forEach(k -> {
					for (i = 0; i < recoveredUserIds.size(); i++) {
					  String k = recoveredUserLogs[i];
						int userId = -1;
						try {
							userId = TardisClientConfig.getUserIdFromUserLogKey(k);
						} catch (Exception e) {
							e.printStackTrace();
						}
						if (userId != -1) {
							recoveredUserIds.remove(userId);
						}
//					});
					}

//					logger.debug("new dirty users " + results.keySet());
					// remove recovered users from EW list
					newEW.removeAll(recoveredUserIds);
					if (newEW.isEmpty()) {
						memcachedClient.delete(ew, index, null);
					} else {
						memcachedClient.set(ew, 
						    MemcachedSetHelper.convertString(newEW), index);
					}
				}
				leaseClient.releaseLease(
				    getEWLogMutationLeaseKey(ew), index);
				index = (index + 1) % NUM_EVENTUAL_WRITE_LOGS;
			} while (index != start);
			sleep();
		}
		return null;
	}

	private void sleep() {
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
			logger.error("sleep got interrupted", e);
		}
	}

	public void shutdown() {
		isRunning = false;
		logger.info("shutdown active recovery worker");
	}
}
