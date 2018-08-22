package com.yahoo.ycsb.db;

import static com.yahoo.ycsb.db.TardisYCSBConfig.NUM_EVENTUAL_WRITE_LOGS;
import static com.yahoo.ycsb.db.TardisYCSBConfig.getEWLogKey;
import static com.yahoo.ycsb.db.TardisYCSBConfig.getHashCode;
import static com.yahoo.ycsb.db.TardisYCSBConfig.RECOVERY_WORKER_BASE_TIME_BETWEEN_CHECKING_EW;
import static com.yahoo.ycsb.db.TardisYCSBConfig.LEASE_TIMEOUT;
import static com.yahoo.ycsb.db.TardisYCSBConfig.getUserLogKey;
import static com.yahoo.ycsb.db.TardisYCSBConfig.getUserId;
import static com.yahoo.ycsb.db.TardisYCSBConfig.getUserLogLeaseKey;

import java.io.IOException;
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
import com.meetup.memcached.MemcachedLease;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.workloads.CoreWorkload;

public class TardisYCSBWorker extends Thread {
	MemcachedClient mc;
	MongoDbClientDelegate mongoClient;
	private final MemcachedLease leaseClient;
	static volatile boolean isRunning = true;

	public static final int CONTEXTUAL_LEASE = 0;
	public static final int RED_LEASE = 1;
	public static int leaseMode = CONTEXTUAL_LEASE;
	private final RecoveryEngine recovery;

	private static AtomicInteger id = new AtomicInteger(0);
	int workerId;

	static Random rand = new Random();
	private int ALPHA = 5;
	static long sleepTime = RECOVERY_WORKER_BASE_TIME_BETWEEN_CHECKING_EW;
	static final long CHECK_SLEEP_TIME = 1000;

	byte[] read_buffer;
	private final ExponentialBackoff backoff = new ExponentialBackoff(1000, 100);

	private final static Logger logger = Logger.getLogger(TardisYCSBWorker.class);

	public TardisYCSBWorker(MongoDbClientDelegate client, int alpha, 
			String[] serverlist) {
		ALPHA = alpha;
		mc = new MemcachedClient(TardisYCSBConfig.BENCHMARK);//MemcachedMongoBGClient.getMemcachedClient(serverlist, "BG");
		mongoClient = client;
		workerId = id.incrementAndGet();
		leaseClient = new MemcachedLease(workerId, mc, false);
		recovery = new RecoveryEngine(mc, mongoClient);

		read_buffer = new byte[1024*5];

		try {
			mongoClient.init();
		} catch (com.yahoo.ycsb.DBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		System.out.println("Start AR worker");
		String[] EWs = new String[NUM_EVENTUAL_WRITE_LOGS];
		for (int i = 0; i < EWs.length; i++) {
			EWs[i] = getEWLogKey(i);
		}

		while (isRunning) {
			int idx = rand.nextInt(EWs.length);
			int start = idx;

			int totalRecovers = 0;
			do {
				// get random EW
				Object ew = mc.get(EWs[idx], idx, true);
				Set<String> keyset = MemcachedSetHelper.convertSet(ew);

				if (keyset == null || keyset.size() == 0) {
					// get an empty ew, move on
					idx = getNextIdx(idx);        
					continue;
				}        

				// pick random alpha ids
				keyset = (Set<String>) pickRandom(keyset, ALPHA);
				logger.debug("Got buffered writes...");
				logger.debug(Arrays.toString(keyset.toArray(new String[0])));

				Set<String> recoverKeys = new HashSet<>();
				for (String key: keyset) {
					if (!isRunning) {
						break;
					}          

					if (leaseMode == CONTEXTUAL_LEASE) {
						recoverWithContextualLeases(key, recoverKeys);
					} else if (leaseMode == RED_LEASE) {
						recoverWithRedlease(key, recoverKeys);
					}
				}

				// update EW
				if (recoverKeys.size() > 0) {
					totalRecovers += recoverKeys.size();
					if (leaseMode == CONTEXTUAL_LEASE)
						updateEWContextualLease(EWs[idx], recoverKeys);
					else if (leaseMode == RED_LEASE)
						updateEWRedlease(EWs[idx], recoverKeys, idx);
				}

				// pick a different idx for next
				idx = getNextIdx(idx);
			} while (idx != start && isRunning);

			if (totalRecovers == 0) {
				sleepFor(CHECK_SLEEP_TIME);
			} else {
				totalRecovers = 0;
			}
		}

		System.out.println("AR Worker stopped.");
	}

	private void updateEWRedlease(String ew, Set<String> recoverKeys, int index) {
		String[] recoveredUserLogs = new String[recoverKeys.size()+1];
		Integer[] hashCodes = new Integer[recoverKeys.size()+1];

		int i = 0;
		for (String key : recoverKeys) {
			long userId = TardisYCSBConfig.extractUserId(key);
			recoveredUserLogs[i] = TardisYCSBConfig.getUserLogKey(userId);
			hashCodes[i] = TardisYCSBConfig.getHashCode(userId);
			i++;
		}
		recoveredUserLogs[i] = ew;
		hashCodes[i] = index;

		leaseClient.acquireTillSuccess(
				TardisYCSBConfig.getEWLogMutationLeaseKey(ew), index, LEASE_TIMEOUT);

		Object[] results = mc.getMultiArray(recoveredUserLogs, hashCodes);
		Set<String> newEW = MemcachedSetHelper.convertSet(results[results.length-1]);
		if (newEW != null) {
			// remove new users that are dirty from recovered set
			for (i = 0; i < recoverKeys.size(); i++) {
				String k = recoveredUserLogs[i];
				recoverKeys.remove(k);
			}

			if (recoverKeys.size() > 0) {
				logger.debug("new dirty users " + recoverKeys);
			}
			// remove recovered users from EW list
			newEW.removeAll(recoverKeys);
			if (newEW.isEmpty()) {
				try {
					mc.delete(ew, index, null);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				try {
					mc.set(ew, 
							MemcachedSetHelper.convertString(newEW), index);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		leaseClient.releaseLease(
				TardisYCSBConfig.getEWLogMutationLeaseKey(ew), index);
	}

	private void recoverWithContextualLeases(String key, Set<String> recoverKeys) {
		Integer hashCode = getHashCode(key);
		
		String pwKey = TardisYCSBConfig.getPWLogKey(getUserId(key));
		Object pwVal = mc.get(pwKey, hashCode, false);
		if (pwVal == null) {
			CADSMongoDbClient.findNoBuff.incrementAndGet();
			recoverKeys.add(key);
			return;
		}

		while (true) {
			String tid = mc.generateSID();
			boolean dbfail = false;
			try {
				docRecover(mc, tid, key, hashCode, 
						mongoClient, null, TardisYCSBConfig.ACT_AR, read_buffer);

				mc.ewcommit(tid, hashCode, false);
				CADSMongoDbClient.numDocsRecoveredInARs.incrementAndGet();
				recoverKeys.add(key);
				break;
			} catch (DatabaseFailureException e ) {
				try {
					mc.ewcommit(tid, hashCode, true);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				dbfail = true;
			} catch (IQException e) { }

			CADSWbMongoDbClient.numSessRetriesInARs.incrementAndGet();

			// at this point, fail to recover
			// sleep exponentially and retry on a different doc
			if (dbfail == true) {
				sleepTime = Math.min(sleepTime * 2, CHECK_SLEEP_TIME);
				sleepFor(sleepTime);
			} else {
				resetTime();
				sleepFor(sleepTime);
			}
			break;
		}
	}

	private void recoverWithRedlease(String key, Set<String> recoverKeys) {
		logger.debug("Recover key="+key);

		long userId = TardisYCSBConfig.extractUserId(key);
		Integer hashCode = getHashCode(userId);

		String logKey = getUserLogKey(userId);
		byte[] pw = (byte[]) mc.get(logKey, getHashCode(userId), false);
		if (pw == null) {
			return;
		}

		if (leaseClient.acquireLease(
				getUserLogLeaseKey(userId), hashCode, LEASE_TIMEOUT)) {      
			RecoveryResult ret = recovery.recover(RecoveryCaller.AR, key, userId, read_buffer);
			logger.debug("Recover success key="+key);

			if (!RecoveryResult.FAIL.equals(ret)) {
				recoverKeys.add(key);
			}

			try {
				mc.delete(logKey, getHashCode(userId), null);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			leaseClient.releaseLease(
					getUserLogLeaseKey(userId), hashCode);

			if (RecoveryResult.FAIL.equals(ret)) {
				backoff.backoff();
			} else if (RecoveryResult.SUCCESS.equals(ret)) {
				logger.debug("### Db recovered");
				backoff.reset();
			}
		}
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

	private int updateEWContextualLease(String ewKey, Set<String> keySet) {
		CLValue val = null;

		List<String> idlist = null;
		while (true) {
			String tid = mc.generateSID();

			try {
				val = mc.ewread(tid, ewKey, getHashCode(ewKey), true);

				if (val == null) {
					logger.fatal("Something went wrong...clval = null with ewKey="+ewKey);
					System.exit(-1);
				} else {
					Object obj = val.getValue();
					if (obj != null) {
						if (obj != null) {
							idlist = MemcachedSetHelper.convertList(obj);
							for (String key: keySet)
								idlist.remove(key);
							String newVal = MemcachedSetHelper.convertString(idlist);
							mc.ewswap(tid, ewKey, getHashCode(ewKey), newVal);
						}
					}
				}

				mc.ewcommit(tid, getHashCode(ewKey), false);
				break;
			} catch (IQException e1) { }

			sleepFor(TardisYCSBConfig.QLEASE_BACKOFF);
		}

		return idlist == null ? 0 : idlist.size();
	}

	public static void docRecover(MemcachedClient mc, String tid, String key, 
			int hashCode, MongoDbClientDelegate client, 
			Map<String, ByteIterator> changes, int action, byte[] read_buffer)
					throws DatabaseFailureException, IQException {    
		logger.debug("Perform recover document key="+key);

		CLValue val = mc.ewread(tid, key, hashCode, false);
		if (!val.isPending()) {
			CADSMongoDbClient.findNoBuff.incrementAndGet();
			return;
		}

		HashMap<String, ByteIterator> m = new HashMap<>();
//		if (val.getValue() != null) {
//			logger.debug("Got value.");
//			CacheUtilities.unMarshallHashMap(m, (byte[])val.getValue(), read_buffer);
//		}

		// get buffered writes
		String pwKey = TardisYCSBConfig.getPWLogKey(getUserId(key));
		Object pwVal = mc.get(pwKey, hashCode, false);
		if (pwVal != null) {
			logger.debug("Got buffered writes");
			CacheUtilities.unMarshallHashMap(m, (byte[])pwVal, read_buffer);
		} else {
			CADSMongoDbClient.findNoBuff.incrementAndGet();
			logger.debug("No buffered writes for key = "+pwKey);
		}

		if (m.size() == 0) {
			logger.debug("Both value and buffered writes do not exist.");
			return;
		} else {
			logger.debug("Updating mongo with buffered writes.");
			Status status = client.update(
					CoreWorkload.TABLENAME_PROPERTY_DEFAULT, key, m);
			if (status != Status.OK) {
				logger.debug("Update failed. Probably PStore still failed.");
				throw new DatabaseFailureException();
			} else {
				logger.debug("Update succeedded.");
			}
		}

		if (pwVal != null) {
			logger.debug("Delete the buffered write");
			try {
				mc.delete(pwKey, hashCode, null);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}    
		}
	}
}
