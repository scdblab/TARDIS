package com.yahoo.ycsb.db;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONException;
import org.json.JSONObject;

public class MetricsHelper {

	public static AtomicInteger numberOfPendingWritesAttheEndOfFailMode = new AtomicInteger(0);
	public static AtomicInteger numberOfPendingWritesAttheEndOfRecoveryMode = new AtomicInteger(0);

	public static String writeMetrics(String fileName, Properties prop, long start, long end, long dbRecoverTime,
			TimedMetrics timedMetrics, ArrayMetrics arrayMetrics) {
		JSONObject obj = new JSONObject();
		try {
			obj.put("start", start);
			obj.put("end", end);

			obj.put(MetricsName.METRICS_TOTAL_PENDING_WRITES_FAIL, numberOfPendingWritesAttheEndOfFailMode.get());
			obj.put(MetricsName.METRICS_TOTAL_PENDING_WRITES_RECOVERY,
					numberOfPendingWritesAttheEndOfRecoveryMode.get());

			if (TardisRecoveryEngine.pendingWritesMetrics.getMetrics().containsKey(MetricsName.METRICS_PENDING_WRITES)
					&& TardisRecoveryEngine.pendingWritesMetrics.getMetrics()
							.containsKey(MetricsName.METRICS_RECOVERED_WRITES_COUNT)) {
				int totalPWs = TardisRecoveryEngine.pendingWritesMetrics.getMetrics().get(MetricsName.METRICS_PENDING_WRITES)
						.get();
				obj.put(MetricsName.METRICS_TOTAL_PENDING_WRITES, totalPWs);
				obj.put(MetricsName.METRICS_RECOVERED_WRITES_COUNT, TardisRecoveryEngine.pendingWritesMetrics.getMetrics()
						.get(MetricsName.METRICS_RECOVERED_WRITES_COUNT).get());
			}

			List<ArrayMetricsRecord> array = ArrayMetrics.getInstance().getMetricArray()
					.get(MetricsName.METRICS_EW_STATS);
			int peak = 0;
			long peakTime = 0;
			long recoveryFinishTime = 0;
			for (int i = 0; i < array.size(); i++) {
				RecoverRecord r = RecoverRecord.class.cast(array.get(i));
				int sum = r.getDirtyRecords().values().stream().mapToInt(Integer::intValue).sum();
				if (sum >= peak) {
					peakTime = r.getEnd();
					peak = sum;
				}
			}

			if (peakTime < dbRecoverTime + start) {
				peakTime = dbRecoverTime + start;
				System.out.println("peak time less than actual db recovery time, use actual recover time " + peakTime
						+ ", " + dbRecoverTime + start);
			}

			for (int i = 0; i < array.size(); i++) {
				RecoverRecord r = RecoverRecord.class.cast(array.get(i));
				if (r.getStart() > peakTime) {
					if (r.getDirtyRecords().values().stream().mapToInt(Integer::intValue).sum() == 0) {
						recoveryFinishTime = r.getStart();
						break;
					}
				}
			}

			if (recoveryFinishTime == 0) {
				recoveryFinishTime = RecoverRecord.class.cast(array.get(array.size() - 1)).getEnd();
			}
			obj.put(MetricsName.METRICS_DIRTY_USER, TardisRecoveryEngine.pendingWritesUsers.getMetrics().size());
			obj.put(MetricsName.METRICS_RECOVERED_USER, TardisRecoveryEngine.recoveredUsers.getMetrics().size());
			obj.put(MetricsName.METRICS_MISSED_USER, TardisRecoveryEngine.pendingWritesUsers.getMetrics().size() - TardisRecoveryEngine.recoveredUsers.getMetrics().size());
			
			obj.put(MetricsName.METRICS_RECOVERY_DURATION, recoveryFinishTime - peakTime);

			System.out.println(obj.toString());

			timedMetrics.getMetrics().keySet().forEach(i -> {
				try {
					obj.put(i, timedMetrics.getMetricsAsJson(i));
				} catch (Exception e) {
					e.printStackTrace();
				}
			});

			arrayMetrics.getMetricArray().keySet().forEach(i -> {
				try {
					obj.put(i, arrayMetrics.toJSON(i));
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		} catch (JSONException e) {
			e.printStackTrace();
		}

		try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(fileName)))) {
			bw.write(obj.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			return obj.toString(4);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static void put(String key, Properties prop, JSONObject obj) throws JSONException {
		if (prop.getProperty(key) != null) {
			obj.put(key, prop.getProperty(key));
		}
	}
}
