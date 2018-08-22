package com.yahoo.ycsb.db;

import java.util.concurrent.atomic.AtomicInteger;

public class TardisRecoveryEngine {
  public static final CountMetrics pendingWritesPerUserMetrics = new CountMetrics();
  public static final CountMetrics pendingWritesMetrics = new CountMetrics();
  public static final CountMetrics pendingWritesUsers = new CountMetrics();
  public static final CountMetrics recoveredUsers = new CountMetrics();
  
	public static final AtomicInteger successfulWriteCount = new AtomicInteger();

	public static void outputAndExit() {
		System.out.println("success write actions " + successfulWriteCount.get());
		System.out.println("byte to success write action ratio "
				+ (double) successfulWriteCount.get() / (double) TardisYCSBConfig.cacheMemoryMB);
		System.exit(0);
	}
}
