package com.yahoo.ycsb.db;

import java.util.concurrent.atomic.AtomicBoolean;

public class LockManager {
	private AtomicBoolean[] locks;

	public LockManager(int numLocks) {
		locks = new AtomicBoolean[numLocks];
		for (int i = 0; i < locks.length; i++) {
			locks[i] = new AtomicBoolean(false);
		}
	}

	public void acquire(String key) {
		int index = index(key);
		while (!locks[index].compareAndSet(false, true))
			;
	}

	public void release(String key) {
		int index = index(key);
		locks[index].set(false);
	}

	private int index(String key) {
		int index = key.hashCode() % locks.length;
		if (index == Integer.MIN_VALUE) {
			return 0;
		}
		return Math.abs(index);
	}
}
