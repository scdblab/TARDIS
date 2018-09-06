package mongodb;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.meetup.memcached.MemcachedClient;

import tardis.TardisClientConfig;

/**
 * A background thread that simulates database failures. Database is assumed to
 * have crashed during the provided intervals. Applications shouldn't contact
 * database if the {@link #failed} is true. Precision is second.
 * 
 * @author haoyuh
 *
 */
public class DatabaseStateSimulator implements Callable<Void> {

	private final AtomicBoolean failed;
	private final long[] invtervals;
	private final MemcachedClient memcachedClient;
	private int index;
	private long now;

	private boolean isRunning = true;
	public static volatile boolean modechanged=false;
	
	private final Logger logger = Logger.getLogger(DatabaseStateSimulator.class);

	/**
	 * @param failed
	 *            A flag indicating the status of database. True means database
	 *            has crashed. False means database is alive.
	 * @param invtervals
	 *            [start, end, start, end...]. Database is considered as failed
	 *            during the given interval. Start and end are relative time to
	 *            when this class is created. Time unit is second.
	 */
	public DatabaseStateSimulator(MemcachedClient memcachedClient, AtomicBoolean failed, long[] invtervals) {
		super();
		this.memcachedClient = memcachedClient;
		this.failed = failed;
		this.invtervals = invtervals;
		this.now = System.currentTimeMillis();
		System.out.println("created db state simulator");
	}

	@Override
	public Void call() throws Exception {
		
		if (MongoBGClientDelegate.DO_ACTUAL_KILL_DB) {
			return null;
		}
		
		while (isRunning) {
			long start = invtervals[index];
			long end = invtervals[index + 1];

			long now = (System.currentTimeMillis() - this.now) / 1000;

			if (now >= start && !failed.get() && !modechanged) {
				System.out.println("Crash at " + System.nanoTime());
				failed.set(true);
			} else if (now >= end) {
				System.out.println("Back at " + System.nanoTime());

				if (TardisClientConfig.measureLostWriteAfterFailure) {
					int count = RecoveryEngine.measureLostWrites(memcachedClient);
					MetricsHelper.numberOfPendingWritesAttheEndOfFailMode.set(count);
				}
				failed.set(false);
				modechanged = true;
				
				System.out.println("#########dbfailed " +  failed.get());
				index += 2;
				if (index >= invtervals.length) {
					break;
				}
			}
			Thread.sleep(1000);
		}
		return null;
	}

	public void shutdown() {
		isRunning = false;
	}

	public long[] getInvtervals() {
		return invtervals;
	}
}
