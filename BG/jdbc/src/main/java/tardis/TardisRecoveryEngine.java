package tardis;

import mongodb.CountMetrics;

public class TardisRecoveryEngine {
	public static final CountMetrics pendingWritesPerUserMetrics = new CountMetrics();
	public static final CountMetrics pendingWritesMetrics = new CountMetrics();
	public static final CountMetrics pendingWritesUsers = new CountMetrics();
	public static final CountMetrics recoveredUsers = new CountMetrics();
}
