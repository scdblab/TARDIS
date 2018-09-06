package mongodb.alpha;

import org.apache.log4j.Logger;

public class ExponentialBackoff {

	private final int maxBackoffTime;
	private final int baseBackoffTime;
	private int currentBackoff;

	private final Logger logger = Logger.getLogger(ExponentialBackoff.class);

	public ExponentialBackoff(int maxBackoffTime, int baseBackoffTime) {
		super();
		this.maxBackoffTime = maxBackoffTime;
		this.baseBackoffTime = baseBackoffTime;
		this.currentBackoff = this.baseBackoffTime;
	}

	public void backoff() {
		try {
			Thread.sleep(currentBackoff);
		} catch (InterruptedException e) {
			logger.error("failed to back-off", e);
		}
		currentBackoff = Math.min(currentBackoff * 2, this.maxBackoffTime);
	}

	public void reset() {
		this.currentBackoff = this.baseBackoffTime;
	}
	
	public static void main(String[] args) {
		ExponentialBackoff b = new ExponentialBackoff(1000, 100);
		System.out.println(b.currentBackoff);
		b.backoff();
		System.out.println(b.currentBackoff);
		b.backoff();
		System.out.println(b.currentBackoff);
		b.backoff();
		System.out.println(b.currentBackoff);
		b.backoff();
		System.out.println(b.currentBackoff);
		b.backoff();
		System.out.println(b.currentBackoff);
		b.reset();
		b.backoff();
		System.out.println(b.currentBackoff);
		
	}
}
