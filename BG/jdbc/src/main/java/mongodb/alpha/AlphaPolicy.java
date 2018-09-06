package mongodb.alpha;

public class AlphaPolicy {
	public static enum AlphaPolicyEnum {
		DYNAMIC, STATIC;
		
		public static AlphaPolicyEnum fromString(String policy) {
			return valueOf(policy.toUpperCase());
		}
	}

	final int baseAlpha;

	public AlphaPolicy(int baseAlpha) {
		super();
		this.baseAlpha = baseAlpha;
	}

	public int getAlpha(int numberOfDocs, int numberOfARWorkers) {
		return this.baseAlpha;
	}
}
