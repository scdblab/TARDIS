package mongodb.alpha;

public class AlphaDynamicPolicy extends AlphaPolicy {
	

	public AlphaDynamicPolicy(int baseAlpha) {
		super(baseAlpha);
	}

	@Override
	public int getAlpha(int numberOfDocs, int numberOfARWorkers) {
		if (numberOfDocs <= baseAlpha) {
			return numberOfDocs;
		}
		return Math.max(baseAlpha, numberOfDocs / numberOfARWorkers);
	}

}
