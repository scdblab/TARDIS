package mongodb;

import java.util.Map;

import org.json.JSONObject;

public class SlabStatsRecord extends ArrayMetricsRecord {

	private final Map<String, Map<String, Long>> slabStats;
	
	public SlabStatsRecord(Map<String, Map<String, Long>> slabStats) {
		super();
		this.slabStats = slabStats;
	}

	@Override
	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();
		obj.put("slabstats", slabStats);
		return obj;
	}

}
