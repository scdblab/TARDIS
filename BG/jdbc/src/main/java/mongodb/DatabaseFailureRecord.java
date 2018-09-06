package mongodb;

import org.json.JSONException;
import org.json.JSONObject;

public class DatabaseFailureRecord extends ArrayMetricsRecord {

	private final long start;
	private final long end;
	
	public DatabaseFailureRecord(long start, long end) {
		super();
		this.start = start;
		this.end = end;
	}

	@Override
	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();
		try {
			obj.put("start", start);
			obj.put("end", end);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return obj;
	}

}
