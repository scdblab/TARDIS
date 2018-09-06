package mongodb;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class RecoverRecord extends ArrayMetricsRecord {
	private final long start;
	private final long end;
	private final int workerID;
	private final Map<String, Integer> dirtyRecords;
	private final Map<String, Integer> recoveredRecords;

	public RecoverRecord(long start, long end, int workerID, Map<String, Integer> dirtyRecords,
			Map<String, Integer> recoveredRecords) {
		super();
		this.start = start;
		this.end = end;
		this.workerID = workerID;
		this.dirtyRecords = dirtyRecords;
		this.recoveredRecords = recoveredRecords;
	}

	public long getStart() {
		return start;
	}

	public long getEnd() {
		return end;
	}

	public int getWorkerID() {
		return workerID;
	}

	public Map<String, Integer> getDirtyRecords() {
		return dirtyRecords;
	}

	public Map<String, Integer> getRecoveredRecords() {
		return recoveredRecords;
	}

	public String toString() {
		return toJSON().toString();
	}

	@Override
	public JSONObject toJSON() {
		JSONObject obj = new JSONObject();
		try {
			obj.put("start", this.start);
			obj.put("end", this.end);
			obj.put("workerId", this.workerID);
			obj.put("dirty", toJSON(this.dirtyRecords));
			obj.put("recovered", toJSON(this.recoveredRecords));
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return obj;
	}
	
	public static void main(String[] args) {
		Map<String, Integer> rec = new HashMap<>();
		rec.put("a", 2);
		RecoverRecord r = new RecoverRecord(100, 10000, 1, rec, rec);
		System.out.println(r.toJSON().toString());
	}
}
