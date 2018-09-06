package mongodb;

import java.util.Map;

import org.json.JSONObject;

public abstract class ArrayMetricsRecord {

	public static JSONObject toJSON(Map<?, ?> map) {
		JSONObject obj = new JSONObject();
		map.forEach((k, v) -> {
			try {
				obj.put(k.toString(), v);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		return obj;
	}

	public abstract JSONObject toJSON();
}
