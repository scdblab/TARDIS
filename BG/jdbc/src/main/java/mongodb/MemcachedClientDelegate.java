package mongodb;

import static tardis.TardisClientConfig.KEY_EVENTUAL_WRITE_LOG;
import static tardis.TardisClientConfig.KEY_USER_PENDING_WRITES_LOG;
import static tardis.TardisClientConfig.LEASE_KEY_EVENTUAL_WRITES_LOG_MUTATION;
import static tardis.TardisClientConfig.LEASE_KEY_USER_PENDING_WRITES_LOG_MUTATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.meetup.memcached.CASResponse;
import com.meetup.memcached.CASValue;
import com.meetup.memcached.ErrorHandler;
import com.meetup.memcached.MemcachedClient;

public class MemcachedClientDelegate extends MemcachedClient {

	private static final Map<String, Long> KEY_COST_MAP = new HashMap<>();
	private static List<String> KEYS = new ArrayList<>();
	private static long DEFAULT_COST = 100000;

	static {
		KEY_COST_MAP.put(KEY_EVENTUAL_WRITE_LOG, 65536000l);
		KEY_COST_MAP.put(KEY_USER_PENDING_WRITES_LOG, 65536000l);
		KEY_COST_MAP.put(LEASE_KEY_EVENTUAL_WRITES_LOG_MUTATION, 400000l);
		KEY_COST_MAP.put(LEASE_KEY_USER_PENDING_WRITES_LOG_MUTATION, 400000l);
		KEYS = new ArrayList<>(KEY_COST_MAP.keySet());
		Collections.sort(KEYS, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return o2.length() - o1.length();
			}
		});
	}
	
	private final Logger logger = Logger.getLogger(MemcachedClientDelegate.class);

	public static long getCost(String key) {
		for (String costKey : KEYS) {
			if (key.startsWith(costKey)) {
				return KEY_COST_MAP.get(costKey);
			}
		}
		return DEFAULT_COST;
	}

	public MemcachedClientDelegate() {
		super();
	}

	public MemcachedClientDelegate(ClassLoader classLoader, ErrorHandler errorHandler, String poolName) {
		super(classLoader, errorHandler, poolName);
	}

	public MemcachedClientDelegate(ClassLoader classLoader, ErrorHandler errorHandler) {
		super(classLoader, errorHandler);
	}

	public MemcachedClientDelegate(ClassLoader classLoader) {
		super(classLoader);
	}

	public MemcachedClientDelegate(String poolName) {
		super(poolName);
	}

	@Override
	public Object get(String key) {
		Object get = super.get(key);
		return get;
	}

	@Override
	public Object get(String key, Integer hashCode, boolean asString) {
		Object get = super.get(key, hashCode, asString);
		return get;
	}

	@Override
	public CASValue gets(String key) {
		CASValue get = super.gets(key);
		return get;
	}

	@Override
	public Map<String, Object> getsMulti(String[] keys) {
		Map<String, Object> gets = super.getsMulti(keys);
		if (gets.size() > keys.length) {
			logger.fatal("BUG: MultiGets return more keys than expected. Returned keys: " + gets.keySet()
					+ " requests keys: " + keys);
		}
		return gets;
	}

	@Override
	public Map<String, Object> getMulti(String[] keys) {
		Map<String, Object> gets = super.getMulti(keys);
		if (gets.size() > keys.length) {
			logger.fatal("BUG: MultiGets return more keys than expected. Returned keys: " + gets.keySet()
					+ " requests keys: " + keys);
		}
		return gets;
	}

	@Override
	public Map<String, Object> getMulti(String[] keys, Integer[] hashCodes) {
		Map<String, Object> gets = super.getMulti(keys, hashCodes);
		if (gets.size() > keys.length) {
			logger.fatal("BUG: MultiGets return more keys than expected. Returned keys: " + gets.keySet()
					+ " requests keys: " + keys);
		}
		return gets;
	}

	@Override
	public boolean set(String key, Object value) throws IOException {
		if (MemcachedMongoBGClient.useCAMP) {
			long cost = getCost(key);
			return super.cset(key, null, value, cost);
		}
		return super.set(key, value);
	}

	@Override
	public boolean append(String key, Object value) throws IOException {
		if (MemcachedMongoBGClient.useCAMP) {
			long cost = getCost(key);
			return super.cappend(key, value, cost);
		}
		return super.append(key, value);
	}

	@Override
	public boolean add(String key, Object value) throws IOException {
		if (MemcachedMongoBGClient.useCAMP) {
			long cost = getCost(key);
			return super.cadd(key, value, cost);
		}
		return super.add(key, value);
	}

	@Override
	public CASResponse cas(String key, Object value, Date expiry, long casToken) {
		if (MemcachedMongoBGClient.useCAMP) {
			long cost = getCost(key);
			return super.ccas(key, value, expiry, casToken, cost);
		}
		return super.cas(key, value, expiry, casToken);
	}

	@Override
	public CASResponse cas(String key, Object value, long casToken) throws IOException {
		if (MemcachedMongoBGClient.useCAMP) {
			long cost = getCost(key);
			return super.ccas(key, value, casToken, cost);
		}
		return super.cas(key, value, casToken);
	}
}