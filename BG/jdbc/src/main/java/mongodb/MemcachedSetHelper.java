package mongodb;

import static tardis.TardisClientConfig.DELIMITER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;

import com.meetup.memcached.MemcachedClient;

import tardis.TardisClientConfig;

public class MemcachedSetHelper {

	private static final int JITTER_BASE = 10;
	private static final int JITTER_RANGE = 20;
	private static final Random random = new Random();

	private static Logger logger = Logger.getLogger(MemcachedSetHelper.class);
	
	public static List<Set<String>> convertSetPW(String input) {
		if (input == null) return null;
		String[] strs = input.split(":", -1);
		assert (strs.length == 2);
		List<Set<String>> list = new ArrayList<>();
		Set<String> add = new HashSet<>();
		String[] ids = strs[0].split(DELIMITER);
		for (String id: ids) add.add(id);
		list.add(add);
		Set<String> rmv = new HashSet<>();
		ids = strs[1].split(DELIMITER);
		for (String id: ids) rmv.add(id);
		list.add(rmv);
		return list;
	}
	
	public static String convertStringPW(Set<String> add, Set<String> rmv) {
		StringBuilder sb = new StringBuilder();
		for (String s: add) sb.append(s+DELIMITER);
		if (sb.length() != 0)
			sb.setCharAt(sb.length()-1, ':');
		else
			sb.append(':');
		for (String s: rmv) sb.append(s+DELIMITER);
		if (sb.charAt(sb.length()-1) == ',')
			sb.setLength(sb.length()-1);
		return sb.toString();
	}

	public static Set<String> convertSet(Object obj) {
		if (obj == null) {
			return null;
		}
		String strVal = String.valueOf(obj);
    if (strVal.contains(":")) 
      strVal = strVal.substring(strVal.indexOf(":")+1);
    
//		if (strVal.charAt(0) == ',') {
//			strVal = strVal.substring(1);
//		}
//		if (strVal.length() == 0) {
//			return Collections.emptySet();
//		}
		String[] vals = strVal.split(DELIMITER);
		Set<String> set = new HashSet<>();
		for (String val : vals) {
		  if (!val.isEmpty())
		    set.add(val);
		}
		return set;
	}

	public static List<String> convertList(Object obj) {
		if (obj == null) {
			return null;
		}
		String strVal = String.valueOf(obj);
		
		if (strVal.contains(":")) 
		  strVal = strVal.substring(strVal.indexOf(":")+1);
		
//		if (strVal.charAt(0) == ',') {
//			strVal = strVal.substring(1);
//		}
//		if (strVal.length() == 0) {
//			return Collections.emptyList();
//		}
		String[] vals = strVal.split(DELIMITER);
		List<String> list = new ArrayList<>();
		for (String val : vals) {
		  if (!val.isEmpty())
		    list.add(val);
		}
		return list;
	}

	public static String convertString(Collection<String> list) {
		if (list.isEmpty()) {
			return DELIMITER;
		}

		StringBuilder builder = new StringBuilder();
		list.stream().forEach(i -> {
			builder.append(i);
			builder.append(DELIMITER);
		});
		return builder.toString();
	}

	public static boolean addToSet(MemcachedClient memcachedClient, 
	    String key, Integer hashCode, 
	    String valueToAdd,
			boolean addIfAppendFailed) {
		boolean ret = false;
		try {
			ret = memcachedClient.append(key, hashCode, valueToAdd + TardisClientConfig.DELIMITER);
			if (!ret && addIfAppendFailed) {
				ret = memcachedClient.add(key, valueToAdd + TardisClientConfig.DELIMITER, hashCode);
			}
		} catch (Exception e) {
			logger.error("Failed to add key to set. key: " + key + " value: " + valueToAdd, e);
			return ret;
		}
		return ret;
	}

	public static void set(MemcachedClient memcachedClient, String key, Integer hashCode, Set<String> list) {
		try {
			StringBuilder builder = new StringBuilder();
			builder.append(",");
			for (String value : list) {
				builder.append(value);
				builder.append(DELIMITER);
			}
			memcachedClient.set(key, builder.toString(), hashCode);
		} catch (Exception e) {
			logger.error(e);
		}
	}

	/**
	 * @param memcachedClient
	 * @param key
	 * @param valueToAdd
	 * @param addIfAppendFailed
	 * @return the result of append, true if key exists and append success.
	 *         false means key doesn't exist.
	 */
	public static boolean addToSetAppend(MemcachedClient memcachedClient, String key, Integer hashCode,
	    String valueToAdd,
			boolean addIfAppendFailed) {
		boolean appendRet = false;
		boolean addRet = false;
		try {
			appendRet = memcachedClient.append(key, hashCode, valueToAdd + TardisClientConfig.DELIMITER);
			if (!appendRet && addIfAppendFailed) {
				addRet = memcachedClient.add(key, valueToAdd + TardisClientConfig.DELIMITER, hashCode);
				return appendRet;
			}
		} catch (Exception e) {
			logger.error("Failed to add key to set. key: " + key + " value: " + valueToAdd, e);
			return appendRet;
		} finally {
			if (appendRet == false && addRet == false) {
				System.out.println("BUG!!!" + key);
			}
		}
		return appendRet;
	}

	public static boolean removeFromSet(MemcachedClient memcachedClient, 
	    String key, Integer hashCode, String valueToRemove) {
		
		Object val = memcachedClient.get(key, hashCode, true);
		if (val == null) {
			return false;
		}
		Set<String> values = convertSet(val);
		if (!values.remove(valueToRemove)) {
			logger.fatal("BUG: key " + key + " val: " + val + " " + values + " " + valueToRemove);
		}
		try {
			return memcachedClient.set(key, convertString(values), hashCode);
		} catch (Exception e) {
			logger.error("Failed to remove value from set. key: " + key + " value: " + valueToRemove, e);
		}
		
		return false;
//		try {
//			do {
//				Object val = memcachedClient.get(key);
//				if (val == null) {
//					return false;
//				}
//				Set<String> values = convertSet(val);
//				if (!values.remove(valueToRemove)) {
//					logger.fatal("BUG: key " + key + " val: " + val + " " + values + " " + valueToRemove);
//				}
//				response = memcachedClient.set(key, convertString(values));
//				if (!CASResponse.SUCCESS.equals(response)) {
//					Thread.sleep(JITTER_BASE + random.nextInt(JITTER_RANGE));
//				} else {
//					break;
//				}
//
//			} while (true);
//		} catch (Exception e) {
//			logger.error("Failed to remove value from set. key: " + key + " value: " + valueToRemove, e);
//		}

//		return true;
	}
	
	static void printList(List<Set<String>> list) {
		System.out.println("=======");
		for (Set<String> set: list) {
			for (String s: set) {
				System.out.print(s+" ");
			}
			System.out.println();
		}
	}
	
	public static void main(String[] args) {
		printList(convertSetPW(":"));
		printList(convertSetPW("2,3:"));
		printList(convertSetPW(":3,2,1"));
		printList(convertSetPW("1:"));
		printList(convertSetPW(":2"));
		
		System.out.println("Test Set to String");
		System.out.println(convertStringPW(
				new HashSet<>(Arrays.asList("1", "2")), 
				new HashSet<>(Arrays.asList())));
		System.out.println(convertStringPW(
				new HashSet<>(Arrays.asList()), 
				new HashSet<>(Arrays.asList())));
		System.out.println(convertStringPW(
				new HashSet<>(Arrays.asList("1")), 
				new HashSet<>(Arrays.asList())));
		System.out.println(convertStringPW(
				new HashSet<>(Arrays.asList()), 
				new HashSet<>(Arrays.asList("2"))));
		System.out.println(convertStringPW(
				new HashSet<>(Arrays.asList("1", "2")), 
				new HashSet<>(Arrays.asList("3", "4"))));
		System.out.println(convertStringPW(
				new HashSet<>(Arrays.asList()), 
				new HashSet<>(Arrays.asList("3", "4"))));
	}
}
