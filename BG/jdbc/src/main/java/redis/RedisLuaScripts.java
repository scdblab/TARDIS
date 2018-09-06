package redis;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;

public class RedisLuaScripts {
	// 3 args return [nil, 0, 0]
	public static String VIEW_PROFILE = "return {redis.call(\"GET\", KEYS[1]), redis.call(\"SCARD\", KEYS[2]), redis.call(\"SCARD\", KEYS[3])}";
	// 4 args return [[friend ids], [profiles], [friend counts]]
	public static String LIST_FRIENDS =
										"local friends = redis.call(\"SMEMBERS\", KEYS[1]);  " + 
										"local zero = 0;  " + 
										"local thisserver =  0; " + 
										"local totalservers =  tonumber(ARGV[2]); " + 
										"if tonumber(ARGV[2]) ~= 1 then " +
										"	thisserver=tonumber(ARGV[1]) % totalservers;" +
										"end; " +
										"if friends ~= nil then  " + 
										"	local fs={};  " + 
										"	local profileKeys = {};  " + 
										"	local friendsCount = {};  " + 
										"	local retrieved=1; " + 
										"	local remotefsindex=1; " + 
										"	local remotefs = {}; " + 
										"	for index,value in ipairs(friends) do  " + 
										"		if retrieved > tonumber(KEYS[2]) then  " + 
										"			break;  " + 
										"		end;  " + 
										"		if value=='t' then  " + 
										"			zero=1;  " + 
										"		end   " + 
										"		if value~='t' then " + 
										"			local server = 0; " + 
										"			if totalservers ~= 1 then " + 
										"				server = tonumber(value) % totalservers; " +
										"			end " + 
										"			if server == thisserver then " + 
										"				fs[retrieved]=value; " + 
										"				profileKeys[retrieved]=KEYS[3] .. value;  " + 
										"				local f=KEYS[4] .. value;  " + 
										"				friendsCount[retrieved]=redis.call(\"SCARD\", f);  " + 
										"				retrieved=retrieved+1;  " + 
										"			else " + 
										"				remotefs[remotefsindex]=value; " + 
										"				remotefsindex=remotefsindex+1; " + 
										"			end; " + 
										"		end;  " + 
										"	end;  " + 
										"	local mget={};  " + 
										"	if retrieved > 1 then  " + 
										"		mget=redis.call(\"MGET\", unpack(profileKeys)); " + 
										"	end; " +
										"	if retrieved > tonumber(KEYS[2]) then " + 
										"		return {fs, mget, friendsCount, {}}; 	 " + 
										"	elseif retrieved > 1 or remotefsindex > 1 then " + 
										"		return {fs, mget, friendsCount, remotefs} " + 
										"	end; " + 
										"	if zero==1 then  " + 
										"		return {{},{},{},{}}  " + 
										"	end;  " + 
										"	return nil  " + 
										"end  " + 
										"return nil";
		
	// 4 args. if one key is empty, return 0. otherwise, return 1;
	public static String ACCEPT_FRIEND_INVITEE = "local t1 = 0; if redis.call(\"EXISTS\", KEYS[1]) == 1 then t1 = 1; redis.call(\"SADD\", KEYS[1], KEYS[2]); end local t2 = 0; if redis.call(\"EXISTS\", KEYS[3]) == 1 then t2 = 1 redis.call(\"SREM\", KEYS[3], KEYS[4]); end if t1 == 0 or t2 == 0 then return 0 end return 1";

	/**
	 * return 0 if nonexist. return 1 otherwise. 
	 */
	public static String SREM_IF_EXISTS = "if redis.call(\"EXISTS\", KEYS[1]) == 0 then return 0 end; redis.call(\"SREM\", KEYS[1], KEYS[2]); return 1";
	
	/**
	 * return 0 if nonexist. return 1 otherwise.  
	 */
	public static String SADD_IF_EXIST = "if redis.call(\"EXISTS\", KEYS[1]) == 0 then return 0 end; redis.call(\"SADD\", KEYS[1], KEYS[2]); return 1";
	
	/**
	 * return 0 if exist.  
	 */
	public static String RPUSH_IF_NON_EXIST = "if redis.call(\"EXISTS\", KEYS[1]) == 0 then redis.call(\"RPUSH\", KEYS[1], KEYS[2]) return 1; end; return 0";
	
	public static String TWO_SET_GET = "local l1 = redis.call(\"SMEMBERS\", KEYS[1]); local l2 = redis.call(\"SMEMBERS\", KEYS[2]); return {l1, l2}";
	
	
	public static String RELEASE_LEASE = "if redis.call(\"GET\", KEYS[1]) == KEYS[2] then return redis.call(\"DEL\", KEYS[1]) end return 0";
	
	public static String MULTI_LIST_LLEN_2 = "local l2 = {}; local l1 = redis.call(\"SMEMBERS\", KEYS[1]); for key,value in ipairs({unpack(ARGV)}) do l2[key]=redis.call(\"LLEN\", value) end; return {l1, l2}";
	
	public static String MULTI_SET_SCARD = "local l2 = {}; for key,value in ipairs({unpack(KEYS)}) do l2[key]=redis.call(\"SCARD\", value) end; return l2";
	
	public static String MULTI_LIST_LLEN = "local l2 = {}; for key,value in ipairs({unpack(KEYS)}) do l2[key]=redis.call(\"LLEN\", value) end; return l2";
	
	/**
	 * return 0 if first time.
	 */
	public static String LIST_APPEND_FIRST_TIME_CHECK = "local first = redis.call(\"EXISTS\", KEYS[1]); redis.call(\"RPUSH\", KEYS[1], KEYS[2]) return first";

	public static List<String> luaScripts = new ArrayList<>();

	static {
		luaScripts.add(VIEW_PROFILE);
		luaScripts.add(LIST_FRIENDS);
		luaScripts.add(ACCEPT_FRIEND_INVITEE);
		luaScripts.add(SREM_IF_EXISTS);
		luaScripts.add(SADD_IF_EXIST);
		luaScripts.add(RPUSH_IF_NON_EXIST);
		luaScripts.add(TWO_SET_GET);
		luaScripts.add(RELEASE_LEASE);
		luaScripts.add(MULTI_SET_SCARD);
		luaScripts.add(MULTI_LIST_LLEN_2);
		luaScripts.add(MULTI_LIST_LLEN);
		luaScripts.add(LIST_APPEND_FIRST_TIME_CHECK);
	}

	public static void main(String[] ab) {
//		viewProfile();
		testListFriend();
//		acceptFriendInvitee();
//		removeIfExist();
//		twoSetGet();
//		multiSetScard();
//		multiSetScard2();
//		SADD_IF_EXIST();
	}
	
	private static void SADD_IF_EXIST() {
		System.out.println("SADD_IF_EXIST");
		Jedis client = new Jedis();

		client.scriptFlush();
		client.flushAll();

		String hash = client.scriptLoad(SADD_IF_EXIST);
		System.out.println(hash);
		client.sadd("a", "t1");

		List<String> keys = new ArrayList<>();
		keys.add("a");
		keys.add("b");
		List<String> args = new ArrayList<>();

		System.out.println(client.evalsha(hash, keys, args));
		
		System.out.println(client.smembers("a"));

		client.close();
	}
	
	private static void multiSetScard2() {
		System.out.println("multiSetScard2");
		Jedis client = new Jedis();

		client.scriptFlush();
		client.flushAll();

		String hash = client.scriptLoad(MULTI_LIST_LLEN_2);
		System.out.println(hash);
		client.sadd("a", "t1");

		List<String> keys = new ArrayList<>();
		keys.add("a");
		List<String> args = new ArrayList<>();
		args.add("a");
		args.add("b");
		args.add("c");

		System.out.println(client.evalsha(hash, keys, args));

		client.close();
	}
	
	private static void multiSetScard() {
		System.out.println("multiSetScard");
		Jedis client = new Jedis();

		client.scriptFlush();
		client.flushAll();

		String hash = client.scriptLoad(MULTI_SET_SCARD);
		System.out.println(hash);
		client.sadd("a", "t1");

		List<String> keys = new ArrayList<>();
		keys.add("a");
		keys.add("t1");

		System.out.println((List<Integer>)client.evalsha(hash, keys, keys));

		client.close();
	}
	
	private static void twoSetGet() {
		System.out.println("twoSetGet");
		Jedis client = new Jedis();

		client.scriptFlush();
		client.flushAll();

		String hash = client.scriptLoad(TWO_SET_GET);
		System.out.println(hash);
		client.sadd("a", "t1");

		List<String> keys = new ArrayList<>();
		keys.add("a");
		keys.add("t1");

		System.out.println(client.evalsha(hash, keys, new ArrayList<>()));

		client.close();
	}

	private static void removeIfExist() {
		System.out.println("removeIfExist");
		Jedis client = new Jedis();

		client.scriptFlush();
		client.flushAll();

		String hash = client.scriptLoad(SREM_IF_EXISTS);
		System.out.println(hash);
		client.sadd("a", "t1");

		List<String> keys = new ArrayList<>();
		keys.add("a");
		keys.add("t1");

		System.out.println(client.evalsha(hash, keys, new ArrayList<>()));

		System.out.println(client.smembers("a"));
		client.close();
	}
	
	private static void acceptFriendInvitee() {
		System.out.println("acceptFriendInvitee");
		Jedis client = new Jedis();

		client.scriptFlush();
		client.flushAll();

		String hash = client.scriptLoad(ACCEPT_FRIEND_INVITEE);
		System.out.println(hash);
		client.sadd("a", "t1");
		client.sadd("b", "10");
		client.sadd("b", "a");

		List<String> keys = new ArrayList<>();
		keys.add("a");
		keys.add("a1");
		keys.add("b");
		keys.add("10");

		System.out.println(client.evalsha(hash, keys, new ArrayList<>()));

		System.out.println(client.smembers("a"));
		System.out.println(client.smembers("b"));
		client.close();
	}

	private static void viewProfile() {
		Jedis client = new Jedis();

		client.scriptFlush();
		client.flushAll();

		String hash = client.scriptLoad(VIEW_PROFILE);
		System.out.println(hash);

		client.sadd("a1", "b");

		List<String> keys = new ArrayList<>();
		keys.add("a");
		keys.add("a1");
		keys.add("10");

		System.out.println(client.evalsha(hash, keys, new ArrayList<>()));

		List<?> a = (List<?>) (client.evalsha(hash, keys, new ArrayList<>()));
		System.out.println(a);

		client.close();
	}

	private static void testListFriend() {
		
		System.out.println("testListFriend");
		
		Jedis client = new Jedis("127.0.0.1", 11211);

		client.scriptFlush();
//		client.flushAll();

		String hash = client.scriptLoad(LIST_FRIENDS);
		System.out.println(hash);

		for (int i = 0; i < 100; i++) {
			if (i <= 10) {
				client.set("V" + String.valueOf(i), "V" + i);
			}
			client.sadd("a", String.valueOf(i));
		}
		client.sadd("a", "t");
		client.sadd("LF0", "t");
		client.sadd("b", "t");
		client.sadd("b", "0");
		client.sadd("b", "1000000");
		client.sadd("b", "2");
		client.sadd("b", "1001");
		client.sadd("b", "102");
		
		// V0, ... V10 -> //
		// a -> (0,...100,t)
		// LF0 -> t
		// b -> (t)

		List<String> keys = new ArrayList<>();
		keys.add("LF2");
		keys.add("100000");
		keys.add("V");
		keys.add("LF");
		List<String> args = new ArrayList<>();
		args.add("2");
		args.add("2");
		

		System.out.println(client.evalsha(hash, keys, args));

		client.close();
	}
}
