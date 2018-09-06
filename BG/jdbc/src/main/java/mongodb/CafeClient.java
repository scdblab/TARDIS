package mongodb;

import static tardis.TardisClientConfig.ACTION_PW_FRIENDS;
import static tardis.TardisClientConfig.ACTION_PW_PENDING_FRIENDS;
import static tardis.TardisClientConfig.PAGE_SIZE_FRIEND_LIST;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.log4j.Logger;

import com.meetup.memcached.SockIOPool;
import com.usc.dblab.cafe.WriteBack;
import com.usc.dblab.cafe.CacheEntry;
import com.usc.dblab.cafe.CachePolicy;
import com.usc.dblab.cafe.CacheStore;
import com.usc.dblab.cafe.Cafe;
import com.usc.dblab.cafe.Change;
import com.usc.dblab.cafe.QueryResult;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.StringByteIterator;
import tardis.TardisClientConfig;

public class CafeClient extends DB {
	private Cafe cafe;
	static DB client;
	
	private static Semaphore initLock = new Semaphore(1);
	private static boolean isInit = false;
	
	public static final boolean LIMIT_FRIENDS = true;
	
	private static String[] serverlist;
	private static boolean fullWarmUp = true;
	private static int numUserInCache = -1;
	
	protected static boolean writeBack = false;
	private static final boolean disjoint = true;
	private static int machineid;
	
	public static int NUM_ACTIVE_RECOVERY_WORKERS = 0;
	
	private static AtomicInteger threads = new AtomicInteger(0);
	
	private final static Logger logger = Logger.getLogger(CafeClient.class);
	
	@Override
	public boolean init() throws DBException {
		try {
			initLock.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		if (!isInit) {
			System.out.println(String.format("MongoDB IP: %s", getProperties().getProperty("mongoip")));
			System.out.println(String.format("Twemcached IP: %s", getProperties().getProperty("twemcachedip")));

			if (getProperties().getProperty("limitfriends") != null) {
				PAGE_SIZE_FRIEND_LIST = Integer.parseInt(getProperties().getProperty("limitfriends"));
			}
			System.out.println("Limit friends = " + LIMIT_FRIENDS);
			System.out.println("Page Size Friend list = " + PAGE_SIZE_FRIEND_LIST);
			
			if (getProperties().getProperty(ReconConfig.KEY_FULL_WARM_UP) != null) {
				fullWarmUp = Boolean.parseBoolean(getProperties().getProperty(ReconConfig.KEY_FULL_WARM_UP));
			}
			if (getProperties().getProperty(ReconConfig.KEY_NUM_USER_IN_CACHE) != null) {
				numUserInCache = Integer.parseInt(getProperties().getProperty(ReconConfig.KEY_NUM_USER_IN_CACHE));
			}

			if (getProperties().getProperty("machineid") != null) {
				machineid = Integer.parseInt(getProperties().getProperty("machineid"));
			}

			if (getProperties().getProperty("writeback") != null) {
				writeBack = Boolean.parseBoolean(getProperties().getProperty("writeback"));				
			}
			
			String[] originServerlist = getProperties().getProperty("twemcachedip").split(",");

			int numCacheServers = originServerlist.length;
			if (getProperties().getProperty("numcacheservers") != null) {
				numCacheServers = Integer.parseInt(getProperties().getProperty("numcacheservers"));
				numCacheServers = numCacheServers > originServerlist.length ?
						originServerlist.length : numCacheServers;
			}

			int cps = Integer.parseInt(getProperties().getProperty("cachesperserver"));
			serverlist = new String[numCacheServers*cps];
			for (int i = 0; i < numCacheServers; i++) {
				if (!originServerlist[i].contains(":")) {
					int port = 11211;
					for (int j = 0; j < cps; j++) {
						serverlist[i*cps+j] = originServerlist[i] + ":" + port;
						port++;
					}
				} else {
					serverlist[i] = originServerlist[i];
				}
			}

			System.out.println("Cache Servers: "+ Arrays.toString(serverlist));
			System.out.println("Num of EWs: "+ TardisClientConfig.NUM_EVENTUAL_WRITE_LOGS);
			
			SockIOPool pool = SockIOPool.getInstance( "BG" );
			pool.setServers( serverlist );
			//pool.setWeights( weights );

			pool.setInitConn(100);
			pool.setMinConn(100);
			pool.setMaxConn( 200 );
			pool.setMaintSleep(0);
			pool.setNagle( false );
			//			pool.setHashingAlg( SockIOPool.CONSISTENT_HASH );
			pool.initialize();

			if (client == null) {
				client = new MongoBGClientDelegate(getProperties().getProperty("mongoip"), new AtomicBoolean(false));
				client.setProperties(getProperties());
				client.init();
			}
			
			if (getProperties().getProperty("numarworker") != null) {
				NUM_ACTIVE_RECOVERY_WORKERS = Integer.parseInt(getProperties().getProperty("numarworker"));
			}

			int alpha = 0;
			if (getProperties().getProperty("alpha") != null) {
				alpha = Integer.parseInt(getProperties().getProperty("alpha"));
				System.out.println("Alpha = "+alpha);
			}
		}
		
		isInit  = true;

		initLock.release();

		String[] serverlist = { getProperties().getProperty(ReconConfig.KEY_TWEMCACHED_IP) + ":11211" };
		threads.incrementAndGet();
		
		BGCacheStore cacheStore = new BGCacheStore(client);
		BGCacheBack cacheBack = new BGCacheBack(client);
		
		cafe = new Cafe(cacheStore, cacheBack, "BG", 
		    CachePolicy.WRITE_BACK, NUM_ACTIVE_RECOVERY_WORKERS);
		
		return true;
	}
	
	@Override
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values,
			boolean insertImage) {
		return client.insertEntity(entitySet, entityPK, values, insertImage);
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
			boolean insertImage, boolean testMode) {
		String query = BGCacheStore.ACT_VIEW_PROFILE+","+requesterID+","+profileOwnerID;
		Set<CacheEntry> entries = cafe.read(query);
		for (CacheEntry entry: entries) {
			if (entry.getKey().contains(BGCacheStore.KEY_PRE_PROFILE)) {
				@SuppressWarnings("unchecked")
				HashMap<String, ByteIterator> res = (HashMap<String, ByteIterator>)entry.getValue();
				result.putAll(res);
			} else if (entry.getKey().contains(BGCacheStore.KEY_PRE_FRIEND_COUNT)) {
				result.put("friendcount", new StringByteIterator((String)entry.getValue()));
			} else if (entry.getKey().contains(BGCacheStore.KEY_PRE_PENDING_COUNT)) {
				result.put("pendingcount", new StringByteIterator((String)entry.getValue()));
			}
		}
		return 0;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
		String query = BGCacheStore.ACT_LIST_FRIENDS+","+requesterID+","+profileOwnerID;
		Set<CacheEntry> entries = cafe.read(query);
		CacheEntry entry = entries.toArray(new CacheEntry[0])[0];
		@SuppressWarnings("unchecked")
    Set<String> friends = MemcachedSetHelper.convertSet((String)entry.getValue());
		for (String friend: friends) {
			HashMap<String, ByteIterator> map = new HashMap<>();
			viewProfile(requesterID, Integer.parseInt(friend), map, insertImage, testMode);
			result.add(map);
		}
		return 0;
	}

	@Override
	public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		String query = BGCacheStore.ACT_LIST_PENDING_FRIENDS+","+profileOwnerID+","+profileOwnerID;
		Set<CacheEntry> entries = cafe.read(query);
		CacheEntry entry = entries.toArray(new CacheEntry[0])[0];
		@SuppressWarnings("unchecked")
    Set<String> pfriends = MemcachedSetHelper.convertSet((String)entry.getValue());
		for (String pfriend: pfriends) {
			HashMap<String, ByteIterator> map = new HashMap<>();
			viewProfile(profileOwnerID, Integer.parseInt(pfriend), map, insertImage, testMode);
			results.add(map);
		}
		return 0;
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		String dml = BGCacheStore.ACT_ACP+","+inviterID+","+inviteeID;
		boolean success = cafe.write(dml);
		return success ? 0 : -1;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		String dml = BGCacheStore.ACT_REJECT+","+inviterID+","+inviteeID;
		boolean success = cafe.write(dml);
		return success ? 0 : -1;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		String dml = BGCacheStore.ACT_INV+","+inviterID+","+inviteeID;
		boolean success = cafe.write(dml);
		return success ? 0 : -1;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		return client.viewTopKResources(requesterID, profileOwnerID, k, result);
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		String dml = BGCacheStore.ACT_THAW+","+friendid1+","+friendid2;
		boolean success = cafe.write(dml);
		return success ? 0 : -1;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		return client.getInitialStats();
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		return client.CreateFriendship(friendid1, friendid2);
	}

	@Override
	public void createSchema(Properties props) {
		client.createSchema(props);
	}

	@Override
	public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
		return client.queryPendingFriendshipIds(memberID, pendingIds);
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
		return client.queryConfirmedFriendshipIds(memberID, confirmedIds);
	}
	
	@Override
	public int getCreatedResources(int creatorID, Vector<HashMap<String, ByteIterator>> result) {
		return client.getCreatedResources(creatorID, result);
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID,
			Vector<HashMap<String, ByteIterator>> result) {
		return client.viewCommentOnResource(requesterID, profileOwnerID, resourceID, result);
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		return client.postCommentOnResource(commentCreatorID, resourceCreatorID, resourceID, values);
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID, int manipulationID) {
		return client.delCommentOnResource(resourceCreatorID, resourceID, manipulationID);
	}
}