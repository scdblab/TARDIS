package mongodb;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import edu.usc.bg.base.ByteIterator;

public class MemcachedMongoBGClientDelegate extends MemcachedMongoBGClient {

	@Override
	public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
			boolean insertImage, boolean testMode) {
		return 1;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> results, boolean insertImage, boolean testMode) {
		return 1;
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		return 1;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		return 1;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		return 1;
	}

	@Override
	public int thawFriendship(int inviterID, int inviteeID) {
		return 1;
	}
	
	
}
