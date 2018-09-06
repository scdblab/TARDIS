package edu.usc.bg.validator;

import edu.usc.bg.workloads.CoreWorkload;

public class MemberEntity {
	public static String lineSeparator = System.getProperty("line.separator");
	public final static String FRIEND_CNT_PROP="FRIEND_CNT";
	public final static String PEND_CNT_PROP="PENDING_CNT";
	public final static String RES_CNT_PROP="RES_CNT";
	
	public final static String MEMBER_ENTITY="MEMBER";
	public static final char NEW_VALUE_UPDATE = 'N';
    public static final char INCREMENT_UPDATE = 'I';
    public static final char NO_READ_UPDATE = 'X';
    public static final char VALUE_READ = 'R';

    public static final char RECORD_ATTRIBUTE_SEPERATOR = ',';
    public static final char ENTITY_SEPERATOR = '&';
    public static final char PROPERY_SEPERATOR = '#';
    public static final char PROPERY_ATTRIBUTE_SEPERATOR = ':';
    public static final char ENTITY_ATTRIBUTE_SEPERATOR = ';';
    public static final char KEY_SEPERATOR = '-';
	public static final char UPDATE_RECORD = 'U';
	public static final char READ_RECORD = 'R';
	private String friendCount,pendingCount, resourceCount,key;
	public MemberEntity(){
		
	}
	public MemberEntity(String key,String fcount, String pcount, String rcount){
		friendCount=fcount;
		pendingCount=pcount;
		resourceCount=rcount;
		this.key=key;
		
		
	}

	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getFriendCount() {
		return friendCount;
	}

	public void setFriendCount(String friendCount) {
		this.friendCount = friendCount;
	}

	public String getPendingCount() {
		return pendingCount;
	}

	public void setPendingCount(String pendingCount) {
		this.pendingCount = pendingCount;
	}

	public String getResourceCount() {
		return resourceCount;
	}

	public void setResourceCount(String resourceCount) {
		this.resourceCount = resourceCount;
	}
	public static void main(String[] args){
		MemberEntity m = new MemberEntity("k",null,"p",null);
		//System.out.println(m.getFriendCount());
		System.out.print(generateLogRecord(CoreWorkload.GET_PENDINGS_ACTION_NAME, 0, 100, 1000, 2000, m, null));
	}
	public static Object generateLogRecord(String actionName, int seqID, int threadid, long start,
			long end, MemberEntity m1, MemberEntity m2) {
		//,GetProfile,0,9,433723055319080,433723059494122,MEMBER;32;
		String log=RECORD_ATTRIBUTE_SEPERATOR
		+actionName+RECORD_ATTRIBUTE_SEPERATOR
		+threadid+RECORD_ATTRIBUTE_SEPERATOR
		+seqID+RECORD_ATTRIBUTE_SEPERATOR
		+start+RECORD_ATTRIBUTE_SEPERATOR
		+end+RECORD_ATTRIBUTE_SEPERATOR
		+MEMBER_ENTITY+ENTITY_ATTRIBUTE_SEPERATOR
		+ m1.key+ENTITY_ATTRIBUTE_SEPERATOR;
		char type='x';
		if (actionName.equals(CoreWorkload.VIEW_PROFILE_ACTION_NAME)||actionName.equals(CoreWorkload.GET_FRIENDS_ACTION_NAME)|| actionName.equals(CoreWorkload.GET_PENDINGS_ACTION_NAME)){
			//FRIEND_CNT:100:R
			log=READ_RECORD+log;
			type=VALUE_READ;
			
		}
		else {
		//	U,InviteFriends,0,26,433724925283621,433725025728836,MEMBER;3585;PENDING_CNT:+1:I
			log=UPDATE_RECORD+log;
			type=INCREMENT_UPDATE;
				}
			log=log+getEntityString(m1,type);
			if (m2!=null){
				log=log+ENTITY_SEPERATOR+MEMBER_ENTITY+ENTITY_ATTRIBUTE_SEPERATOR
						+ m2.key+ENTITY_ATTRIBUTE_SEPERATOR;
				log=log+getEntityString(m2,type);
			}

	
		return log+lineSeparator;
	}
	private static String getEntityString(MemberEntity m1, char type) {
		String log="";
		if (m1.friendCount!=null){
			log=log+FRIEND_CNT_PROP+PROPERY_ATTRIBUTE_SEPERATOR
			+ m1.friendCount+PROPERY_ATTRIBUTE_SEPERATOR
			+type;
			}
			if (m1.pendingCount!=null){
				if (m1.friendCount!=null){
				log=log+PROPERY_SEPERATOR;
				}
				
						log=log+PEND_CNT_PROP+PROPERY_ATTRIBUTE_SEPERATOR
						+ m1.pendingCount+PROPERY_ATTRIBUTE_SEPERATOR
						+type;
			}
			
			if (m1.resourceCount!=null){
				if (m1.friendCount!=null ||m1.pendingCount!=null){
				log=log+PROPERY_SEPERATOR;
				}
				
						log=log+RES_CNT_PROP+PROPERY_ATTRIBUTE_SEPERATOR
						+ m1.resourceCount+PROPERY_ATTRIBUTE_SEPERATOR
						+type;
			}
			
		return log;
	}
	


}
