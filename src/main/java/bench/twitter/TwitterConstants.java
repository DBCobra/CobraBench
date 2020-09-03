package bench.twitter;

public final class TwitterConstants {
	public final static String TBL_User = "user";
	public final static String TBL_Tweet = "tweet";
	public final static String TBL_LastTweet = "ltweet";
	public final static String TBL_Following= "following";
	public final static String TBL_Followers= "followers";
	public final static String TBL_FollowList= "followlist";
	
	public final static int TWEETS_PER_USER = 10;
	
	public enum TWIT_TXN_TYPE {TWEET, FOLLOW, TIMELINE, SHOWFOLLOW, SHOWTWEETS};
}
