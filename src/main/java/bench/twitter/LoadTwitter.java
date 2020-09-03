package bench.twitter;

import bench.BenchUtils;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class LoadTwitter extends TwitterTransaction {
	private int userId;

	public LoadTwitter(KvInterface kvi, int userId) {
		super(kvi, "load");
		this.userId = userId;
	}

	@Override
	public void inputGeneration() {

	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();
		String name = BenchUtils.getRandomValue(10);
		String info = BenchUtils.getRandomValue(200);
		insertUser(userId, name, info);

		// each user have 10 tweets.
		for (int i = 0; i < TwitterConstants.TWEETS_PER_USER; i++) {
			String data = BenchUtils.getRandomValue(100);
			int tweetId = userId * TwitterConstants.TWEETS_PER_USER + i;
			insertTweet(tweetId, i, data);
		}
		setLastTweet(userId, (userId + 1) * TwitterConstants.TWEETS_PER_USER - 1);
		// each user follows himself.
		insertFollowList(userId);
		setFollowers(userId, userId, "notime", true);
		setFollowing(userId, userId, "notime", true);

		return commitTxn();
	}

}
