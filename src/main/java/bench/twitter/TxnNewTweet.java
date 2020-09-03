package bench.twitter;

import bench.BenchUtils;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class TxnNewTweet extends TwitterTransaction {
	private int userId, tweetId;
	private String data;

	public TxnNewTweet(KvInterface kvi, int userId, int tweetId) {
		super(kvi, "NewTweet");
		this.userId = userId;
		this.tweetId = tweetId;
	}

	@Override
	public void inputGeneration() {
		data = BenchUtils.getRandomValue(120);
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();
		
		insertTweet(tweetId, userId, data);
		setLastTweet(userId, tweetId);
		
		return commitTxn();
	}

}
