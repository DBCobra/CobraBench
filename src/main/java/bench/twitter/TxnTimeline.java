package bench.twitter;

import java.util.ArrayList;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class TxnTimeline extends TwitterTransaction {
	private int userId;

	public TxnTimeline(KvInterface kvi, int userId) {
		super(kvi, "Timeline");
		this.userId = userId;
	}

	@Override
	public void inputGeneration() {

	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();

		ArrayList<Integer> following = getFollowList(userId);
		for (int i = 0; i < following.size(); i++) {
			if (i >= 20) {
				// only look for 20 newest tweets.
				break;
			}
			int dstId = following.get(i);
			int lastTweet = getLastTweet(dstId);
			getTweet(lastTweet);
		}

		return commitTxn();
	}

}
