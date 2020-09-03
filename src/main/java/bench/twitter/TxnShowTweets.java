package bench.twitter;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class TxnShowTweets extends TwitterTransaction {
	private int userId;

	public TxnShowTweets(KvInterface kvi, int userId) {
		super(kvi, "ShowTweets");
		this.userId = userId;
	}

	@Override
	public void inputGeneration() {

	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();
		
		int lastTweet = getLastTweet(userId);
		int i = 0;
		if(lastTweet - 10 > 0) {
			// only show 10 tweets
			i = lastTweet - 10;
		}
		while(i <= lastTweet) {
			getTweet(i); // but those tweets are not from the same userId
			i++;
		}

		return commitTxn();
	}

}
