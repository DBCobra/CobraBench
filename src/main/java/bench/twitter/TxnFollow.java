package bench.twitter;

import bench.BenchUtils;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class TxnFollow extends TwitterTransaction {
	private int src, dst;
	private boolean follow;

	public TxnFollow(KvInterface kvi, int src, int dst) {
		super(kvi, "Follow");
		this.src = src;
		this.dst = dst;
	}

	@Override
	public void inputGeneration() {
		follow = (BenchUtils.getRandomInt(0, 2) == 0);
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		String time = BenchUtils.MakeTimeStamp();
		beginTxn();
		
		setFollowers(src, dst, time, follow);
		setFollowing(src, dst, time, follow);
		updateFollowList(src, dst, follow);

		return commitTxn();
	}

}
