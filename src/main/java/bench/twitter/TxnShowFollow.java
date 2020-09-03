package bench.twitter;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class TxnShowFollow extends TwitterTransaction {
	private int userId;

	public TxnShowFollow(KvInterface kvi, int userId) {
		super(kvi, "ShowFollow");
		this.userId = userId;
	}

	@Override
	public void inputGeneration() {

	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();

		getFollowList(userId);

		return commitTxn();
	}

}
