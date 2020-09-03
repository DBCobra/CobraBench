package bench.rubis;

import java.util.HashMap;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class StoreComment extends RubisTransaction {
	private int commentId, fromId, toId, itemId, rating;
	private String comment;
	private boolean init;

	public StoreComment(KvInterface kvi, int commentId, int itemId, int fromId, int toId, boolean init) {
		super(kvi, "StoreComment");
		this.fromId = fromId;
		this.toId = toId;
		this.itemId = itemId;
		this.commentId = commentId;
		this.init = init;
	}

	@Override
	public void inputGeneration() {
		rating = commentId % 5;
		comment = "This is a comment on an item which nobody will ever read";
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
//		System.out.println("storeComment item" + itemId +" toId"+toId);
		beginTxn();

		boolean res = insertComment(commentId, fromId, toId, itemId, rating, "2018-3-12", comment);
		if (res == false) {
			commitTxn();
			return false;
		}

		if(init) {
			commitTxn();
			return true;
		}
		
		HashMap<String, Object> userTo = getUser(toId);
		if (userTo == null) {
			commitTxn();
			return false;
		}

		res = insertRating(toId, rating, true);
		if (res == false) {
			commitTxn();
			return false;
		}

		commitTxn();
		return true;
	}

}
