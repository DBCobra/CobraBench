package bench.rubis;

import java.util.HashMap;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class StoreBuyNow extends RubisTransaction {
	private int itemId, qty, userId, buyId;

	public StoreBuyNow(KvInterface kvi, int buyId, int itemId, int qty, int userId) {
		super(kvi, "StoreBuyNow");
		this.itemId = itemId;
		this.qty = qty;
		this.userId = userId;
		this.buyId = buyId;
	}

	@Override
	public void inputGeneration() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();
		HashMap<String, Object> item = getItem(itemId);
		if (item == null) {
//			System.out.println("get itemId"+itemId+" failed!");
			commitTxn();
			return false;
		}
		int iqty = (Integer) item.get("quantity");
		item.put("quantity", iqty - qty);
		boolean res = updateItem(itemId, item);
		if (res == false) {
			commitTxn();
			return false;
		}
		res = insertBuyNow(buyId, userId, itemId, qty);
		if (res == false) {
			commitTxn();
			return false;
		}
		commitTxn();
		return true;
	}

}
