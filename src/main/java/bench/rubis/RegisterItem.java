package bench.rubis;

import java.util.HashMap;

import bench.BenchUtils;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class RegisterItem extends RubisTransaction {
	int itemId, userId;
	String name = null, description = null;
	float iPrice, buyNow, rPrice;
	int quantity, duration;
	Integer category;
	boolean loading;

	public RegisterItem(KvInterface kvi, int itemId, int userId, boolean loading) {
		super(kvi, "RegisterItem");
		this.itemId = itemId;
		this.userId = userId;
		this.loading = loading;
	}

	@Override
	public void inputGeneration() {
		name = BenchUtils.getRandomValue(10);
		description = BenchUtils.getRandomValue(40);
		iPrice = BenchUtils.getRandomInt(10, 20);
		buyNow = BenchUtils.getRandomInt(10, 20);
		rPrice = iPrice + BenchUtils.getRandomInt(10, 20);
		quantity = BenchUtils.getRandomInt(10, 100);
		duration = BenchUtils.getRandomInt(10, 100);
		category = 0;
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		// TODO Auto-generated method stub
		beginTxn();
		if(!loading) {
			HashMap<String, Object> user = getUser(userId);
			if (user == null) {
				commitTxn();
				return false;
			}
		}
		boolean res = insertItem(itemId, name, description, iPrice, quantity, rPrice, buyNow, 0, 10, userId, category);
		commitTxn();
		return res;
	}

}