package bench.rubis;

import java.util.Set;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class RandomBrose extends RubisTransaction {
	private int[] itemIds;
	private Set<Integer> ratingIds;

	public RandomBrose(KvInterface kvi, int itemIds[], Set<Integer> ratingIds) {
		super(kvi, "Brose");
		this.itemIds = itemIds;
		this.ratingIds = ratingIds;
	}

	@Override
	public void inputGeneration() {
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();
		for (int i = 0; i < itemIds.length; i++) {
			getItem(itemIds[i]);
		}
		for (int rating : ratingIds) {
			getRating(rating);
		}
		commitTxn();
		return true;
	}

}
