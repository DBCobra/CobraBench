package bench.tpcc;

import java.util.HashMap;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class StockLevel extends TPCCTransaction {
	private int W_ID;
	private int D_ID;
	private int threshold;
	
	public StockLevel(int W_ID, int D_ID, KvInterface kvi) {
		super(kvi, "TPCC-Slevel");
		this.W_ID = W_ID;
		this.D_ID = D_ID;
	}

	@Override
	public void inputGeneration() {
		this.threshold = Utils.RandomNumber(10, 20);
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();
		int stock_count = 0;
		HashMap<String, Object> district = selectDistrict(W_ID, D_ID);
		commitTxn();
		int D_NEXT_O_ID = (int) district.get("D_NEXT_O_ID");
		for (int o_id = D_NEXT_O_ID - 20; o_id < D_NEXT_O_ID; o_id++) {
			beginTxn();
			HashMap<String, Object> order = selectOrder(W_ID, D_ID, o_id);
			int ol_count = (int) order.get("O_OL_CNT");
			for (int ol_num = 1; ol_num <= ol_count; ol_num++) {
				int ol_i_id = (int) selectOrderLine(W_ID, D_ID, o_id, ol_num).get("OL_I_ID");
				HashMap<String, Object> stock = selectStock(ol_i_id, W_ID);
				if ((int) stock.get("S_QUANTITY") < threshold) {
					stock_count++;
				}
			}
			commitTxn();
		}
		return true;
	}
}
