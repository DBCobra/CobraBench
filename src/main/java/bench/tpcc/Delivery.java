package bench.tpcc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class Delivery extends TPCCTransaction {

	private int W_ID;
	private int O_CARRIER_ID;
	private String OL_DELIVERY_D;

	public Delivery(int W_ID, KvInterface kvi) {
		super(kvi, "TPCC-Delivery");
		this.W_ID = W_ID;
	}

	@Override
	public void inputGeneration() {
		this.O_CARRIER_ID = Utils.RandomNumber(1, 10);
		this.OL_DELIVERY_D = Utils.MakeTimeStamp();
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		int d_id = Utils.RandomNumber(1, TPCCConstants.DIST_PER_WARE);
//		for (int d_id = 1; d_id <= TPCCConstants.DIST_PER_WARE; d_id++) {
			beginTxn();
			ArrayList<String> nolist = getNewOrderList(W_ID, d_id); // 1st read
			int noid_first = Integer.parseInt(nolist.get(0));
			int noid_last = Integer.parseInt(nolist.get(1));
			if(noid_first > noid_last) {
				assert noid_first == noid_last+1; // which symbolizes the list is empty
				commitTxn();
//				continue;
				return true;
			}
			int o_id = noid_first;
			HashMap<String, Object> newOrder = selectNewOrder(o_id, d_id, W_ID); // 2nd read
			deleteNewOrder(o_id, d_id, W_ID); // 1st write (update)
			deleteFromNewOrderList(W_ID, d_id, o_id, nolist); // 2nd write (update)
			HashMap<String, Object> order = selectOrder(W_ID, d_id, o_id);
			int c_id = (int) order.get("O_C_ID");
			order.put("O_CARRIER_ID", O_CARRIER_ID);
			updateOrder(order); // 3rd write(update)
			int ol_cnt = (int) order.get("O_OL_CNT");
			double ol_amount = 0;
			for(int ol_id = 1; ol_id <= ol_cnt; ol_id++) {
				HashMap<String, Object> ol = selectOrderLine(W_ID, d_id, o_id, ol_id); // 10*reads
				ol_amount += (double) ol.get("OL_AMOUNT");
				ol.put("OL_DELIVERY_D", OL_DELIVERY_D);
				updateOrderLine(ol); // 10*updates
			}
			HashMap<String, Object> customer = selectCustomer(W_ID, d_id, c_id); // 13th read
			double bal = (double) customer.get("C_BALANCE");
			int c_delivery_cnt = (int)customer.get("C_DELIVERY_CNT");
			bal += ol_amount;
			c_delivery_cnt -= 1;
			customer.put("C_BALANCE", bal);
			customer.put("C_DELIVERY_CNT", c_delivery_cnt);
			updateCustomer(customer); // 14th update
			commitTxn();
//		}
		return true;
	}
}
