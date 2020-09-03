package bench.tpcc;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class OrderStatus extends TPCCTransaction {
	private int W_ID;
	private int D_ID;
	private boolean customer_by_lastname;
	private int C_ID;
	private String C_LAST;
	
	public OrderStatus(int W_ID, KvInterface kvi) {
		super(kvi, "TPCC_OrdStat");
		this.W_ID = W_ID;
	}

	@Override
	public void inputGeneration() {
		this.D_ID = Utils.RandomNumber(1, 10);
		int lastnameRand = Utils.RandomNumber(1, 100);
		if (lastnameRand <= 60) {
			customer_by_lastname = true;
			C_LAST = Utils.Lastname(Utils.NURand(255, 0, 999));
			C_ID = -1;
		} else {
			customer_by_lastname = false;
			C_ID = Utils.NURand(1023, 1, 3000);
			C_LAST = "!!!";
		}
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();
		HashMap<String, Object> customer = null;
		if(customer_by_lastname) {
			ArrayList<HashMap<String, Object>> custs = new ArrayList<HashMap<String, Object>>();
			ArrayList<String> secondaryIndex = getCustomer2Index(W_ID, D_ID, C_LAST);
			for (String scid : secondaryIndex) {
				int cid = Integer.valueOf(scid);
				HashMap<String, Object> cust = selectCustomer(W_ID, D_ID, cid);
				custs.add(cust);
			}
			Collections.sort(custs, new sortByFirstName());
			customer = custs.get(custs.size()/2);
		} else {
			customer = selectCustomer(W_ID, D_ID, C_ID);
		}
		// retrieve something from customer
		double c_balance = (double) customer.get("C_BALANCE");
		String c_first = (String) customer.get("C_FIRST");
		String c_middle = (String) customer.get("C_MIDDLE");
		C_LAST = (String) customer.get("C_LAST");
		C_ID = (int) customer.get("C_ID");
		
		HashMap<String, Object> order = selectOrder(W_ID, D_ID, C_ID);
		int O_OL_CNT = (int) order.get("O_OL_CNT");
		int o_id = (int) order.get("O_ID");
		for(int ol_num = 1; ol_num <= O_OL_CNT; ol_num++) {
			HashMap<String, Object> ol = selectOrderLine(W_ID, D_ID, o_id, ol_num);
			// retrieve something from ol
			ol.get("OL_I_ID");
			ol.get("OL_SUPPLY_W_ID");
			ol.get("OL_QUANTITY");
			ol.get("OL_AMOUNT");
			ol.get("OL_DELIVERY_D");
		}
		commitTxn();
		return true;
	}
}
