package bench.tpcc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import bench.chengTxn.ChengTxnConstants;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Config;

public class Payment extends TPCCTransaction {

	private int W_ID;
	private int D_ID;
	private boolean customer_by_lastname;
	private int C_D_ID;
	private int C_W_ID;
	private String C_LAST;
	private int C_ID;
	private double H_AMOUNT;
	private String H_DATE;
	private boolean home;
	
	public Payment(int W_ID, KvInterface kvi) {
		super(kvi, "TPCC-Payment");
		this.W_ID = W_ID;
	}

	@Override
	public void inputGeneration() {
		int x = Utils.RandomNumber(1, 100);
		int y = Utils.RandomNumber(1, 100);
		D_ID = Utils.RandomNumber(1, 10);
		C_W_ID = W_ID;
		if(x <= 85) {
			C_D_ID = D_ID;
		} else {
			C_D_ID = Utils.RandomNumber(1, 10);
			if(Config.get().WAREHOUSE_NUM > 1) {
				C_W_ID = Utils.RandomNumber(1, Config.get().WAREHOUSE_NUM-1);
				if(C_W_ID >= W_ID) {
					C_W_ID += 1;
				}
			}
		}
		if(y <= 60) {
			customer_by_lastname = true;
			C_LAST = Utils.Lastname(Utils.NURand(255, 0, 999));
			C_ID = -1;
		} else {
			C_LAST = "!!!";
			C_ID = Utils.NURand(1023, 1, 3000);
		}
		H_AMOUNT = (double) Utils.RandomNumber(100, 500000) / 100.00;
		H_DATE = Utils.MakeTimeStamp();
		home = (C_W_ID == W_ID);
		
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();
		HashMap<String, Object> wh = selectWarehouse(W_ID); // 1st data retrieval
		double w_ytd = (double) wh.get("W_YTD");
		w_ytd += H_AMOUNT;
		wh.put("W_YTD", w_ytd);
		String W_NAME = (String) wh.get("W_NAME");
		wh.get("W_STREET_1");
		wh.get("W_STREET_2");
		wh.get("W_CITY");
		wh.get("W_STATE");
		wh.get("W_ZIP");
		
		HashMap<String, Object> dist = selectDistrict(W_ID, D_ID); // 2nd data retrieval
		double d_ytd = (double) dist.get("D_YTD");
		d_ytd += H_AMOUNT;
		dist.put("D_YTD", d_ytd);
		String D_NAME = (String) dist.get("D_NAME");
		dist.get("D_STREET_1");
		dist.get("D_STREET_2");
		dist.get("D_CITY");
		dist.get("D_STATE");
		dist.get("D_ZIP");
		
		HashMap<String, Object> customer = null;
		if(customer_by_lastname) {
			ArrayList<HashMap<String, Object>> custs = new ArrayList<HashMap<String, Object>>();
			ArrayList<String> secondaryIndex = getCustomer2Index(W_ID, D_ID, C_LAST); // 3rd data retrieval
			for (String scid : secondaryIndex) {
				int cid = Integer.valueOf(scid);
				HashMap<String, Object> cust = selectCustomer(W_ID, D_ID, cid); // data retrieval, amount of times depends on data
				custs.add(cust);
			}
			Collections.sort(custs, new sortByFirstName());
			customer = custs.get(custs.size()/2);
		} else {
			customer = selectCustomer(W_ID, D_ID, C_ID); // 3rd data retrieval
		}
		C_ID = (int) customer.get("C_ID");
		C_LAST = (String) customer.get("C_LAST");
		customer.get("C_FIRST");
		customer.get("C_MIDDLE");
		customer.get("C_STREET_1");
		customer.get("C_STREET_2");
		double cbal = (double) customer.get("C_BALANCE");
		customer.put("C_BALANCE", cbal - H_AMOUNT);
		double c_ytd = (double) customer.get("C_YTD_PAYMENT");
		customer.put("C_YTD_PAYMENT", c_ytd - H_AMOUNT);
		int c_pay_cnt = (int) customer.get("C_PAYMENT_CNT");
		customer.put("C_PAYMENT_CNT", c_pay_cnt + 1);
		String ccredit = (String) customer.get("C_CREDIT");
		if(ccredit == "BC") {
			String cdata = (String) customer.get("C_DATA");
			cdata =  Integer.toString(C_ID) + Integer.toString(C_D_ID) + Integer.toString(C_W_ID) + Integer.toString(C_ID) + Integer.toString(W_ID) + cdata;
			cdata = cdata.substring(0, Math.min(cdata.length(), 500));
			customer.put("C_DATA", cdata);
		}
		updateCustomer(customer);

		String H_DATA = W_NAME + "    " + D_NAME;
		insertHistory(C_ID, C_D_ID, C_W_ID, D_ID, W_ID, H_DATE, H_AMOUNT, H_DATA);
		commitTxn();
		return true;
	}
}
