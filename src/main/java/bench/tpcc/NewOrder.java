package bench.tpcc;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Config;
import main.Logger;

public class NewOrder extends TPCCTransaction{
	private int W_ID;
	private int D_W_ID;
	private int D_ID;
	private int C_W_ID;
	private int C_D_ID;
	private int C_ID;
	private int OL_CNT;
	private boolean rbk;
	private boolean OL_ALL_LOCAL;
	private String O_ENTRY_D;
	private ArrayList<HashMap<String, Object>> items;
	
	public NewOrder(int W_ID, KvInterface kvi) {
		super(kvi, "TPCC-NewOrder");
		this.W_ID = W_ID;
		items = new ArrayList<HashMap<String,Object>>();
	}

	public void inputGeneration() {
		this.D_ID = Utils.RandomNumber(1, 10);
		this.D_W_ID = this.W_ID;
		this.C_D_ID = this.D_ID;
		this.C_W_ID = this.W_ID;
		this.C_ID = Utils.NURand(1023, 1, 3000);
		this.OL_CNT = Utils.RandomNumber(5, 15);
		this.rbk = Utils.RandomNumber(1, 100) <= 1;
		this.OL_ALL_LOCAL = false;
		
		for(int i = 0; i < this.OL_CNT; i++) {
			int OL_I_ID = 0;
			boolean needReRandom = true;
			while(needReRandom) {
				OL_I_ID = Utils.NURand(8191, 1, 100000);
				needReRandom = false;
				for (HashMap<String, Object> item : items) {
					if((Integer) item.get("OL_I_ID") == OL_I_ID) {
						needReRandom = true;
					}
				}
			}

			if(this.rbk && i == this.OL_CNT-1) {
				OL_I_ID = -1;
			}
			int OL_SUPPLY_W_ID = W_ID;
			if(Utils.RandomNumber(1, 100) == 1 && Config.get().WAREHOUSE_NUM > 1) {
				OL_ALL_LOCAL = false;
				// random in [1, WAREHOUSE)NUM] \ {W_ID}
				OL_SUPPLY_W_ID = Utils.RandomNumber(1, Config.get().WAREHOUSE_NUM-1);
				if(OL_SUPPLY_W_ID >= W_ID) {
					OL_SUPPLY_W_ID++;
				}
			}
			int OL_QUANTITY = Utils.RandomNumber(1, 10);
			HashMap<String, Object> item = new HashMap<String, Object>();
			item.put("OL_I_ID", OL_I_ID);
			item.put("OL_SUPPLY_W_ID", OL_SUPPLY_W_ID);
			item.put("OL_QUANTITY", OL_QUANTITY);
			items.add(item);
		}
		O_ENTRY_D = new SimpleDateFormat("dd-MM-yyyy").format(new Date());
	}

	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();
		Logger.logDebug("NO: 1st data retrieval");
		HashMap<String, Object> wh = selectWarehouse(W_ID); // 1st data retrieval
		if(wh == null) {
			abortTxn();
			return false;
		}
		double w_tax = (Double) wh.get("W_TAX");
		Logger.logDebug("NO: 2nd data retrieval");
		HashMap<String, Object> district = selectDistrict(D_W_ID, D_ID);// 2nd data retrieval
		if(district == null) {
			abortTxn();
			return false;
		}
		double d_tax = (Double) district.get("D_TAX");
		int o_id = (Integer) district.get("D_NEXT_O_ID");
		district.put("D_NEXT_O_ID", o_id+1);
		Logger.logDebug("NO: 1st update");
		updateDistrict(district); // 1st update
		Logger.logDebug("NO: 3rd retrieval");
		HashMap<String, Object> customer = selectCustomer(C_W_ID, C_D_ID, C_ID); // 3rd data retrieval
		if(customer == null) {
			abortTxn();
			return false;
		}
		double c_discount = (Double) customer.get("C_DISCOUNT");
		String c_credit = (String) customer.get("C_CREDIT");
		String c_last = (String) customer.get("C_LAST");
		
		Logger.logDebug("NO: 1st insertion");
		insertOrder(o_id, D_ID, W_ID, C_ID, O_ENTRY_D, OL_CNT, OL_ALL_LOCAL); // 1st insertion
		insertNewOrder(o_id, D_ID, W_ID); // 2nd insertion
		updateNewOrderList(W_ID, D_ID, o_id);
		
		double total_amount = 0;
		for(int ol_number = 1; ol_number <= OL_CNT; ol_number++) {
			HashMap<String, Object> item = items.get(ol_number-1);
			int i_id = (Integer) item.get("OL_I_ID");
			int ol_supply_w_id = (Integer) item.get("OL_SUPPLY_W_ID");
			int ol_quantity = (Integer) item.get("OL_QUANTITY");
			Logger.logDebug("NO: select item");
			HashMap<String, Object> itemRow = selectItem(i_id); // 1st retrieval
			if(itemRow == null) {
				abortTxn();
				return false;
			}
			double i_price = (Double) itemRow.get("I_PRICE");
			String i_name = (String) itemRow.get("I_NAME");
			String i_data = (String) itemRow.get("I_DATA");
			
			Logger.logDebug("NO: select stock");
			HashMap<String, Object> stock = selectStock(i_id, ol_supply_w_id); // 2nd retrieval
			if(stock == null) {
				abortTxn();
				return false;
			}
			int s_quantity = (Integer) stock.get("S_QUANTITY");
			String s_dist_xx = (String) stock.get("S_DIST_" + String.format("%02d", D_ID));
			String s_data = (String) stock.get("S_DATA");
			if(s_quantity >= ol_quantity + 10) {
				s_quantity -= ol_quantity;
			} else {
				s_quantity = s_quantity - ol_quantity + 91;
			}
			int s_ytd = (Integer) stock.get("S_YTD") + ol_quantity;
			int s_order_cnt = (Integer) stock.get("S_ORDER_CNT") + 1;
			int s_remote_cnt = (Integer) stock.get("S_REMOTE_CNT");
			if(ol_supply_w_id != W_ID) {
				s_remote_cnt++;
			}
			stock.put("S_QUANTITY", s_quantity);
			stock.put("S_YTD", s_ytd);
			stock.put("S_ORDER_CNT", s_order_cnt);
			stock.put("S_REMOTE_CNT", s_remote_cnt);
			Logger.logDebug("NO: update stock");
			updateStock(stock); // 1st update
			
			double ol_anoumt = ol_quantity * i_price;
			total_amount += ol_anoumt;
			char brand_generic = 'G';
			if(i_data.contains("ORIGINAL") && s_data.contains("ORIGINAL")) {
				brand_generic = 'B';
			}
			Logger.logDebug("NO: insert order line");
			insertOrderLine(o_id, D_ID, W_ID, ol_number, i_id, 
					ol_supply_w_id, ol_quantity, ol_anoumt, s_dist_xx, ""); // 1 insertion
		}
		total_amount = total_amount * (1+w_tax+d_tax) * (1-c_discount);
		commitTxn();
		return true;
	}
}
