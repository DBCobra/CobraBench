package bench.tpcc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Table;

import bench.Tables;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Config;
import main.Logger;

// TPCCTransaction is a class that convert queries to String key-value store operations

public abstract class TPCCTransaction extends bench.Transaction{
	private static AtomicInteger history_id = new AtomicInteger(Config.get().CLIENT_ID*10000000);

	public TPCCTransaction(KvInterface kvi, String name) {
		super(kvi, name);
	}
	
	protected HashMap<String, Object> selectWarehouse(int W_ID) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {W_ID};
		String key = Tables.encodeKey(TPCCConstants.TBL_Warehouse, keys);
		String res = kvi.get(txn, key);
		return Tables.decodeTable(res);
	}
	
	protected boolean insertWarehouse(int W_ID, String W_NAME, String W_STREET_1, String W_STREET_2, String W_CITY,
			String W_STATE, String W_ZIP, double W_TAX, double W_YTD) throws KvException, TxnException {
		check_txn_exists();
		HashMap<String, Object> warehouse = new HashMap<String, Object>();
		warehouse.put("W_ID", W_ID);
		warehouse.put("W_NAME", W_NAME);
		warehouse.put("W_STREET_1", W_STREET_1);
		warehouse.put("W_STREET_2", W_STREET_2);
		warehouse.put("W_CITY", W_CITY);
		warehouse.put("W_STATE", W_STATE);
		warehouse.put("W_ZIP", W_ZIP);
		warehouse.put("W_TAX", W_TAX);
		warehouse.put("W_YTD", W_YTD);
		String key = Tables.encodeKey(TPCCConstants.TBL_Warehouse, new int[] {W_ID});
		String val = Tables.encodeTable(warehouse);
		return kvi.insert(txn, key, val);
	}
	
	protected boolean updateWarehouse(HashMap<String, Object> warehouse) throws KvException, TxnException {
		check_txn_exists();
		int wid = (Integer) warehouse.get("W_ID");
		String key = Tables.encodeKey(TPCCConstants.TBL_Warehouse, new int[] {wid});
		String val = Tables.encodeTable(warehouse);
		return kvi.set(txn, key, val);
	}
	
	protected HashMap<String, Object> selectDistrict(int D_W_ID, int D_ID) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {D_W_ID, D_ID};
		String key = Tables.encodeKey(TPCCConstants.TBL_District, keys);
		String res = kvi.get(txn, key);
		return Tables.decodeTable(res);
	}
	
	protected boolean insertDistrict(int D_ID, int D_W_ID, String D_NAME, String D_STREET_1, String D_STREET_2,
			String D_CITY, String D_STATE, String D_ZIP, double D_TAX, double D_YTD, int D_NEXT_O_ID) throws KvException, TxnException {
		check_txn_exists();
		HashMap<String, Object> district = new HashMap<String, Object>();
		district.put("D_ID", D_ID);
		district.put("D_W_ID", D_W_ID);
		district.put("D_NAME", D_NAME);
		district.put("D_STREET_1", D_STREET_1);
		district.put("D_STREET_2", D_STREET_2);
		district.put("D_CITY", D_CITY);
		district.put("D_STATE", D_STATE);
		district.put("D_ZIP", D_ZIP);
		district.put("D_TAX", D_TAX);
		district.put("D_YTD", D_YTD);
		district.put("D_NEXT_O_ID", D_NEXT_O_ID);
		int keys[] = {D_W_ID, D_ID};
		String key = Tables.encodeKey(TPCCConstants.TBL_District, keys);
		String val = Tables.encodeTable(district);
		return kvi.insert(txn, key, val);
	}
	
	
	protected boolean updateDistrict(HashMap<String, Object> district) throws KvException, TxnException {
		check_txn_exists();
		assert txn != null && kvi != null;
		int d_w_id = (Integer) district.get("D_W_ID");
		int d_id = (Integer) district.get("D_ID");
		int keys[] = {d_w_id, d_id};
		String key = Tables.encodeKey(TPCCConstants.TBL_District, keys);
		String val = Tables.encodeTable(district);
		return kvi.set(txn, key, val);
	}

	protected HashMap<String, Object> selectStock(int S_I_ID, int S_W_ID) throws KvException, TxnException{
		check_txn_exists();
		int keys[] = {S_W_ID, S_I_ID};
		String key = Tables.encodeKey(TPCCConstants.TBL_Stock, keys);
		String res = kvi.get(txn, key);
		return Tables.decodeTable(res);
	}

	protected boolean insertStock(int S_I_ID, int S_W_ID, int S_QUANTITY, String[] S_DIST_XX, 
			int S_YTD, int S_ORDER_CNT, int S_REMOTE_CNT, String S_DATA) throws KvException, TxnException {
		check_txn_exists();
		HashMap<String, Object> stock = new HashMap<String, Object>();
		stock.put("S_I_ID", S_I_ID);
		stock.put("S_W_ID", S_W_ID);
		stock.put("S_QUANTITY", S_QUANTITY);
		for(int i = 0; i < 10; i++) {
			String keyname = String.format("S_DIST_%02d", i+1);
			stock.put(keyname, S_DIST_XX[i]);
		}
		stock.put("S_YTD", S_YTD);
		stock.put("S_ORDER_CNT", S_ORDER_CNT);
		stock.put("S_REMOTE_CNT", S_REMOTE_CNT);
		stock.put("S_DATA", S_DATA);
		int keys[] = {S_W_ID, S_I_ID};
		String key = Tables.encodeKey(TPCCConstants.TBL_Stock, keys);
		String val = Tables.encodeTable(stock);
		return kvi.insert(txn, key, val);
	}
	
	protected boolean updateStock(HashMap<String, Object> stock) throws KvException, TxnException {
		check_txn_exists();
		int S_W_ID = (Integer) stock.get("S_W_ID");
		int S_I_ID = (Integer) stock.get("S_I_ID");
		int keys[] = {S_W_ID, S_I_ID};
		String key = Tables.encodeKey(TPCCConstants.TBL_Stock, keys);
		String val = Tables.encodeTable(stock);
		return kvi.set(txn, key, val);
	}
	
	protected HashMap<String, Object> selectOrderLine(int OL_W_ID, int OL_D_ID, int OL_O_ID, int OL_NUMBER) throws KvException, TxnException{
		check_txn_exists();
		int keys[] = {OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER};
		String key = Tables.encodeKey(TPCCConstants.TBL_OrderLine, keys);
		String res = kvi.get(txn, key);
		return Tables.decodeTable(res);
	}
	
	protected boolean updateOrderLine(HashMap<String, Object> ol) throws KvException, TxnException {
		check_txn_exists();
		int OL_W_ID = (int) ol.get("OL_W_ID");
		int OL_D_ID = (int) ol.get("OL_D_ID");
		int OL_O_ID = (int) ol.get("OL_O_ID");
		int OL_NUMBER = (int) ol.get("OL_NUMBER");
		int keys[] = {OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER};
		String key = Tables.encodeKey(TPCCConstants.TBL_OrderLine, keys);
		String val = Tables.encodeTable(ol);
		return kvi.set(txn, key, val);
	}

	protected boolean insertOrderLine(int OL_O_ID, int OL_D_ID, int OL_W_ID, int OL_NUMBER, 
			int OL_I_ID, int OL_SUPPLY_W_ID, int OL_QUANTITY, double OL_AMOUNT, String OL_DIST_INFO, String OL_DELIVERY_D)
			throws KvException, TxnException {
		check_txn_exists();
		HashMap<String, Object> orderline = new HashMap<String, Object>();
		orderline.put("OL_O_ID", OL_O_ID);
		orderline.put("OL_D_ID", OL_D_ID);
		orderline.put("OL_W_ID", OL_W_ID);
		orderline.put("OL_NUMBER", OL_NUMBER);
		orderline.put("OL_I_ID", OL_I_ID);
		orderline.put("OL_SUPPLY_W_ID", OL_SUPPLY_W_ID);
		orderline.put("OL_QUANTITY", OL_QUANTITY);
		orderline.put("OL_AMOUNT", OL_AMOUNT);
		orderline.put("OL_DIST_INFO", OL_DIST_INFO);
		orderline.put("OL_DELIVERY_D", OL_DELIVERY_D);
		String val = Tables.encodeTable(orderline);
		int keys[] = {OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER};
		String key = Tables.encodeKey(TPCCConstants.TBL_OrderLine, keys);
		return kvi.insert(txn, key, val);
	}
	
	protected HashMap<String, Object> selectCustomer(int C_W_ID, int C_D_ID, int C_ID) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {C_W_ID, C_D_ID, C_ID};
		String key = Tables.encodeKey(TPCCConstants.TBL_Customer, keys);
		String res = kvi.get(txn, key);
		return Tables.decodeTable(res);
	}
	
	protected boolean updateCustomer(HashMap<String, Object> customer) throws KvException, TxnException {
		check_txn_exists();
		int C_W_ID = (int) customer.get("C_W_ID");
		int C_D_ID = (int) customer.get("C_D_ID");
		int C_ID = (int) customer.get("C_ID");
		int keys[] = {C_W_ID, C_D_ID, C_ID};
		String key = Tables.encodeKey(TPCCConstants.TBL_Customer, keys);
		String val = Tables.encodeTable(customer);
		return kvi.set(txn, key, val);
	}
	
	protected boolean insertCustomer(int C_ID, int C_D_ID, int C_W_ID, String C_FIRST, String C_MIDDLE, String C_LAST,
			String C_STREET_1, String C_STREET_2, String C_CITY, String C_STATE, String C_ZIP, String C_PHONE, String C_SINCE,
			String C_CREDIT, double C_CREDIT_LIM, double C_DISCOUNT, double C_BALANCE, double C_YTD_PAYMENT, int C_PAYMENT_CNT,
			int C_DELIVERY_CNT, String C_DATA) throws KvException, TxnException {
		check_txn_exists();
		HashMap<String, Object> customer = new HashMap<String, Object>();
		customer.put("C_ID", C_ID);
		customer.put("C_D_ID", C_D_ID);
		customer.put("C_W_ID", C_W_ID);
		customer.put("C_FIRST", C_FIRST);
		customer.put("C_MIDDLE", C_MIDDLE);
		customer.put("C_LAST", C_LAST);
		customer.put("C_STREET_1", C_STREET_1);
		customer.put("C_STREET_2", C_STREET_2);
		customer.put("C_CITY", C_CITY);
		customer.put("C_STATE", C_STATE);
		customer.put("C_ZIP", C_ZIP);
		customer.put("C_PHONE", C_PHONE);
		customer.put("C_SINCE", C_SINCE);
		customer.put("C_CREDIT", C_CREDIT);
		customer.put("C_CREDIT_LIM", C_CREDIT_LIM);
		customer.put("C_DISCOUNT", C_DISCOUNT);
		customer.put("C_BALANCE", C_BALANCE);
		customer.put("C_YTD_PAYMENT", C_YTD_PAYMENT);
		customer.put("C_PAYMENT_CNT", C_PAYMENT_CNT);
		customer.put("C_DELIVERY_CNT", C_DELIVERY_CNT);
		customer.put("C_DATA", C_DATA);
		int keys[] = {C_W_ID, C_D_ID, C_ID};
		String key = Tables.encodeKey(TPCCConstants.TBL_Customer, keys);
		String val = Tables.encodeTable(customer);
		return kvi.insert(txn, key, val);
	}
	
	// observation: the NewOrderList is always continuous, so here we just save the start and end OrderID (inclusive).
	protected boolean insertNewOrderList(int w_id, int d_id, int noid_from, int noid_to) throws KvException, TxnException {
		assert noid_from <= noid_to;
		check_txn_exists();
		int keys[] = {w_id, d_id};
		String key = Tables.encodeKey("NewOrderList", keys);
		ArrayList<String> nol = new ArrayList<>();
		nol.add(Integer.toString(noid_from));
		nol.add(Integer.toString(noid_to));
		String value = Tables.encodeList(nol);
		return kvi.insert(txn, key, value);
	}

	protected ArrayList<String> getNewOrderList(int w_id, int d_id) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {w_id, d_id};
		String key = Tables.encodeKey("NewOrderList", keys);
		String val = kvi.get(txn, key);
		ArrayList<String> neworders = Tables.decodeList(val);
		// neworders = [from, to]
		return neworders;
	}
	
	protected boolean updateNewOrderList(int w_id, int d_id, int no_id) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {w_id, d_id};
		String key = Tables.encodeKey("NewOrderList", keys);
		String val = kvi.get(txn, key);
		ArrayList<String> neworders = Tables.decodeList(val);
		int first_oid = Integer.parseInt(neworders.get(0));
		int last_oid = Integer.parseInt(neworders.get(1));
		assert last_oid + 1 == no_id && first_oid <= no_id;
		neworders.set(1, Integer.toString(no_id));
		return kvi.set(txn, key, Tables.encodeList(neworders));
	}

	protected boolean deleteFromNewOrderList(int w_id, int d_id, int o_id, ArrayList<String> neworders) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {w_id, d_id};
		String key = Tables.encodeKey("NewOrderList", keys);
		int first_oid = Integer.parseInt(neworders.get(0));
		int last_oid = Integer.parseInt(neworders.get(1));
		assert o_id == first_oid && o_id <= last_oid;
		neworders.set(0, Integer.toString(o_id+1));
		return kvi.set(txn, key, Tables.encodeList(neworders));
	}
	
	protected boolean insertCustomer2Index(int w_id, int d_id, String lastname, ArrayList<String> ids) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {w_id, d_id};
		String key = Tables.encodeKey("Lastname2Customer:"+lastname, keys);
		String id_string = Tables.encodeList(ids);
		return kvi.insert(txn, key, id_string);
	}
	
	protected ArrayList<String> getCustomer2Index(int w_id, int d_id, String lastname) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {w_id, d_id};
		String key = Tables.encodeKey("Lastname2Customer:"+lastname, keys);
		String res = kvi.get(txn, key);
		ArrayList<String> ids = Tables.decodeList(res);
		return ids;
	}
	
	protected HashMap<String, Object> selectItem(int I_ID) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {I_ID};
		String res = kvi.get(txn, Tables.encodeKey(TPCCConstants.TBL_Item, keys));
		return Tables.decodeTable(res);
	}
	
	protected boolean insertItem(int I_ID, int I_IM_ID, String I_NAME, double I_PRICE, String I_DATA) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {I_ID};
		HashMap<String, Object> item = new HashMap<String, Object>(); 
		item.put("I_ID", I_ID);
		item.put("I_IM_ID", I_IM_ID);
		item.put("I_NAME", I_NAME);
		item.put("I_PRICE", I_PRICE);
		item.put("I_DATA", I_DATA);
		String key = Tables.encodeKey(TPCCConstants.TBL_Item, keys);
		String val = Tables.encodeTable(item);
//		Logger.logDebug("Inserted to item - key: " + key + ", val: " + val);
		return kvi.insert(txn, key, val);
	}
	
	protected boolean insertOrder(int o_id, int o_d_id, int o_w_id, int o_c_id, String o_entry_d, int o_ol_cnt, boolean o_all_local) throws KvException, TxnException {
		check_txn_exists();
		HashMap<String, Object> newOrder = new HashMap<String, Object>(); 
		newOrder.put("O_ID", o_id);
		newOrder.put("O_D_ID", o_d_id);
		newOrder.put("O_W_ID", o_w_id);
		newOrder.put("O_C_ID", o_c_id);
		newOrder.put("O_ENTRY_D", o_entry_d);
		newOrder.put("O_OL_CNT", o_ol_cnt);
		newOrder.put("O_ALL_LOCAL", o_all_local);
		newOrder.put("O_CARRIER_ID", "");
		int keys[] = {o_w_id, o_d_id, o_id};
		String key = Tables.encodeKey(TPCCConstants.TBL_Order, keys);
		String val = Tables.encodeTable(newOrder);
		return kvi.insert(txn, key, val);
	}
	
	protected HashMap<String, Object> selectOrder(int o_w_id, int o_d_id, int o_id) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {o_w_id, o_d_id, o_id};
		String key = Tables.encodeKey(TPCCConstants.TBL_Order, keys);
		String res = kvi.get(txn, key);
		return Tables.decodeTable(res);
	}
	
	protected boolean updateOrder(HashMap<String, Object> order) throws KvException, TxnException {
		check_txn_exists();
		int o_w_id = (int) order.get("O_W_ID");
		int o_d_id = (int) order.get("O_D_ID");
		int o_id = (int) order.get("O_ID");
		int keys[] = {o_w_id, o_d_id, o_id};
		String key = Tables.encodeKey(TPCCConstants.TBL_Order, keys);
		String val = Tables.encodeTable(order);
		return kvi.set(txn, key, val);
	}
	
	protected boolean insertNewOrder(int o_id, int d_id, int w_id) throws KvException, TxnException {
		check_txn_exists();
		HashMap<String, Object> newOrder = new HashMap<String, Object>();
		newOrder.put("NO_O_ID", o_id);
		newOrder.put("NO_D_ID", d_id);
		newOrder.put("NO_W_ID", w_id);
		int keys[] = {w_id, d_id, o_id};
		String val = Tables.encodeTable(newOrder);
		String key = Tables.encodeKey(TPCCConstants.TBL_NewOrder, keys);
		return kvi.insert(txn, key, val);
	}
	
	protected HashMap<String, Object> selectNewOrder(int o_id, int d_id, int w_id) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {w_id, d_id, o_id};
		String key = Tables.encodeKey(TPCCConstants.TBL_NewOrder, keys);
		String res = kvi.get(txn, key);
		return Tables.decodeTable(res);
	}
	
	protected boolean deleteNewOrder(int o_id, int d_id, int w_id) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = {w_id, d_id, o_id};
		String key = Tables.encodeKey(TPCCConstants.TBL_NewOrder, keys);
		return kvi.set(txn, key, "");
//		return kvi.delete(txn, key);
	}
	
	protected boolean insertHistory(int H_C_ID, int H_C_D_ID, int H_C_W_ID, int H_D_ID, int H_W_ID, String H_DATE, double H_AMOUNT, String H_DATA) throws KvException, TxnException {
		check_txn_exists();
		HashMap<String, Object> hist = new HashMap<String, Object>();
		hist.put("H_C_ID", H_C_ID);
		hist.put("H_C_D_ID", H_C_D_ID); 
		hist.put("H_C_W_ID", H_C_W_ID); 
		hist.put("H_D_ID", H_D_ID);
		hist.put("H_W_ID", H_W_ID);
		hist.put("H_DATE", H_DATE);
		hist.put("H_AMOUNT", H_AMOUNT);
		hist.put("H_DATA", H_DATA);

		int keys[] = {history_id.incrementAndGet()};
		String key = Tables.encodeKey(TPCCConstants.TBL_History, keys);
		String val = Tables.encodeTable(hist);
		return kvi.insert(txn, key, val);
	}

}
