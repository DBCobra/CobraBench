package bench.tpcc;

public class TPCCConstants {
	
	public static enum TXN_TYPE {TXN_NEWORDER, TXN_PAYMENT, TXN_ORDERSTATUS, TXN_DELIVERY, TXN_STOCKLEVEL};

	// TPCC constants, DO NOT CHANGE
	public final static int MAXITEMS = 100000;
	public final static int CUST_PER_DIST = 3000;
	public final static int DIST_PER_WARE = 10;
	public final static int ORD_PER_DIST = 3000;
	
	// Table prefixes
	public final static String TBL_Warehouse = "WAREHOUSE";
	public final static String TBL_District = "DISTRICT";
	public final static String TBL_Customer = "CUSTOMER";
	public final static String TBL_History = "HISTORY";
	public final static String TBL_NewOrder = "NEWORDER";
	public final static String TBL_Order = "ORDER";
	public final static String TBL_OrderLine = "ORDERLINE";
	public final static String TBL_Item = "ITEM";
	public final static String TBL_Stock = "STOCK";
}
