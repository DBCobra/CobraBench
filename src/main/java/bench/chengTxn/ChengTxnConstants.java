package bench.chengTxn;

public class ChengTxnConstants {
	// task type
	enum TASK_TYPE {
		INSERT, READ, UPDATE, DELETE, READ_MODIFY_WRITE,
	}

	// profiling related
	public final static String TXN_READ_TAG = "txnread";
	public final static String TXN_INSERT_TAG = "txninsert";
	public final static String TXN_UPDATE_TAG = "txnupdate";
	public final static String TXN_DELETE_TAG = "txndelete";
	public final static String TXN_RMW_TAG = "txnrmw";

}