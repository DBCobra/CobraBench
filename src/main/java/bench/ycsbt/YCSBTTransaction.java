package bench.ycsbt;

import bench.Transaction;
import bench.BenchUtils;
import bench.ycsbt.YCSBTConstants.TASK_TYPE;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Logger;

public class YCSBTTransaction extends Transaction {

	private TASK_TYPE taskType;
	private String keys[];

	public YCSBTTransaction(KvInterface kvi, TASK_TYPE taskType, String keys[]) {
		super(kvi, getOpTag(taskType));
		this.taskType = taskType;
		this.keys = keys;
	}

	@Override
	public void inputGeneration() {
		// do nothing
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();
		int key_counter = 0;
		String key = keys[key_counter++];
		String val = "defaultValue";
		switch (taskType) {
		case INSERT:
			val = BenchUtils.getRandomValue();
			kvi.insert(txn, key, val);
			break;
		case UPDATE:
			String v = kvi.get(txn, key);
			v = BenchUtils.shuffleString(v);
			kvi.set(txn, key, v);
			break;
		case READ:
			kvi.get(txn, key);
			break;
		case READ_MODIFY_WRITE:
			String key2 = keys[key_counter++];
			String v1 = kvi.get(txn, key);
			String v2 = kvi.get(txn, key2);
			kvi.set(txn, key, v2);
			kvi.set(txn, key2, v1);
			break;
		case DELETE:
			kvi.delete(txn, key);
			break;
		default:
			assert false;
			break;
		}
		commitTxn();
		return true;
	}

	private static String getOpTag(TASK_TYPE op) {
		String tag = "";
		switch (op) {
		case INSERT:
			tag = YCSBTConstants.TXN_INSERT_TAG;
			break;
		case READ:
			tag = YCSBTConstants.TXN_READ_TAG;
			break;
		case UPDATE:
			tag = YCSBTConstants.TXN_UPDATE_TAG;
			break;
		case DELETE:
			tag = YCSBTConstants.TXN_DELETE_TAG;
			break;
		case READ_MODIFY_WRITE:
			tag = YCSBTConstants.TXN_RMW_TAG;
			break;
		case SCAN:
			tag = YCSBTConstants.TXN_SCAN_TAG;
			break;
		default:
			tag = "unkonwn";
			Logger.logError("UNKOWN TASK_TYPE[" + op + "]");
			break;
		}
		return tag;
	}
}
