package bench.chengTxn;

import bench.Transaction;
import bench.BenchUtils;
import bench.chengTxn.ChengTxnConstants.TASK_TYPE;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Logger;
import main.Profiler;

public class ChengTransaction extends Transaction {
	private TASK_TYPE taskType;
	private int opsPerTxn;
	private String keys[];

	public ChengTransaction(KvInterface kvi, TASK_TYPE taskType, String keys[], int opsPerTxn) {
		super(kvi, getOpTag(taskType));
		this.taskType = taskType;
		this.opsPerTxn = opsPerTxn;
		this.keys = keys;
		switch (taskType) {
		case INSERT: this.txnName = ChengTxnConstants.TXN_INSERT_TAG; break;
		case UPDATE: this.txnName = ChengTxnConstants.TXN_UPDATE_TAG; break;
		case READ: this.txnName = ChengTxnConstants.TXN_READ_TAG; break;
		case READ_MODIFY_WRITE: this.txnName = ChengTxnConstants.TXN_RMW_TAG; break;
		case DELETE: this.txnName = ChengTxnConstants.TXN_DELETE_TAG; break;
		default: this.txnName = "wrong_txn_type"; break;
		}
	}

	@Override
	public void inputGeneration() {
		// do nothing
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		Profiler.getInstance().startTick("kvi");
		beginTxn();
		Profiler.getInstance().endTick("kvi");;
		int key_counter = 0;
		for (int i = 0; i < opsPerTxn; i++) {
			String key = keys[key_counter++];
			Profiler.getInstance().startTick("kvi");
			String val = "defaultValue";
			switch (taskType) {
			case INSERT:
				val = BenchUtils.getRandomValue();
				kvi.insert(txn, key, val);
				break;
			case UPDATE:
				val = BenchUtils.getRandomValue();
				kvi.set(txn, key, val);
				break;
			case READ:
				kvi.get(txn, key);
				break;
			case READ_MODIFY_WRITE:
				String v = kvi.get(txn, key);
				v += "M";
				kvi.set(txn, key, v);
				i++;
				break;
			case DELETE:
				break;
			default:
				assert false;
				break;
			}
			Profiler.getInstance().endTick("kvi");;
		}
		Profiler.getInstance().startTick("kvi");
		commitTxn();
		Profiler.getInstance().endTick("kvi");;
		return true;
	}

	private static String getOpTag(TASK_TYPE op) {
		String tag = "";
		switch (op) {
		case INSERT:
			tag = ChengTxnConstants.TXN_INSERT_TAG;
			break;
		case READ:
			tag = ChengTxnConstants.TXN_READ_TAG;
			break;
		case UPDATE:
			tag = ChengTxnConstants.TXN_UPDATE_TAG;
			break;
		case DELETE:
			tag = ChengTxnConstants.TXN_DELETE_TAG;
			break;
		case READ_MODIFY_WRITE:
			tag = ChengTxnConstants.TXN_RMW_TAG;
			break;
		default:
			Logger.logError("UNKOWN TASK_TYPE[" + op + "]");
			break;
		}
		return tag;
	}
}
