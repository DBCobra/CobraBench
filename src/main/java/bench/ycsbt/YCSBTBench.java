package bench.ycsbt;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import bench.Benchmark;
import bench.Transaction;
import bench.chengTxn.ChengTransaction;
import bench.ycsbt.YCSBTConstants.TASK_TYPE;
import kv_interfaces.KvInterface;
import main.Config;

public class YCSBTBench extends Benchmark {
	private AtomicInteger keyNum = new AtomicInteger(Config.get().KEY_INDX_START);
	private int totalCash = 1000;

	public YCSBTBench(KvInterface kvi) {
		super(kvi);
	}

	@Override
	public Transaction[] preBenchmark() {
		// Fill in some keys
		int num_txn = Config.get().NUM_KEYS;
		Transaction ret[] = new Transaction[num_txn];
		for (int i = 0; i < num_txn; i++) {
			ret[i] = getTheTxn(TASK_TYPE.INSERT);
		}
		return ret;
	}

	private TASK_TYPE nextOperation() {
		int diceSum = Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE
				+ Config.get().RATIO_DELETE + Config.get().RATIO_RMW;
		int dice = YCSBTUtils.RndIntRange(1, diceSum);
		if (dice <= Config.get().RATIO_INSERT) {
			return TASK_TYPE.INSERT;
		} else if (dice <= Config.get().RATIO_INSERT + Config.get().RATIO_READ) {
			return TASK_TYPE.READ;
		} else if (dice <= Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE) {
			return TASK_TYPE.UPDATE;
		} else if (dice <= Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE
				+ Config.get().RATIO_DELETE) {
			return TASK_TYPE.DELETE;
		} else {
			return TASK_TYPE.READ_MODIFY_WRITE;
		}
	}

	private String getNewKey() {
		int cur_indx = keyNum.getAndIncrement();
		return Config.get().KEY_PRFIX + cur_indx;
	}

	private String getExistingKey() {
		return Config.get().KEY_PRFIX + (Config.get().KEY_INDX_START + YCSBTUtils.zipfian());
	}

	private String[] getExistingKeys(int num) {
		HashSet<String> visited = new HashSet<>();
		String k = null;
		for (int i = 0; i < num; i++) {
			do {
				k = getExistingKey();
			} while (visited.contains(k));
			visited.add(k);
		}
		return visited.toArray(new String[0]);
	}

	private Transaction getTheTxn(TASK_TYPE op) {
		String keys[] = null;
		switch (op) {
		case INSERT:
			keys = new String[1];
			keys[0] = getNewKey();
			break;
		case UPDATE:
		case READ:
		case DELETE:
			keys = new String[1];
			keys[0] = getExistingKey();
			break;
		case READ_MODIFY_WRITE:
			keys = new String[2];
			keys[0] = getExistingKey();
			keys[1] = getExistingKey();
			break;
		case SCAN:
			keys = new String[10];
			for (int i = 0; i < keys.length; i++) {
				keys[i] = getExistingKey();
			}
			break;
		default:
			assert false;
			break;
		}
		YCSBTTransaction nextTxn = new YCSBTTransaction(kvi, op, keys);
		return nextTxn;
	}

	@Override
	public Transaction getNextTxn() {
		TASK_TYPE op = nextOperation();
		return getTheTxn(op);
	}

	@Override
	public void afterBenchmark() {
		assert false;
		// TODO Auto-generated method stub

	}

	@Override
	public String[] getTags() {
		return new String[] { YCSBTConstants.TXN_READ_TAG, YCSBTConstants.TXN_INSERT_TAG, YCSBTConstants.TXN_RMW_TAG,
				YCSBTConstants.TXN_UPDATE_TAG, YCSBTConstants.TXN_SCAN_TAG, YCSBTConstants.TXN_DELETE_TAG };
	}

}
