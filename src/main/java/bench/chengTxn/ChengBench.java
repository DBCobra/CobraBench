package bench.chengTxn;

import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import bench.Benchmark;
import bench.Transaction;
import bench.BenchUtils;
import bench.chengTxn.ChengTxnConstants.TASK_TYPE;
import kv_interfaces.KvInterface;
import main.Config;

public class ChengBench extends Benchmark {
	private static AtomicInteger keyNum;
	private Random rand = new Random(Config.get().SEED);

	public ChengBench(KvInterface kvi) {
		super(kvi);
		keyNum = new AtomicInteger(Config.get().KEY_INDX_START);
		if(Config.get().SKIP_LOADING) {
			keyNum.addAndGet(Config.get().NUM_KEYS);
		}
	}

	@Override
	public Transaction[] preBenchmark() {
		// TODO need to clear the database

		// Fill in some keys
		int num_txn = (int) Math.ceil(((double) Config.get().NUM_KEYS) / Config.get().OP_PER_CHENGTXN);
		Transaction ret[] = new Transaction[num_txn];
		for (int i = 0; i < num_txn; i++) {
			ret[i] = getTheTxn(TASK_TYPE.INSERT);
		}
		return ret;
	}

	private TASK_TYPE nextOperation() {
		int dice = rand.nextInt(Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE
				+ Config.get().RATIO_DELETE + Config.get().RATIO_RMW);
		if (dice < Config.get().RATIO_INSERT) {
			return ChengTxnConstants.TASK_TYPE.INSERT;
		} else if (dice < Config.get().RATIO_INSERT + Config.get().RATIO_READ) {
			return ChengTxnConstants.TASK_TYPE.READ;
		} else if (dice < Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE) {
			return ChengTxnConstants.TASK_TYPE.UPDATE;
		} else if (dice < Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE
				+ Config.get().RATIO_DELETE) {
			return ChengTxnConstants.TASK_TYPE.DELETE;
		} else {
			return ChengTxnConstants.TASK_TYPE.READ_MODIFY_WRITE;
		}
	}

	private String getNewKey() {
		int cur_indx = keyNum.getAndIncrement();
		return Config.get().KEY_PRFIX + cur_indx;
	}

	private String getExistingKey() {
		int cur_indx = keyNum.get();
		return Config.get().KEY_PRFIX + BenchUtils.getRandomInt(Config.get().KEY_INDX_START, cur_indx);
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

	public ChengTransaction getTheTxn(TASK_TYPE op) {
		String keys[] = null;
		switch (op) {
		case INSERT:
			keys = new String[Config.get().OP_PER_CHENGTXN];
			for (int i = 0; i < keys.length; i++) {
				keys[i] = getNewKey();
			}
			break;
		case UPDATE:
		case READ:
		case DELETE:
		case READ_MODIFY_WRITE:
			keys = getExistingKeys(Config.get().OP_PER_CHENGTXN);
			break;
		default:
			assert false;
			break;
		}
		ChengTransaction nextTxn = new ChengTransaction(kvi, op, keys, Config.get().OP_PER_CHENGTXN);
		return nextTxn;
	}

	@Override
	public Transaction getNextTxn() {
		TASK_TYPE op = nextOperation();
		return getTheTxn(op);
	}

	@Override
	public void afterBenchmark() {
		// TODO Auto-generated method stub

	}

	@Override
	public String[] getTags() {
		return new String[] { ChengTxnConstants.TXN_INSERT_TAG, ChengTxnConstants.TXN_DELETE_TAG,
				ChengTxnConstants.TXN_READ_TAG, ChengTxnConstants.TXN_RMW_TAG, ChengTxnConstants.TXN_UPDATE_TAG, "kvi"};
	}

}
