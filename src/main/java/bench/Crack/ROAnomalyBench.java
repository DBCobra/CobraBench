package bench.Crack;

import java.util.Random;

import bench.Benchmark;
import bench.Transaction;
import bench.Crack.WriteSkewBench.InitTransction;
import bench.Crack.WriteSkewBench.WriteSkewTransction;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Config;

public class ROAnomalyBench extends Benchmark {
	
	// FIXME: copied from WriteSkewBench
  public static final String TABLE_A = WriteSkewBench.TABLE_A;
  public static final String TABLE_B = WriteSkewBench.TABLE_B;
	public static final int NUM_KEY_PERTABLE = Config.get().NUM_KEY_PERTABLE;
	public static final int SLEEP_IN_MS = 200;
	public static final int ACTIVE_ABORT_RATE = Config.get().CRACK_ACTIVE_ABORT_RATE;
	int counter = 0;

	public ROAnomalyBench(KvInterface kvi) {
		super(kvi);
	}

	@Override
	public Transaction[] preBenchmark() {
		Transaction[] init = new Transaction[1];
		init[0] = new InitTransction(kvi, TABLE_A, TABLE_B, NUM_KEY_PERTABLE);
		return init;
	}

	@Override
	public Transaction getNextTxn() {
		counter++;
		Random rand = new Random();
		int type = rand.nextInt(Config.get().ROANOMALY_NUM_RO + 2);
		// .5 to choose RO
		if (type >= 2) {
			return new ReadOnlyTransaction(kvi, TABLE_A, TABLE_B, NUM_KEY_PERTABLE);
		} else {
			// type == 0 or 1
			return new WriteSkewTransction(kvi, TABLE_A, TABLE_B, NUM_KEY_PERTABLE, type);
		}
	}

	@Override
	public void afterBenchmark() {
	}

	@Override
	public String[] getTags() {
		return null;
	}
	
	// ====
	static class ReadOnlyTransaction extends Transaction {
		
		private String tableA, tableB;
		private int num_keys;

		public ReadOnlyTransaction(KvInterface kvi, String tableA, String tableB, int num_keys) {
			super(kvi, "ROAnomialy");
			this.tableA = tableA;
			this.tableB = tableB;
			this.num_keys = num_keys;
		}

		@Override
		public void inputGeneration() {
		}

		@Override
		public boolean doTansaction() throws KvException, TxnException {
			Random rand = new Random();
			beginTxn();
			String[] tables = {tableA, tableB};
			for (String table : tables) {
				for (int i = 0; i < num_keys; i++) {
					// read one key from tableA
					String r_key = WriteSkewBench.encodeKey(table, "key" + i);
					String v = kvi.get(txn, r_key);
					System.out.println("[INFO] read " + r_key + " => String.size=" + v.length());
				}
			}
			// decide we should active abort?
			if (rand.nextInt(100) >= ACTIVE_ABORT_RATE) {
				commitTxn();
			} else {
				abortTxn();
			}
			// wait sometime afterwards
			try {
				Thread.sleep(rand.nextInt(SLEEP_IN_MS));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return true;
		}
		
	}

}
