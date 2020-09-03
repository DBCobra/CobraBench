package bench.Crack;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import bench.BenchUtils;
import bench.Benchmark;
import bench.Transaction;
import kv_interfaces.CockroachDB;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Config;

public class WriteSkewBench extends Benchmark {
	
  public static final String TABLE_A = "wskewa";
  public static final String TABLE_B = "wskewb";
	public static final int SLEEP_IN_MS = 200;
	public static final int NUM_KEY_PERTABLE = Config.get().NUM_KEY_PERTABLE;
	public static final int ACTIVE_ABORT_RATE = Config.get().CRACK_ACTIVE_ABORT_RATE;
	int counter = 0;

	public WriteSkewBench(KvInterface kvi) {
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
		
		return new WriteSkewTransction(kvi, TABLE_A, TABLE_B, NUM_KEY_PERTABLE, rand.nextInt(2));
	}

	@Override
	public void afterBenchmark() {
	}

	@Override
	public String[] getTags() {
		return null;
	}
	
	
	
	// ==== helper functions ======
	
	public static String encodeKey(String table, String key) {
		return table + "###" + key;
	}
	
	static AtomicInteger content_counter = new AtomicInteger(0);
	static String RandString() {
		// TODO: return two types of strings; one is really large; the other is really small.
		
		/*
		int size = 10000;
		Random rand = new Random();
		if (rand.nextInt(100) > 50) {
			size = 60000;
		}
		return BenchUtils.getRandomValue(size);
		*/
	
		return "V[" + content_counter.getAndIncrement() + "]";
	}
	
	// === two types of transactions ======
	
	static class InitTransction extends Transaction {
		private int num_keys;
		private String tableA;
		private String tableB;

		public InitTransction(KvInterface kvi, String tableA, String tableB, int num_keys) {
			super(kvi, "init");
			this.num_keys = num_keys;
			this.tableA = tableA;
			this.tableB = tableB;
		}

		@Override
		public void inputGeneration() {}

		@Override
		public boolean doTansaction() throws KvException, TxnException {
			beginTxn();
			// create two keys in table A&B
			for (int i=0; i<num_keys; i++) {
				kvi.insert(txn, encodeKey(tableA, "key"+i), "init");
				kvi.insert(txn, encodeKey(tableB, "key"+i), "init");
			}
			commitTxn();
			return true;
		}
	}
	
	static class WriteSkewTransction extends Transaction {
		protected int num_keys;
		protected String tableA;
		protected String tableB;

		public WriteSkewTransction(KvInterface kvi, String tableA, String tableB, int num_keys, int choice) {
			super(kvi, "writeSkew");
			this.num_keys = num_keys;
			assert choice < 2 && choice >= 0;
			if (choice == 0) {
				this.tableA = tableA;
				this.tableB = tableB;
			} else {
				this.tableA = tableB;
				this.tableB = tableA;
			}
		}

		@Override
		public void inputGeneration() {
		}

		@Override
		public boolean doTansaction() throws KvException, TxnException {
			Random rand = new Random();
			
			beginTxn();
			// read one key from tableA
			String r_key = encodeKey(tableA, "key"+rand.nextInt(num_keys));
			String v = kvi.get(txn, r_key);
			System.out.println("[INFO] read " + r_key + " => String.size=" + v.length());
			// wait sometime
			try {
				Thread.sleep(rand.nextInt(SLEEP_IN_MS));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// write one key to tableB
			String w_key = encodeKey(tableB, "key"+rand.nextInt(num_keys));
			String w_val = RandString();
			System.out.println("[INFO] write " + w_key + " <= String.size=" + w_val.length());
			kvi.set(txn, w_key, w_val);
			
			// decide we should active abort?
			if (rand.nextInt(100) >= ACTIVE_ABORT_RATE) {
				commitTxn();
			} else {
				abortTxn();
			}
			return true;
		}
		
	}

}
