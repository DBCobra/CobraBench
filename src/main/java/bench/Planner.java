package bench;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.rocksdb.RocksDB;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import bench.Crack.ROAnomalyBench;
import bench.Crack.WOROAnomalyBench;
import bench.Crack.WriteSkewBench;
import bench.chengTxn.ChengBench;
import bench.rubis.RubisBench;
import bench.tpcc.TPCCBench;
import bench.twitter.TwitterBench;
import bench.ycsbt.YCSBTBench;
import kv_interfaces.CockroachDB;
import kv_interfaces.InstKV;
import kv_interfaces.KvInterface;
import kv_interfaces.RocksDBKV;
import kv_interfaces.SqlKV;
import kv_interfaces.instrument.ChengClientState;
import kv_interfaces.instrument.SignatureManager;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Config;
import main.Logger;
import main.Profiler;

public class Planner {
	private static Planner instance = null;
	private Benchmark bench;
	private BlockingQueue<Transaction> taskq;
	private ArrayList<Report> reports;
	private ArrayList<TaskRunner> threadPool;
	private final KvInterface kvi;

	// Thread safe efficient singleton
	public static Planner getInstance() {
		if (instance == null) {
			synchronized (Planner.class) {
				if (instance == null) {
					instance = new Planner();
				}
			}
		}
		return instance;
	}

	private Planner() {
		this.taskq = new ArrayBlockingQueue<>(1000);
		this.threadPool = new ArrayList<>();
		this.reports = new ArrayList<>();
		for (int i = 0; i < Config.get().THREAD_NUM; i++) {
			reports.add(new Report());
		}
		
		this.kvi = Benchmark.getKvi(Config.get().LIB_TYPE, Config.get().USE_INSTRUMENT);
		switch (Config.get().BENCH_TYPE) {
		case CHENG:
			this.bench = new ChengBench(kvi);
			break;
		case TPCC:
			this.bench = new TPCCBench(kvi);
			break;
		case YCSB:
			this.bench = new YCSBTBench(kvi);
			break;
		case RUBIS:
			this.bench = new RubisBench(kvi);
			break;
		case TWITTER:
			this.bench = new TwitterBench(kvi);
			break;
		case WRITESKEW:
			this.bench = new WriteSkewBench(kvi);
			break;
		case ROANOMALY:
			this.bench = new ROAnomalyBench(kvi);
			break;
		case WOROANOMALY:
			this.bench = new WOROAnomalyBench(kvi);
			break;
		default:
			assert false;
			break;
		}
	}

	private void initThreadPool() {
		for (int i = 0; i < Config.get().THREAD_NUM; i++) {
			// register task runners
			TaskRunner tr = new TaskRunner(taskq, reports.get(i), true); // these are the real client worker threads
			threadPool.add(tr);
			// start task runners
			tr.start();
		}
	}

	private class loadRunner extends Thread {
		private ArrayList<Transaction> txns;
		public loadRunner() {
			this.txns = new ArrayList<>();
			return;
		}
		public void addTask(Transaction t) {
			txns.add(t);
		}
		
		@Override
		public void run() {
			KvInterface kvi = Benchmark.getKvi(main.Config.get().LIB_TYPE, false);
			for(Transaction t : txns) {
				if(Config.get().DONT_RECORD_LOADING) {
					t.setNewKvi(kvi);
				}
				t.inputGeneration();
				try {
					t.doTansaction();
				} catch (KvException | TxnException e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}
		}
	}

	public void preBench() {
		// bootstrap
		initThreadPool();
		if (Config.get().SIGN_DATA) {
			SignatureManager.getInstance().start();
		}

		if (!Config.get().SKIP_LOADING) {
			// Do pre-benchmark db-related work
			int loadThreadNum = 64;
			ArrayList<loadRunner> runners = new ArrayList<>();
			for (int i = 0; i < loadThreadNum; i++) {
				runners.add(new loadRunner());
			}
			Transaction preBenchTxns[] = bench.preBenchmark();
			for (int i = 0; i < preBenchTxns.length; i++) {
				runners.get(i % loadThreadNum).addTask(preBenchTxns[i]);
			}
			for (Thread r : runners) {
				r.start();
			}
			for (Thread r : runners) {
				try {
					r.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}
		}
		
		KvInterface kvi1 = kvi;
		if (kvi instanceof InstKV) {
			kvi1 = ((InstKV)kvi).kvi;
		}

		if (kvi1 instanceof RocksDBKV) {
			RocksDBKV rockskvi = (RocksDBKV)kvi1;
			rockskvi.numOp.set(0);
		} else if (kvi1 instanceof SqlKV) {
			SqlKV sqlkvi = (SqlKV)kvi1;
			sqlkvi.clearNumOp();
		}
	}

	private void waitForRunners() {
		int total_txn = Config.get().TXN_NUM;
		ArrayList<TaskRunner> threadPool1 = (ArrayList<TaskRunner>) threadPool.clone();
		int last_complete = 0;
		long last_time = System.currentTimeMillis();
		while (!threadPool1.isEmpty()) {
			// sleep for a while for each round of checking
			try {
				Thread.sleep(Config.get().SLEEP_TIME);
			} catch (InterruptedException e) {
				Logger.logError(e.getMessage());
			}

			int commit_txn = 0;
			int abort_txn = 0;

			for (TaskRunner tr : threadPool) {
				commit_txn += tr.commitcounter.get();
				abort_txn += tr.abortcounter.get();
			}

			int complete_txn = commit_txn + abort_txn;
			DecimalFormat df = new DecimalFormat();
			df.setMaximumFractionDigits(2);

//			if (complete_txn > total_txn * current_progress) {
//				System.out.println("progress%=" + df.format((((double) complete_txn) / total_txn) * 100) + "%"
//						+ " #commit=" + commit_txn + " #abort_txn=" + abort_txn + "  success%="
//						+ df.format((((double) commit_txn) / complete_txn) * 100) + "%");
//				current_progress += 0.1;
//			}
			
			long this_time = System.currentTimeMillis();
			System.out.println("progress%=" + df.format((((double) complete_txn) / total_txn) * 100) + "%" + " #commit="
					+ commit_txn + " #abort_txn=" + abort_txn + " success%="
					+ df.format((((double) commit_txn) / complete_txn) * 100) + "%" + " tps = "
					+ df.format((double) (complete_txn - last_complete) * 1000 / (this_time - last_time)));

			last_complete = complete_txn;
			last_time = this_time;
			
			// go through all the runners
			TaskRunner[] tr = threadPool1.toArray(new TaskRunner[0]);
			int alive_runners = tr.length;
			for (int i = 0; i < tr.length; i++) {
				// remove from runners if it is not alive
				if (!tr[i].isAlive()) {
					threadPool1.remove(tr[i]);
					alive_runners --;
				}
			}
			System.out.println("  #runners = " + alive_runners);
			if (this.kvi instanceof InstKV && ((InstKV)this.kvi).kvi instanceof CockroachDB) {
				System.out.println("  " + ((CockroachDB)((InstKV)this.kvi).kvi).Status());
			}
		}
	}

	private class Producer extends Thread {
		private int wait_time = Config.get().WAIT_BETWEEN_TXNS;
		private int batch_txn_amount = Config.get().THROUGHPUT_PER_WAIT;
		
		@Override
		public void run() {
			// start feeding tasks to task runners
			for (int i = 0; i < Config.get().TXN_NUM; i++) {
				try {
					Transaction nxt = bench.getNextTxn();
					taskq.put(nxt);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				if (wait_time > 0 && (i + 1) % batch_txn_amount == 0) {
					try {
						Thread.sleep(wait_time);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}

			// send END signals to all
			for (int i = 0; i < Config.get().THREAD_NUM; i++) {
				try {
					taskq.put(new Transaction.EndSignal());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			Logger.logDebug("Producer: all transactions are sent");
		}
	}

	public void doBench() {
		Profiler.getInstance().startTick("doBench");
		Producer p = new Producer();
		p.start();
		waitForRunners();
		Profiler.getInstance().endTick("doBench");
	}

	public void afterBench() {
		// do clean-up stuff here
	}

	public String getResult() {
		DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(2);
		StringBuilder sb = new StringBuilder();
		
		String[] tags = Profiler.getTags();
		
		int commit_txn = 0;
		int abort_txn = 0;

		for(TaskRunner tr : threadPool) {
			commit_txn += tr.commitcounter.get();
			abort_txn += tr.abortcounter.get();
		}
		
//		for (int i = 0; i < reports.size(); i++) {
//			Report report = reports.get(i);
//			sb.append(report.getReport());
//		}

		int total_txn =  commit_txn + abort_txn;
		
		sb.append("========Summary=============\n");
		sb.append("#Txn="+total_txn + " #commit=" + commit_txn + " #abort_txn=" + abort_txn
				+ "  success%="+ df.format((((double)commit_txn)/total_txn) * 100) + "%\n");
		sb.append("\n======Performance=========\n");
		
		// throughput
		long total_runtime = Profiler.getInstance().getTime("doBench");
		double throughput = commit_txn / (total_runtime / 1000000000.0);
		sb.append("total runtime: " + df.format(total_runtime / 1000000.0) + "ms \n"); 
		sb.append("Throughput: " + df.format(throughput) + " txn/sec (commit txn)\n");
		
		// latency
		sb.append("Latency:\n");
		
		for (int i=0; i<tags.length; i++) {
			String tag = tags[i];
			long time = Profiler.getAggTime(tag);
			int counter = Profiler.getAggCount(tag);
			double latency = (counter==0) ? 0 : ((double)time) / counter;
			sb.append("  " + tag + ": " + df.format(latency/1000000) + "ms  [" + time/1000000 + "ms/" + df.format(counter/1000.0) + "k]\n");
		}
		
		// Other statistics: number of operation, size of trace
		if (kvi instanceof SqlKV) {
			SqlKV sqlkvi = (SqlKV) kvi;
			sb.append("NumOp: " + sqlkvi.getNumOp() + "\n");
		} else if (kvi instanceof RocksDBKV) {
			RocksDBKV rockskvi = (RocksDBKV) kvi;
			sb.append("NumOp: " + rockskvi.getNumOp() + "\n");
		} else if (kvi instanceof InstKV) {
			InstKV instkvi = (InstKV) kvi;
			if (instkvi.kvi instanceof SqlKV) {
				SqlKV sqlkvi = (SqlKV) instkvi.kvi;
				sb.append("NumOp: " + sqlkvi.getNumOp() + "\n");
			} else if (instkvi.kvi instanceof RocksDBKV) {
				RocksDBKV rockskvi = (RocksDBKV) instkvi.kvi;
				sb.append("NumOp: " + rockskvi.getNumOp() + "\n");
			}
			sb.append("ExtraOp: " + ChengClientState.getExtraOps() + "; " +
					"ExtraTxn: " + ChengClientState.getExtraTxns() + "; " +
					"NumTxnInMonitoring: " + ChengClientState.getTxnsInMonitoring() + "\n");
			sb.append("SizeOfTrace: " + instkvi.getTraceSize() + "\n");
		}

		return sb.toString();
	}
	
	private void waitBarrier() {
		RedisClient redisClient = RedisClient.create(Config.get().REDIS_ADDRESS);
		StatefulRedisConnection<String, String> connection = redisClient.connect();
		RedisCommands<String, String> syncCommands = connection.sync();
//		syncCommands.auth("cobra318<>");
		String key = "cobra_clients";
		syncCommands.incr(key);
		String val;
		do {
			val = syncCommands.get(key);
		} while(!val.equals("start"));
		
		connection.close();
		redisClient.shutdown();
	}

	public static void standardProcedure() {
		Planner p = Planner.getInstance();
		p.preBench();
		if(!Config.get().SKIP_TESTING) {
			System.out.println("waiting for barrier");
			if(Config.get().ENABLE_BARRIER) {
				p.waitBarrier();
			}
			System.out.println("start benchmark");
			p.doBench();
			System.out.println("benchmark finished");
			p.afterBench();
			System.out.println("Result: ");
			System.out.println(p.getResult());
			System.out.println("Dumping latencies...");
			int clientId = Config.get().CLIENT_ID;
			for (int i = 0; i < p.reports.size(); i++) {
				Report report = p.reports.get(i);
				try {
					report.dumpLats(Config.get().LATENCY_FOLDER + "report"+clientId+"_"+i+".txt");
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}
			
			try {
				File directory = new File(Config.get().RESULT_FILE_NAME).getParentFile();
				if (directory != null && !directory.exists()){
					directory.mkdirs();
				}
				FileWriter fw = new FileWriter(Config.get().RESULT_FILE_NAME);
				fw.write(Config.get().toString() + "\n");
				fw.write(p.getResult());
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
		System.out.println("[SIGNAL] The test is finished! [rejungofszbj]");
	}
}
