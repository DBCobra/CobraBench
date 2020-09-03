package bench;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.postgresql.util.PSQLException;

import bench.Transaction.EndSignal;
import kv_interfaces.CockroachDB;
import kv_interfaces.YugaByteDB;
import kv_interfaces.instrument.ChengInstrumentAPI;
import kv_interfaces.instrument.ChengLogger;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Config;
import main.Logger;
import main.Profiler;

public class TaskRunner extends Thread {
	private BlockingQueue<Transaction> task_queue;
	private Report report;
	public AtomicInteger commitcounter = new AtomicInteger(0);
	public AtomicInteger abortcounter = new AtomicInteger(0);
	public boolean client_thread = false;

	public TaskRunner(BlockingQueue<Transaction> taskq, Report report) {
		this.task_queue = taskq;
		this.report = report;
	}
	
	public TaskRunner(BlockingQueue<Transaction> taskq, Report report, boolean client_thread) {
		this(taskq, report);
		this.client_thread = client_thread;
	}
	
	private void innerRollbackAndReconnect(Transaction txn) {
		System.out.println("[ERROR] server shutting down experienced! reconnecting to other nodes.");
		// FIXME: break the abstraction; but we need to notify the underlying log that we've aborted
		ChengInstrumentAPI.doTransactionRollback(txn.kvi.getTxnId(txn.txn));
		// report the node is down; connect to another node next time
		// FIXME: ugly
		if (Config.get().LIB_TYPE == Config.LibType.COCKROACH_LIB) {
			CockroachDB.getInstance().ReportServerDown();
		} else if (Config.get().LIB_TYPE == Config.LibType.YUGABYTE_LIB) {
			YugaByteDB.getInstance().ReportServerDown();
		} else {
			System.err.println("[ERROR] should never be here");
			System.exit(-1);
		}
	}

	@Override
	public void run() {
		if (client_thread) {
			ChengLogger.getInstance().registerClient();
		}
		Profiler p = Profiler.getInstance();
		Transaction txn = null;
		boolean reportNewOrderOnly = Config.get().REPORT_NEWORDER_ONLY;
		Config.BenchType benchType = Config.get().BENCH_TYPE;
		try {
			while (!((txn = task_queue.take()) instanceof EndSignal)) {
				txn.inputGeneration();
				long t1 = p.startTick(txn.getname());
				try {
					boolean success = txn.doTansaction();
					if (success) {
						commitcounter.incrementAndGet();
						long t2 = p.endTick(txn.getname());
						
						if (benchType == Config.BenchType.TPCC) {
							if (!reportNewOrderOnly || txn.getname().equals("TPCC-NewOrder")) {
								report.addLat(t1, t2);
							}
						} else {
							report.addLat(t1, t2);
						}
					} else {
						abortcounter.incrementAndGet();
						p.endTick(txn.getname());
					}
				} catch (KvException | TxnException e) {
					ChengLogger.getInstance().debug("[ERROR] [KvException|TxnException] Tid[" + getId() + "] msg[" + e.toString() + "]");
					assert e != null;
					// for CockroachDB when killing the server
					if (e.toString().contains("server is shutting down") ||
							e.toString().contains("An I/O error occurred while sending to the backend")) {
						innerRollbackAndReconnect(txn);
					} else if (e.toString().contains("could not find valid split key")) {
						// we should wait for long enough for the split to finish
						txn.rollback();
						System.out.println("[INFO] wait for spliting range");
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e2) {
							System.out.print("[INFO] " + e2.getMessage());
						}
				  } else {
						if (e instanceof KvException) {
							e.printStackTrace();
						} else {
							ChengLogger.getInstance().debug(e.toString());
						}
						txn.rollback();
					}
					
					abortcounter.incrementAndGet();
					p.endTick(txn.getname());
				} catch (java.lang.AssertionError err) {
					System.out.println("[ERROR] [AssertionError] Tid[" + getId() + "] msg[" + err.toString() + "]");
					err.printStackTrace();
					innerRollbackAndReconnect(txn);
				} catch (Exception e1) {
					System.out.println("[ERROR] [Exception] Tid[" + getId() + "] msg[" + e1.toString() + "]");
					e1.printStackTrace();
					innerRollbackAndReconnect(txn);
				}
			}
		} catch (InterruptedException e) {
			System.out.println("[ERROR] [InterruptedException] Tid[" + getId() + "] msg[" + e.getMessage() + "]");
			e.printStackTrace();
		}
		
		Logger.logDebug("thread " + getId() + " received endSignal and terminated");
		Logger.logDebug("thread " + getId() + " committed " + commitcounter.get() + " txns");
	}
}
