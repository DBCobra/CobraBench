package kv_interfaces.instrument;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import main.Config;

//Keeps track of all the states of clients, including:
//- Log information
//  -- log object with (1) client logs (2) previous log object hashes
//- transaction information
//  -- current txnid
//- epoch info
// -- monitoring phase
// -- other client's epochs
// -- my epoch
public class ChengClientState {
	// global vars
	private static final Long OUTSIDE_TXN = Long.MAX_VALUE;
	private static final String INIT_CLOG_CHAIN_HASH = "0";
	// map tid => ClientInfo
	private static Map<Long, ChengClientState> clients = Collections
			.synchronizedMap(new HashMap<Long, ChengClientState>());

	// ==================
	// === local vars ===
	// ==================
	private String logkey_prefix = "LOGKEY_";

	// -----transaction information-----
	public Long cur_txnid = OUTSIDE_TXN;

	// ---------log information (CLOUD_LOG)------
	private int entity_id = 0; // current logobject
	public int txn_in_logobj = 0; // how many transactions in the logobject
	public LogObject prev_committed_entity = null;
	public LogObject cur_entity = null;
	
	// ------------epoch status---------
	// current epoch number
	public long curEpoch = 0;
	// the transaction number in current epoch
	public int epochTxnNum = 0;
	// if in monitoring mode for GC
	public boolean gc_monitoring = false;
	// if in monitoring mode for epoch
	public boolean monitoring = false;
	// other client's epochs
	public Map<String, Long> prev_epochs = new HashMap<String, Long>();
	// OPT: read/write sets
	public Set<String> r_set = new HashSet<String>();
	public int extra_op = 0;  // for information reasoning
	public int num_monitoring_txns = 0; // for information reasoning
	
	

	private ChengClientState(long tid) {
		logkey_prefix = ChengIdGenerator.genClientThreadId();
		prev_committed_entity = new LogObject(getLogKey(), INIT_CLOG_CHAIN_HASH); // start with empty hash
		cur_entity = new LogObject(prev_committed_entity);
	}

	public static ChengClientState getInstance() {
		long tid = Thread.currentThread().getId();
		assert clients.containsKey(tid);
		return clients.get(tid);
	}

	// =====================
	// Naming functions
	// =====================

	public String getLogKey() {
		return logkey_prefix + "_" + entity_id;
	}
	
	public String getClientName() {
		String host = "UNKNOWN";
		try {
			host = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			assert false;
		}
		String client = "C" + Thread.currentThread().getId();
		return host + client;
	}

	// =====================
	// Txn state management
	// =====================

	public static long getTxnId() {
		long tid = Thread.currentThread().getId();
		assert clients.containsKey(tid);
		return clients.get(tid).cur_txnid;
	}

	public static boolean inTxn() {
		long tid = Thread.currentThread().getId();
		// txnid != MAX_VALUE means it is in txn
		return (clients.containsKey(tid) && clients.get(tid).cur_txnid != OUTSIDE_TXN);
	}

	public static void initTxnId(long txnid) {
		long tid = Thread.currentThread().getId();
		if (!clients.containsKey(tid)) {
			ChengClientState instance = new ChengClientState(tid);
			clients.put(tid, instance);
		}
		ChengClientState cs = clients.get(tid);
		assert cs.cur_txnid == OUTSIDE_TXN;
		cs.cur_txnid = txnid;
	}

	//
	public static void removeTxnId() {
		long tid = Thread.currentThread().getId();
		assert clients.containsKey(tid);
		assert clients.get(tid).cur_txnid != OUTSIDE_TXN;
		clients.get(tid).cur_txnid = OUTSIDE_TXN;
	}

	// =====================
	// Online-log Entity
	// =====================

	/*
	 * LogObject: Each log chunk (one object) contains 100 txns. One txn start with
	 * the last hash_chain value. Use SHA256 as the hash function. Client-id-#number
	 * as the key.
	 * 
	 * <Client-id>-0-cop (an entity): 0: txn1 0xXXXX: txn2 (where
	 * 0xXXXX==SHA256(txn1 + 0x0000)) 0xXXXY: txn3 (where 0xXXXY==SHA256(txn2 +
	 * 0xXXXX)) ...
	 */

	public static LogObject append2logobj(ArrayList<byte[]> clog) {
		long tid = Thread.currentThread().getId();
		assert clients.containsKey(tid);// "removeTxnId(): doesn't has tid=" + tid );
		assert clients.get(tid).cur_txnid != OUTSIDE_TXN; // "removeTxnId(): doesn't in transaction, tid=" + tid);

		// append on current log object
		LogObject logobj = clients.get(tid).cur_entity;

		// (1) append the current clog
		logobj.appendClientLog(clog);
		
		// (2) return the log entity
		return logobj;
	}

	// When successfully commit one txn, take care of
	// -- (1) update the number of txns in current log object
	// -- (2) if we need to move to a new logobject, then
	// -- (i) calculate the hash, (ii) put it to the next log object
	// -- (3) move forward current logobject to prev_object
	public void successCommitOneEntity() {
		// (1)
		txn_in_logobj++;
		// (2)
		if (txn_in_logobj >= Config.get().NUM_TXN_IN_ENTITY) {
			txn_in_logobj = 0;
			entity_id++;
			// create a new entity with new LogKey name
			String k = getLogKey();
			// calculate the previous log object hash
			String cur_cl_hash = cur_entity.getClientLogHash();
			// move forward the pointer of current log object
			cur_entity = new LogObject(k, cur_cl_hash);
		}
		// (3) update committed log object
		prev_committed_entity = new LogObject(cur_entity);
	}

	public void rollbackLogObj() {
		// rollback the log object
		cur_entity = new LogObject(prev_committed_entity);
	}
	
	// =====================
	// ===== statistics ====
	// =====================
	
	public static synchronized int getExtraOps() {
		int extra_ops = 0;
		for (ChengClientState status : clients.values()) {
			extra_ops += status.extra_op;
		}
		return extra_ops;
	}
	
	public static synchronized int getExtraTxns() {
		int epoch_txns = 0;
		for (ChengClientState status : clients.values()) {
			epoch_txns += status.curEpoch; // curEpoch roughly equals to num of epoch
		}
		return epoch_txns;
	}
	
	public static synchronized int getTxnsInMonitoring() {
		int monitoring_txns = 0;
		for (ChengClientState status : clients.values()) {
			monitoring_txns += status.num_monitoring_txns; // curEpoch roughly equals to num of epoch
		}
		return monitoring_txns;
	}

	public static String printEpochs(Map<String, Long> epochs) {
		StringBuilder sb = new StringBuilder();
		String[] names = epochs.keySet().toArray(new String[0]);
		Arrays.sort(names);
		for (String c : names) {
			sb.append( "(" + c + "," + epochs.get(c) + ") ");
		}
		return sb.toString();
	}

}