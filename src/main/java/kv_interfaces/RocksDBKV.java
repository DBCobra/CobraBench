package kv_interfaces;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.rocksdb.*;

import bench.chengTxn.ChengTxnConstants;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnAbortException;
import kvstore.exceptions.TxnException;
import main.Config;

public class RocksDBKV implements KvInterface {
	// the client id
	private long client_id = 0; 
	
	// options
	private Options options = null;
	private TransactionDBOptions txndboptions = null;
	private TransactionOptions txnoptions = null;
	private WriteOptions woption = null;
	//private ReadOptions roption = null;
	
	// entities
	private TransactionDB txn_db = null;
	
	// keep track of all readoptions (=snapshots)
	private Map<Transaction, ReadOptions> readoptions = null;
	// alive transactions
	private Map<Long, Transaction> alive_txns = null;
	
	// statistics
	public final AtomicInteger numOp = new AtomicInteger(0);
	
	// singleton
	private static RocksDBKV instance = null;

	
	
	// ==== TXN IDS ====
	private AtomicLong txn_counter = new AtomicLong(10);
	private Map<Transaction, Long> txn2id = new ConcurrentHashMap<Transaction,Long>();
	
	// FIXME: RocksDB doesn't provide me a unique id! That's annoying!
	public long getTxnId(Object txn) {
		assert txn instanceof Transaction;
		Transaction real_txn = (Transaction)txn;
		if (txn2id.containsKey(real_txn)) {
			return txn2id.get(real_txn);
		}
		long txn_id = ((client_id << 32) & 0x00ffffffffffffffL) + txn_counter.incrementAndGet(); // should be unique among all the client libs
		txn2id.put(real_txn, txn_id);
		return txn_id;
	}
	
	// === FUNCTIONS ===
	
	
	public static RocksDBKV getInstance() {
		if (instance == null) {
			synchronized (RocksDBKV.class) {
				if(instance == null) {
					instance = new RocksDBKV();
				}
			}
		}
		return instance;
	}
	
	private RocksDBKV() {
		client_id = Config.get().CLIENT_ID;
		options = new Options().setCreateIfMissing(true);
		txndboptions = new TransactionDBOptions();
		txnoptions = new TransactionOptions().setSetSnapshot(true).setDeadlockDetect(true);
		woption = new WriteOptions();
		
		try {
			txn_db = TransactionDB.open(options, txndboptions, Config.get().ROCKSDB_PATH);
		} catch (RocksDBException e) {
			e.printStackTrace();
			assert false; // For now, we stop here
		}
		
		readoptions = new ConcurrentHashMap<Transaction, ReadOptions>();
		alive_txns = new ConcurrentHashMap<Long, Transaction>();
	}


	@Override
	public Object begin() throws KvException, TxnException {
		// create one transaction with its snapshot
		RocksDBKV db = RocksDBKV.getInstance();
		numOp.incrementAndGet();
		Transaction txn = db.txn_db.beginTransaction(db.woption, db.txnoptions);
		assert !readoptions.containsKey(txn);
		readoptions.put(txn, new ReadOptions().setSnapshot(txn.getSnapshot()));
		alive_txns.put(getTxnId(txn), txn); // a new txn id generated within getTxnId(...)
		
		return txn;
	}

	@Override
	public boolean commit(Object txn) throws KvException, TxnException {
		assert txn instanceof Transaction;
		numOp.incrementAndGet();
		Transaction real_txn = (Transaction) txn;
		try {
			real_txn.commit();
		} catch (RocksDBException e) {
			throw new TxnAbortException(e.toString());
		} finally {
			readoptions.remove(real_txn);
			alive_txns.remove(getTxnId(real_txn));
			txn2id.remove(real_txn);
			real_txn.close();
		}
		return true;
	}

	@Override
	public boolean abort(Object txn) throws KvException, TxnException {
		assert txn instanceof Transaction;
		numOp.incrementAndGet();
		Transaction real_txn = (Transaction) txn;
		try {
			real_txn.rollback();
		} catch (RocksDBException e) {
			throw new TxnException(e.toString());
		} finally {
			readoptions.remove(real_txn);
			alive_txns.remove(getTxnId(real_txn));
			txn2id.remove(real_txn);
			real_txn.close();
		}
		return true;
	}

	@Override
	public boolean insert(Object txn, String key, String value) throws KvException, TxnException {
		// FIXME: no check on the existance of the key
		numOp.incrementAndGet();
		return set(txn, key, value);
	}

	@Override
	public boolean delete(Object txn, String key) throws KvException, TxnException {
		numOp.incrementAndGet();
		assert txn instanceof Transaction;
		Transaction real_txn = (Transaction) txn;
		
		try {
			real_txn.delete(key.getBytes());
		} catch (RocksDBException e) {
			throw new TxnAbortException(e.toString());
		}
		
		return true;
	}

	@Override
	public String get(Object txn, String key) throws KvException, TxnException {
		assert txn instanceof Transaction;
		numOp.incrementAndGet();
		Transaction real_txn = (Transaction) txn;
		String ret = null;
		ReadOptions roption = RocksDBKV.getInstance().readoptions.get(real_txn);
		
		try {
			byte[] val = real_txn.getForUpdate(roption, key.getBytes(), true);  // for serializability
			if (val != null) {
				ret = new String(val);
			}
		} catch (RocksDBException e) {
			throw new TxnAbortException(e.toString());
		}
		
		return ret;
	}
	
	@Override
	public boolean set(Object txn, String key, String value) throws KvException, TxnException {
		assert txn instanceof Transaction;
		numOp.incrementAndGet();
		Transaction real_txn = (Transaction) txn;
		
		try {
			real_txn.put(key.getBytes(), value.getBytes());
		} catch (RocksDBException e) {
			throw new TxnAbortException(e.toString());
		}
		
		return true;
	}

	@Override
	public boolean rollback(Object txn) {
		try {
			numOp.incrementAndGet();
			abort(txn);
		} catch (KvException | TxnException e) {
			e.printStackTrace();
			assert false; // FIXME: for now, we just stop
		}
		return true;
	}

	@Override
	public boolean isalive(Object txn) {
		assert txn instanceof Transaction;
		Transaction real_txn = (Transaction) txn;
		assert readoptions.containsKey(real_txn) == alive_txns.containsKey(getTxnId(real_txn));
		return alive_txns.containsKey(getTxnId(real_txn));
	}

	@Override
	public boolean isInstrumented() {
		return false;
	}

	@Override
	public Object getTxn(long txnid) {
		if(!alive_txns.containsKey(txnid)) {
			return null;
		} 	
		return alive_txns.get(txnid);
	}
	
	public int getNumOp() {
		return numOp.get();
	}
	
	public void clearDB() {
		System.out.print("Clear all RocksDB files");
		// remove all the logs
		File log_fd = new File(Config.get().ROCKSDB_PATH);
		String[] entries = log_fd.list();
		if (entries != null) {
			for (String s : entries) {
				File currentFile = new File(log_fd.getPath(), s);
				currentFile.delete();
			}
		}
		System.out.println("...DONE");
	}
}
