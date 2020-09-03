package kv_interfaces;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Transaction;

import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnAbortException;
import kvstore.exceptions.TxnException;
import main.Config;
import main.Logger;

public class GoogleDataStore implements KvInterface {

	private Datastore ds = null;
	private int client_id = 0;
	private KeyFactory kf = null;

	private Map<Long, Transaction> alive_txns = new ConcurrentHashMap<Long, Transaction>();
	private AtomicLong txn_counter = new AtomicLong(1);
	private Map<Transaction, Long> txn2id = new ConcurrentHashMap<Transaction, Long>();

	public GoogleDataStore(Datastore ds, String kind) {
		this.ds = ds;
		this.kf = ds.newKeyFactory().setKind(kind);
		this.client_id = Config.get().CLIENT_ID;
	}

	public Object begin() throws KvException, TxnException {
		Transaction txn = ds.newTransaction();
		alive_txns.put(getTxnId(txn), txn);
		return txn;
	}

	public boolean commit(Object txn) throws KvException, TxnException {
		assert txn instanceof Transaction;
		Transaction t = (Transaction) txn;
		try {
			t.commit();
			alive_txns.remove(getTxnId(t));
			txn2id.remove(t);
			return true;
		} catch (DatastoreException e) {
			throw new TxnAbortException(e.toString());
		}
	}

	public boolean abort(Object txn) throws KvException, TxnException {
		assert txn instanceof Transaction;
		Transaction t = (Transaction) txn;
		try {
			t.rollback();
			alive_txns.remove(getTxnId(t));
			txn2id.remove(t);
			return true;
		} catch (DatastoreException e) {
			throw new TxnAbortException(e.toString());
		}
	}

	public boolean rollback(Object txn) {
		assert txn instanceof Transaction;
		Transaction t = (Transaction) txn;
		if (t.isActive()) {
			try {
				t.rollback();
			} catch (DatastoreException e) {
				// double exception, we just do nothing
				Logger.logError("Txn rollback failed, exception:" + e.toString() + "\n");
			}
		}
		return true;
	}

	public boolean isalive(Object txn) {
		assert txn instanceof Transaction;
		assert alive_txns.containsKey(txn);
		return ((Transaction) txn).isActive();
	}

	// if txn == null, this is operation only; otherwise, this is in txn.
	private boolean put(Object txn, String key, String value) throws KvException, TxnException {
		boolean inTxn = (txn != null);
		// The Cloud Datastore key for the new entity
		Key taskKey = kf.newKey(key);
		// The maximum length of a property is 1500 so cut the new entity into pieces
		Entity.Builder taskBuilder = Entity.newBuilder(taskKey);
		for (int i = 0; i < value.length(); i+=1500) {
			int end = i + 1500;
			if (end > value.length())
				end = value.length();
			String propertyName = Config.get().GOOGLEVALUE;
			if(i != 0)
				propertyName += (i/1500);
			String piece = value.substring(i, end);
//			if(value.length() > 1500)
//				System.out.println(propertyName );
			taskBuilder.set(propertyName, piece);
		}
		Entity task = taskBuilder.build();
		
		try {
			if (inTxn) {
				assert txn instanceof Transaction;
				Transaction t = (Transaction) txn;
				t.put(task);
			} else {
				ds.put(task);
			}
			return true;
		} catch (DatastoreException e) {
			throw new TxnAbortException(e.toString());
		}
	}

	public boolean delete(Object txn, String key) throws KvException, TxnException {
		// TODO: implement this
		return false;
	}

	public String get(Object txn, String key) throws KvException, TxnException {
		boolean inTxn = (txn != null);
		// The Cloud Datastore key for the new entity
		Key task_key = kf.newKey(key);
		Entity retrieved = null;
		try {
			if (inTxn) {
				assert txn instanceof Transaction;
				Transaction t = (Transaction) txn;
				retrieved = t.get(task_key);
			} else {
				retrieved = ds.get(task_key);
			}
			if (retrieved == null)
				return null;
			return retrieved.getString(Config.get().GOOGLEVALUE);
		} catch (DatastoreException e) {
			throw new TxnAbortException(e.toString());
		}
	}

	public boolean insert(Object txn, String key, String value) throws KvException, TxnException {
		return put(txn, key, value);
	}

	public boolean set(Object txn, String key, String value) throws KvException, TxnException {
		return put(txn, key, value);
	}

	@Override
	public long getTxnId(Object txn) {
		assert txn instanceof Transaction;
		Transaction real_txn = (Transaction) txn;
		if (txn2id.containsKey(real_txn)) {
			return txn2id.get(real_txn);
		}
		long txn_id = ((client_id << 32) & 0x00ffffffffffffffL) + txn_counter.incrementAndGet(); 
		
		txn2id.put(real_txn, txn_id);
		return txn_id;
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

	public void clearChengKeys() {
		// legacy function. TODO: make it universal for all benchmarks
		assert false;
		System.out.print("Clear Google Datastore keys");
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		for (int i = 0; i < Config.get().NUM_KEYS; i++) {
			int indx = Config.get().KEY_INDX_START + i;
			String str_key = Config.get().KEY_PRFIX + indx;
			Key taskKey = datastore.newKeyFactory().setKind("ooo").newKey(str_key);
			try {
				datastore.delete(taskKey);
			} catch (Exception e) {
				// do nothing
			}
		}
		System.out.println("...DONE");
	}
}
