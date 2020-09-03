package kv_interfaces;

import kvstore.exceptions.DupInsertException;
import kvstore.exceptions.KvException;
import kvstore.exceptions.NonExistingKeyException;
import kvstore.exceptions.TxnAbortException;
import kvstore.exceptions.TxnException;
import main.Config;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SqlKV implements KvInterface {

	// singleton per thread
	private Map<Long, SQLSingleConn> instances = new ConcurrentHashMap<Long, SQLSingleConn>();
	public AtomicLong nextTxnId;
	
	public SQLSingleConn getInstance() {
		long threadId = Thread.currentThread().getId();
		if (!instances.containsKey(threadId)) {
			instances.put(threadId, new SQLSingleConn(nextTxnId, TABLE_NAME));
		}
		return instances.get(threadId);
	}

	private final String TABLE_NAME;

	public SqlKV(String tableName) {
		this.TABLE_NAME = tableName;
		nextTxnId = new AtomicLong(((long)Config.get().CLIENT_ID) << 32);
	}

	

	@Override
	public Object begin() throws KvException, TxnException {
		return this.getInstance().begin();
	}

	@Override
	public boolean commit(Object txn) throws KvException, TxnException {
		return this.getInstance().commit(txn);
	}

	@Override
	public boolean abort(Object txn) throws KvException, TxnException {
		return this.getInstance().abort(txn);
	}

	@Override
	public boolean insert(Object txn, String key, String value) throws KvException, TxnException {
		return this.getInstance().insert(txn, key, value);
	}

	@Override
	public boolean delete(Object txn, String key) throws KvException, TxnException {
		return this.getInstance().delete(txn, key);
	}

	@Override
	public String get(Object txn, String key) throws KvException, TxnException {
		return this.getInstance().get(txn, key);
	}

	@Override
	public boolean set(Object txn, String key, String value) throws KvException, TxnException {
		return this.getInstance().set(txn, key, value);
	}

	@Override
	public boolean rollback(Object txn) {
		return this.getInstance().rollback(txn);
	}

	@Override
	public boolean isalive(Object txn) {
		return this.getInstance().isalive(txn);
	}

	@Override
	public long getTxnId(Object txn) {
		/**
		 * We use per thread singleton for SQL connection. Each thread must only have no
		 * more than one ongoing transaction. So it is useless to specify txn in the
		 * parameter of all those operations(begin, set, put,...) so the txn is just
		 * transaction id and we can always check whether the passed-in txnid is the
		 * same with the ongoing one in our state.
		 */
		return (Long) txn;
	}

	@Override
	public Object getTxn(long txnid) {
		return txnid;
	}

	@Override
	public boolean isInstrumented() {
		return false;
	}
	
	/**
	 * @return An integer, which is the sum of operation counts of all connections
	 */
	public int getNumOp() {
		int total_num_op = 0;
		for (SQLSingleConn conn : instances.values()) {
			total_num_op += conn.num_op;
		}
		return total_num_op;
	}
	
	public void clearNumOp() {
		for (SQLSingleConn conn: instances.values()) {
			conn.num_op = 0;
		}
	}
}
