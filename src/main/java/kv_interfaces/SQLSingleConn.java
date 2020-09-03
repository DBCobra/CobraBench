package kv_interfaces;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.postgresql.ds.PGSimpleDataSource;

import kvstore.exceptions.DupInsertException;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnAbortException;
import kvstore.exceptions.TxnException;
import main.Config;

public class SQLSingleConn {
	// ==== connection states of each thread ========
	private Connection conn = null;
	private Statement writeBuffer = null;
	private int writeBufferSize = 0;
	private long currTxnId = -1;
	public int num_op = 0;
	private AtomicLong nextTxnId;
	private final String TABLE_NAME;
	public int port;
	
	// 0: postgresql; 1: cockroachdb
	public SQLSingleConn(AtomicLong tid, String table_name) {
		nextTxnId = tid;
		TABLE_NAME = table_name;
		initPostgresql();
	}
	
	private void initPostgresql() {
		try {
			Class.forName("org.postgresql.Driver");
			this.conn = DriverManager.getConnection(Config.get().DB_URL, Config.get().PG_USERNAME, Config.get().PG_PASSWORD);
			this.conn.setAutoCommit(false);
			this.conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			this.writeBuffer = null;
			this.writeBufferSize = 0;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	// =============== kv interface =================
	/**
	 * In JDBC we don't have to specify the start of a transaction So we just
	 * construct a transaction ID. And this is a single thread case, so it's really
	 * simple.
	 */
	public Object begin() throws KvException, TxnException {
//		try {
//			Statement st = conn.createStatement();
//			ResultSet rs = st.executeQuery("select current_setting('transaction_isolation');");
//			while(rs.next()) {
//				System.out.println(rs.getString("current_setting"));
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
		assert conn != null;
		assert currTxnId == -1; // last transaction is finished
		assert writeBufferSize == 0;
		currTxnId = nextTxnId.getAndIncrement();
		try {
			writeBuffer = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		num_op++;
		return currTxnId;
	}

	public boolean commit(Object txn) throws KvException, TxnException {
		assert conn != null;
		assert (Long) txn == currTxnId && currTxnId != -1;
		num_op++;

		try {
			int[] res = writeBuffer.executeBatch();
			assert res.length == writeBufferSize;
			for(int i : res) {
				assert i == 1;
			}
			conn.commit();
			writeBufferSize = 0;
			currTxnId = -1;
		} catch (SQLException e) {
			throw new TxnException(e.getMessage());
		}
		return true;
	}

	public boolean abort(Object txn) throws KvException, TxnException {
		assert conn != null;
		assert (Long) txn == currTxnId && currTxnId != -1;
		num_op++;

		try {
			writeBuffer.clearBatch();
			writeBufferSize = 0;
			conn.rollback();
			currTxnId = -1;
		} catch (SQLException e) {
			throw new TxnException(e.getMessage());
		}
		return true;
	}

	public boolean insert(Object txn, String key, String value) throws KvException, TxnException {
		assert conn != null;
		assert writeBuffer != null;
		assert (Long) txn == currTxnId && currTxnId != -1;
		num_op++;

		try {
			String sqlstmt = String.format("INSERT INTO " + TABLE_NAME + " (key, value) VALUES ('%s', '%s')", key, value);
			writeBuffer.addBatch(sqlstmt);
			writeBufferSize++;
		} catch (SQLException e) {
			String errMsg = e.getMessage();
			if (errMsg.contains("already exists.")) {
				throw new DupInsertException(errMsg);
			}
			if (errMsg.contains("could not serialize access due to concurrent update")) {
				throw new TxnAbortException(e.getMessage());
			}
			throw new TxnException(e.getMessage());
		}
		return true;
	}

	public boolean delete(Object txn, String key) throws KvException, TxnException {
		assert conn != null;
		assert (Long) txn == currTxnId && currTxnId != -1;
		num_op++;

		PreparedStatement st;
		try {
			st = conn.prepareStatement("DELETE FROM " + TABLE_NAME + " WHERE key = ?");
			st.setString(1, key);
			int updatedRows = st.executeUpdate();
			if (updatedRows == 0) {
				return false;
			}
		} catch (SQLException e) {
			String errMsg = e.getMessage();
			if (errMsg.contains("could not serialize access due to concurrent update")) {
				throw new TxnAbortException(e.getMessage());
			}
			throw new TxnException(e.getMessage());
		}
		return true;
	}

	public String get(Object txn, String key) throws KvException, TxnException {
		assert conn != null;
		assert (Long) txn == currTxnId && currTxnId != -1;
		num_op++;

		PreparedStatement st;
		String value = null;
		try {
			st = conn.prepareStatement("SELECT value FROM " + TABLE_NAME + " WHERE key = ?");
			st.setString(1, key);
			ResultSet rs = st.executeQuery();
			int rowCount = 0;
			while (rs.next()) {
				rowCount++;
				value = rs.getString("value");
			}
			assert rowCount == 0 || rowCount == 1;
		} catch (SQLException e) {
			String errMsg = e.getMessage();
			if (errMsg.contains("could not serialize access due to read/write dependencies")) {
				throw new TxnAbortException(e.getMessage());
			}
			throw new TxnException(e.getMessage());
		}
		return value;
	}

	public boolean set(Object txn, String key, String value) throws KvException, TxnException {
		assert conn != null;
		assert (Long) txn == currTxnId && currTxnId != -1;
		num_op++;

		try {
			// Backup code: only update, if key don't exist, then there will be SQLException.
//			st = conn.prepareStatement("UPDATE " + TABLE_NAME + " SET value = ? where key = ?");
//			st.setString(1, value);
//			st.setString(2, key);
			
			// This 'put' can only be done in PostgreSQL 9.5 or newer. In MySQL it becomes:
			// "INSERT INTO table (key, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = ?"
			String sqlstmt = String.format("INSERT INTO " + TABLE_NAME
					+ " (key, value) VALUES ('%s', '%s') ON CONFLICT (key) DO UPDATE SET value = '%s'", key, value, value);
			writeBuffer.addBatch(sqlstmt);
			writeBufferSize++;
		} catch (SQLException e) {
			String errMsg = e.getMessage();
			if (errMsg.contains("could not serialize access due to")) {
				throw new TxnAbortException(e.getMessage());
			} else if (errMsg.contains("deadlock detected")) {
				throw new TxnAbortException(e.getMessage());
			} else {
				e.printStackTrace();
				throw new TxnException(e.getMessage());
			}
		}
		return true;
	}

	public boolean rollback(Object txn) {
		assert currTxnId == (Long) txn && currTxnId != -1;
		num_op++;
		currTxnId = -1;
		try {
			writeBuffer.clearBatch();
			writeBufferSize = 0;
			conn.rollback();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return true;
	}

	public boolean isalive(Object txn) {
		return (Long) txn == currTxnId;
	}
}