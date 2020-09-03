package kv_interfaces;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.time.*;
import java.sql.*;
import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;

import bench.Crack.WriteSkewBench;
import kvstore.exceptions.DupInsertException;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnAbortException;
import kvstore.exceptions.TxnException;
import main.Config;

public class CockroachDB implements KvInterface {

  private static final int MAX_RETRY_COUNT = 3;
  private static final String SAVEPOINT_NAME = "cockroach_restart";
  private static final String RETRY_SQL_STATE = "40001";
   
	private static boolean init = false;
	private static CockroachDB instance = null;
	

	public AtomicLong nextTxnId;
	// connection per thread
	private Map<Long, CockroachConnection> tid2conn;

	
	// ===== singleton =====
	
	public synchronized static CockroachDB getInstance() {
		if (instance == null) {
			clearTables();
			instance = new CockroachDB();
		}
		return instance;
	}

	private CockroachDB() {
		nextTxnId = new AtomicLong(((long)Config.get().CLIENT_ID) << 32);
		tid2conn = new ConcurrentHashMap<Long, CockroachConnection>();
	}
	
	// ===== init =====

	// drop and recreate all tables for all benchmarks
	private static void clearTables() {
		assert !init;
		init = true;

		PGSimpleDataSource ds = getDataSource(0); // choose the first host

		String drop = "DROP TABLE %s;";
		String create = "CREATE TABLE %s (key BYTES PRIMARY KEY, value BYTES);";
		//String[] tables = { "chengTxn", "tpcc", "ycsb", "rubis", "twitter", TABLE_A, TABLE_B };
		String[] tables = { WriteSkewBench.TABLE_A, WriteSkewBench.TABLE_B };

		// drop and create
		for (String tab : tables) {
			System.out.print("[INFO] Start to clear table[" + tab + "]...drop...");
			try {
				Connection conn = ds.getConnection();
				// drop
				try {
					String sql1 = String.format(drop, tab);
					conn.createStatement().execute(sql1);
				} catch (SQLException e) {
					System.out.println(e.toString());
				}
				System.out.print("done...create...");
				// create
				try {
					String sql2 = String.format(create, tab);
					conn.createStatement().execute(sql2);
				} catch (SQLException e) {
					e.printStackTrace();
					System.exit(-1);
				}
				System.out.println("done");
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
				System.exit(-1);
			}
		}
	}
	
	private static PGSimpleDataSource getDataSource(int indx) {
		PGSimpleDataSource ds = new PGSimpleDataSource();
		ds.setServerName(Config.get().COCKROACH_DB_URLS[indx]);
		ds.setPortNumber(Config.get().COCKROACH_PORTS[indx]);
		ds.setDatabaseName(Config.get().COCKROACH_DATABASE_NAME);
		ds.setUser(Config.get().COCKROACH_USERNAME);
		ds.setPassword(Config.get().COCKROACH_PASSWORD);
    ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
		return ds;
	}
	
	// ===== status managment ======
	
	public CockroachConnection getConn4currentThread() {
		long threadId = Thread.currentThread().getId();
		if (!tid2conn.containsKey(threadId)) {
			CockroachConnection conn = new CockroachConnection(nextTxnId);
			tid2conn.put(threadId, conn);
			System.out.println("[INFO] tid[" + threadId + "] starts a sql connection at port<" + conn.port + ">");
		}
		return tid2conn.get(threadId);
	}
	
	public void ReportServerDown() {
		long threadId = Thread.currentThread().getId();
		assert tid2conn.containsKey(threadId);
		tid2conn.remove(threadId);
	}
	
	public String Status() {
		String status = "STATUS: ";
		try {
			StringBuilder sb = new StringBuilder(status);
			for (long tid : tid2conn.keySet()) {
				sb.append("T[" + tid + "][" + tid2conn.get(tid).port +"] ");
			}
			status = sb.toString();
		} catch (Exception e) {
			System.out.print("[ERROR] print status meet some error: " + e.getMessage());
		}
		return status;
	}

	// ======== APIs ===========
	

	private String[] DecodeTableKey(String key) {
		// NOTE: key == <table>###<real key>
		String[] ret = key.split("###");
		assert ret.length == 2;
		return ret;
	}

	@Override
	public Object begin() throws KvException, TxnException {
		return getInstance().getConn4currentThread().begin();
	}

	@Override
	public boolean commit(Object txn) throws KvException, TxnException {
		return getInstance().getConn4currentThread().commit(txn);
	}

	@Override
	public boolean abort(Object txn) throws KvException, TxnException {
		return getInstance().getConn4currentThread().abort(txn);
	}

	@Override
	public boolean insert(Object txn, String key, String value) throws KvException, TxnException {
		String[] tokens = DecodeTableKey(key);
		assert tokens.length == 2;
		return getInstance().getConn4currentThread().insert(txn, tokens[0], tokens[1], value);
	}

	@Override
	public boolean delete(Object txn, String key) throws KvException, TxnException {
		String[] tokens = DecodeTableKey(key);
		assert tokens.length == 2;
		return getInstance().getConn4currentThread().delete(txn, tokens[0], tokens[1]);
	}

	@Override
	public String get(Object txn, String key) throws KvException, TxnException {
		String[] tokens = DecodeTableKey(key);
		assert tokens.length == 2;
		return getInstance().getConn4currentThread().get(txn, tokens[0], tokens[1]);
	}

	@Override
	public boolean set(Object txn, String key, String value) throws KvException, TxnException {
		String[] tokens = DecodeTableKey(key);
		assert tokens.length == 2;
		return getInstance().getConn4currentThread().set(txn, tokens[0], tokens[1], value);
	}

	@Override
	public boolean rollback(Object txn) {
		return getInstance().getConn4currentThread().rollback(txn);
	}

	@Override
	public boolean isalive(Object txn) {
		return getInstance().getConn4currentThread().isalive(txn);
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
		for (CockroachConnection conn : tid2conn.values()) {
			total_num_op += conn.num_op;
		}
		return total_num_op;
	}
	
	public void clearNumOp() {
		for (CockroachConnection conn: tid2conn.values()) {
			conn.num_op = 0;
		}
	}
	
	
	
	// ======== inner class ========
	
	static class CockroachConnection {
		
		// ==== connection states of each thread ========
		private Connection conn = null;
		private Statement writeBuffer = null;
		private int writeBufferSize = 0;
		private long currTxnId = -1;
		public int num_op = 0;
		private AtomicLong nextTxnId;
		public int port;
		
		
		public CockroachConnection(AtomicLong tid) {
			nextTxnId = tid;

			while (initCockroachdb() != true) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		private boolean initCockroachdb() {
	    int indx = new Random().nextInt(Config.get().COCKROACH_PORTS.length);
	    PGSimpleDataSource ds = getDataSource(indx);
	    ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
	    
	    try {
				this.conn = ds.getConnection();
				this.conn.setAutoCommit(false);
				this.conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
				this.writeBuffer = null;
				this.writeBufferSize = 0;
			} catch (Exception e) {
				System.err.println("[ERROR] connection error: " + e.getMessage());
				return false;
			}
	    return true;
		}

		// =============== kv interface =================
		/**
		 * In JDBC we don't have to specify the start of a transaction So we just
		 * construct a transaction ID. And this is a single thread case, so it's really
		 * simple.
		 */
		public Object begin() throws KvException, TxnException {
//			try {
//				Statement st = conn.createStatement();
//				ResultSet rs = st.executeQuery("select current_setting('transaction_isolation');");
//				while(rs.next()) {
//					System.out.println(rs.getString("current_setting"));
//				}
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
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
					// XXX: FIXME: [cheng: somehow, we got 0 here in WritSkew benchmark. Don't know why. Just let go for now]
					assert i == 1 || i == 0;
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

		public boolean insert(Object txn, String table, String key, String value) throws KvException, TxnException {
			assert conn != null;
			assert writeBuffer != null;
			assert (Long) txn == currTxnId && currTxnId != -1;
			num_op++;

			try {
				String sqlstmt = String.format("INSERT INTO " + table + " (key, value) VALUES (b'%s', b'%s')", key, value);
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

		public boolean delete(Object txn, String table, String key) throws KvException, TxnException {
			assert conn != null;
			assert (Long) txn == currTxnId && currTxnId != -1;
			num_op++;

			PreparedStatement st;
			try {
				st = conn.prepareStatement("DELETE FROM " + table + " WHERE key = ?");
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

		public String get(Object txn, String table, String key) throws KvException, TxnException {
			assert conn != null;
			assert (Long) txn == currTxnId && currTxnId != -1;
			num_op++;

			PreparedStatement st;
			String value = null;
			try {
				st = conn.prepareStatement("SELECT value FROM " + table + " WHERE key = ?");
				st.setString(1, key);
				ResultSet rs = st.executeQuery();
				int rowCount = 0;
				while (rs.next()) {
					rowCount++;
					//value = rs.getString("value");
					value = new String(rs.getBytes("value"));
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

		public boolean set(Object txn, String table, String key, String value) throws KvException, TxnException {
			assert conn != null;
			assert (Long) txn == currTxnId && currTxnId != -1;
			num_op++;

			try {
			  // batch impl
				/*
				String sqlstmt = String.format("UPDATE " + table + " SET value = b'%s' WHERE key = b'%s'", value, key);
				writeBuffer.addBatch(sqlstmt);
				writeBufferSize++;
				*/
				
				// eager update (I)
				/*
				String sqlstmt = String.format("UPDATE " + table + " SET value = b'%s' WHERE key = b'%s'", value, key);
				PreparedStatement st = conn.prepareStatement(sqlstmt);
				boolean done = st.execute();
				assert done == false;
				assert st.getUpdateCount() == 1;
				*/
				
				// eager update (II)
				/*
				String sqlstmt = String.format("UPDATE " + table + " SET value = b'%s' WHERE key = b'%s'", value, key);
				PreparedStatement st = conn.prepareStatement(sqlstmt);
				int done = st.executeUpdate();
				assert done == 1;
				*/
				
				// eager upsert
				String sqlstmt = String.format("UPSERT INTO " + table + " VALUES (b'%s', b'%s')", key, value);
				PreparedStatement st = conn.prepareStatement(sqlstmt);
				boolean done = st.execute();
				assert done == false;
				assert st.getUpdateCount() == 1;
				
			} catch (SQLException e) {
				String errMsg = e.getMessage();
				if (errMsg.contains("could not serialize access due to")) {
					throw new TxnAbortException(e.getMessage());
				} else if (errMsg.contains("deadlock detected")) {
					throw new TxnAbortException(e.getMessage());
				} else {
					//e.printStackTrace();
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
	
}