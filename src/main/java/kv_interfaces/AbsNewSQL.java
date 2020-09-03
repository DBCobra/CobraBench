package kv_interfaces;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import bench.Crack.WriteSkewBench;
import kvstore.exceptions.DupInsertException;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnAbortException;
import kvstore.exceptions.TxnException;
import main.Config;

public abstract class AbsNewSQL implements KvInterface {
	
	// init status
	private static boolean init = false;
	// unique txn id
	public AtomicLong nextTxnId;
	// connection per thread
	protected Map<Long, NewSQLConnection> tid2conn;
  // ports
	private final String[] possible_urls;
	private final int[] possible_ports;

	
	
	// customized parts
	public abstract Connection getConnection(String url, int port) throws SQLException;
	
	public abstract NewSQLConnection getConn4currentThread();
	public abstract void ReportServerDown();

	
	// ===== singleton =====
	
	AbsNewSQL(String[] urls, int[] ports) {
		nextTxnId = new AtomicLong(((long)Config.get().CLIENT_ID) << 32);
		tid2conn = new ConcurrentHashMap<Long, NewSQLConnection>();
		this.possible_urls = urls;
		this.possible_ports = ports;
	}
	
	// ===== init =====

	// NOTE: should be call when creating the singleton
	// drop and recreate all tables for all benchmarks
	protected void initTable() {
		assert !init;
		init = true;

		String url = possible_urls[0];
		int port = possible_ports[0];
		String drop = "DROP TABLE %s;";
		// https://docs.yugabyte.com/latest/api/ysql/datatypes/type_character/
		String create = "CREATE TABLE %s (key VARCHAR PRIMARY KEY, value VARCHAR);";
		//String[] tables = { "chengTxn", "tpcc", "ycsb", "rubis", "twitter", TABLE_A, TABLE_B };
		String[] tables = { WriteSkewBench.TABLE_A, WriteSkewBench.TABLE_B };

		// drop and create
		for (String tab : tables) {
			System.out.print("[INFO] Start to clear table[" + tab + "]...drop...");
			try {
				Connection conn = getConnection(url, port);
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
				
				// check it is empty
				PreparedStatement st = conn.prepareStatement("SELECT * from " + tab);
				ResultSet rs = st.executeQuery();
				// should return nothing
				if (rs.next()) {
					System.err.println("[ERROR] drop-create table[" + tab + "] is not empty");
				} else {
					System.out.println("[INFO] table[" + tab + "] is empty");
				}
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
				System.exit(-1);
			}
		}
	}

	
	public String Status() {
		String status = "STATUS: ";
		try {
			StringBuilder sb = new StringBuilder(status);
			for (long tid : tid2conn.keySet()) {
				sb.append("T[" + tid + "][" + tid2conn.get(tid).url + ":" + tid2conn.get(tid).port +"] ");
			}
			status = sb.toString();
		} catch (Exception e) {
			System.out.print("[ERROR] print status meet some error: " + e.getMessage());
		}
		return status;
	}
	
	
	//======== APIs ===========
	

	private String[] DecodeTableKey(String key) {
		// NOTE: key == <table>###<real key>
		String[] ret = key.split("###");
		assert ret.length == 2;
		return ret;
	}

	public Object begin() throws KvException, TxnException {
		return getConn4currentThread().begin();
	}

	@Override
	public boolean commit(Object txn) throws KvException, TxnException {
		return getConn4currentThread().commit(txn);
	}

	@Override
	public boolean abort(Object txn) throws KvException, TxnException {
		return getConn4currentThread().abort(txn);
	}

	@Override
	public boolean insert(Object txn, String key, String value) throws KvException, TxnException {
		String[] tokens = DecodeTableKey(key);
		assert tokens.length == 2;
		return getConn4currentThread().insert(txn, tokens[0], tokens[1], value);
	}

	@Override
	public boolean delete(Object txn, String key) throws KvException, TxnException {
		String[] tokens = DecodeTableKey(key);
		assert tokens.length == 2;
		return getConn4currentThread().delete(txn, tokens[0], tokens[1]);
	}

	@Override
	public String get(Object txn, String key) throws KvException, TxnException {
		String[] tokens = DecodeTableKey(key);
		assert tokens.length == 2;
		return getConn4currentThread().get(txn, tokens[0], tokens[1]);
	}

	@Override
	public boolean set(Object txn, String key, String value) throws KvException, TxnException {
		String[] tokens = DecodeTableKey(key);
		assert tokens.length == 2;
		return getConn4currentThread().set(txn, tokens[0], tokens[1], value);
	}

	@Override
	public boolean rollback(Object txn) {
		return getConn4currentThread().rollback(txn);
	}

	@Override
	public boolean isalive(Object txn) {
		return getConn4currentThread().isalive(txn);
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
		for (NewSQLConnection conn : tid2conn.values()) {
			total_num_op += conn.num_op;
		}
		return total_num_op;
	}
	
	public void clearNumOp() {
		for (NewSQLConnection conn: tid2conn.values()) {
			conn.num_op = 0;
		}
	}
	
	//
	abstract class NewSQLConnection {
		
		// ==== connection states of each thread ========
		protected Connection conn = null;
		protected Statement writeBuffer = null;
		protected int writeBufferSize = 0;
		protected long currTxnId = -1;
		protected int num_op = 0;
		protected AtomicLong nextTxnId;
		protected String url;
		protected int port;
		
		
		public NewSQLConnection(AtomicLong tid) {
			nextTxnId = tid;

			while (initConnection() != true) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		private boolean initConnection() {
			int i = (new Random().nextInt(possible_ports.length) ) % Config.get().NUM_REPLICA;
			url = possible_urls[i];
	    port = possible_ports[i];
	    try {
				this.conn = getConnection(url, port);
				this.conn.setAutoCommit(false);
				this.conn.setTransactionIsolation(Config.get().getIsolationLevel());
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
			assert conn != null;
			assert currTxnId == -1; // last transaction is finished
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

		public boolean insert(Object txn, String table, String key, String value) throws KvException, TxnException {
			assert conn != null;
			assert writeBuffer != null;
			assert (Long) txn == currTxnId && currTxnId != -1;
			num_op++;

			try {
				String sqlstmt = String.format("INSERT INTO " + table + " (key, value) VALUES ('%s', '%s')", key, value);
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
					value = rs.getString("value");
					//value = new String(rs.getBytes("value"));
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
				String sqlstmt = String.format("UPDATE " + table + " SET value = '%s' WHERE key = '%s'", value, key);
				writeBuffer.addBatch(sqlstmt);
				writeBufferSize++;
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
