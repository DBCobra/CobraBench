package kv_interfaces;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

import org.postgresql.ds.PGSimpleDataSource;

import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnAbortException;
import kvstore.exceptions.TxnException;
import main.Config;



public class YugaByteDB extends AbsNewSQL {
	
	private static YugaByteDB instance = null;
	
	
	
	// ======= singleton =======
	
	private YugaByteDB() {
		super(Config.get().YUGABYTE_DB_URLS, Config.get().YUGABYTE_PORTS);
	}
	
	public synchronized static YugaByteDB getInstance() {
		if (instance == null) {
			instance = new YugaByteDB();
			instance.initTable(); // one time init
		}
		return instance;
	}
	
	
	// ====== DB connection ==========

	@Override
	public Connection getConnection(String url, int port) throws SQLException {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
			System.exit(-1);
		}
		// Aug.10.2019
		// https://docs.yugabyte.com/latest/quick-start/build-apps/java/ysql-jdbc/

		String str_conn = "jdbc:postgresql://" + url + ":" + port + "/" + Config.get().YUGABYTE_DATABASE_NAME;

		Connection conn = DriverManager.getConnection(
				str_conn, 
				Config.get().YUGABYTE_USERNAME,
				Config.get().YUGABYTE_PASSWORD);

		return conn;
	}

	//===== status managment ======
	
	public NewSQLConnection getConn4currentThread() {
		long threadId = Thread.currentThread().getId();
		if (!tid2conn.containsKey(threadId)) {
			YugaByteConnection conn = new YugaByteConnection(nextTxnId);
			tid2conn.put(threadId, conn);
			System.out.println("[INFO] tid[" + threadId + "] starts a sql connection at [" + conn.url + ":" + conn.port + "]");
		}
		return tid2conn.get(threadId);
	}
	
	public void ReportServerDown() {
		long threadId = Thread.currentThread().getId();
		assert tid2conn.containsKey(threadId);
		tid2conn.remove(threadId);
	}


	// ======== connection ========
	
	class YugaByteConnection extends NewSQLConnection {

		public YugaByteConnection(AtomicLong tid) {
			super(tid);
		}
		
		public boolean set(Object txn, String table, String key, String value) throws KvException, TxnException {
			assert conn != null;
			assert (Long) txn == currTxnId && currTxnId != -1;
			num_op++;

			try {
				/*
				// batch impl I
				String sqlstmt = String.format("INSERT INTO " + table
						+ " (key, value) VALUES ('%s', '%s') ON CONFLICT (key) DO UPDATE SET value = '%s'", key, value, value);
				writeBuffer.addBatch(sqlstmt);
				writeBufferSize++;
				*/
				
				// batch impl II
				String sqlstmt = String.format("UPDATE " + table + " SET value = '%s' WHERE key = '%s'", value, key);
				writeBuffer.addBatch(sqlstmt);
				writeBufferSize++;

				/*
				// eager impl
				String sqlstmt = String.format("UPDATE " + table + " SET value = '%s' WHERE key = '%s'", value, key);
				PreparedStatement st = conn.prepareStatement(sqlstmt);
				int done = st.executeUpdate();
				assert done == 1;
				*/
				
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
	}
	
}
