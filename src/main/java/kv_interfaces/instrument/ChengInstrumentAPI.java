package kv_interfaces.instrument;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Config;
import net.openhft.hashing.LongHashFunction;

public class ChengInstrumentAPI {
	
	public static void main(String[] args) {
		String val = "asdasas";
		long txnid = 1222313;
		long wid = 0xdeadbeefL;
		String encoded_str = OpEncoder.encodeCobraValue(val, txnid, wid);
		long t1 = System.currentTimeMillis(); 
		for(int i = 0; i < 1600000; i++) {
			OpEncoder.decodeCobraValue(encoded_str);
		}
		long t2 = System.currentTimeMillis();
		System.out.println(t2 - t1);
	}

	
	enum WRITE_TYPE {INSERT, UPDATE, DELETE};
	
	static class OpEncoder {
		public String val;
		public long txnid;
		public long wid;
		
		public OpEncoder(String val, long txnid, long wid) {
			this.val = val;
			this.txnid = txnid;
			this.wid = wid;
		}
		
		@Override
		public String toString() {
			return "val: " + val + ", txnid: " + txnid + ", wid: " + wid;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof OpEncoder) {
				OpEncoder that = (OpEncoder) obj;
				return this.val.equals(that.val) && this.txnid == that.txnid
						&& this.wid == that.wid;
			} else {
				return false;
			}
		}
		
		public static String encodeCobraValue(String val, long txnid, long wid) {
			ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
			buffer.putLong(txnid);
			buffer.putLong(wid);
			String str_sig = Base64.getEncoder().encodeToString(buffer.array());
			String val_sign = str_sig + val;
			return "&"+val_sign; // to mark this thing is encoded
		}
		
		public static OpEncoder decodeCobraValue(String encoded_str) {
			try {
				if(encoded_str.length() < 25 || encoded_str.charAt(0) != '&') {
					return null;
				}
				String str_sig = encoded_str.substring(1, 25);
				String real_val = encoded_str.substring(25);
				byte[] barray = Base64.getDecoder().decode(str_sig);
				ByteBuffer bf = ByteBuffer.wrap(barray);
				long txnid = bf.getLong();
				long wid = bf.getLong();
				return new OpEncoder(real_val, txnid, wid);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
	}
		
	// ==========================
	// === Begin/Commit/Abort ===
	// ==========================
	
	public static void doTransactionBegin(long txnid) {
		ChengClientState.initTxnId(txnid);
		ChengLogger logger = ChengLogger.getInstance();
		logger.txnStart(txnid);
		logger.debug("Txn[" + Long.toHexString(txnid) + "] start");
		
		assert ChengClientState.getInstance().r_set.size() == 0;
	}

	public static LogObject doTransactionCommitPre(KvInterface kvi, long txnid) throws TxnException {
		ChengLogger logger = ChengLogger.getInstance();
		
		logger.txnCommitPre(txnid); // add the COMMIT to byte array
		logger.debug("Txn[" + Long.toHexString(txnid) + "] try to commit");
		
		// if online log, construct the log entity
		if (Config.get().CLOUD_LOG) {
			// (1) update the opLogBuffer to log object
			// (2) return the log object
			LogObject lobj = ChengClientState.append2logobj(logger.getOpLogBuffer());
			return lobj;
		}
		return null;
	}
	
	public static void doTransactionCommitPost(long txnid) {
		ChengLogger logger = ChengLogger.getInstance();
		logger.txnCommitPost(txnid);
		ChengClientState.getInstance().epochTxnNum++;
		logger.debug("Txn[" + Long.toHexString(txnid) + "] successfully commit");
		
		ChengClientState.removeTxnId();
		// if Cloud_LOG, need to move forward the entity and hash
		if (Config.get().CLOUD_LOG) {
			// txn committed successfully, so we can safely moving forward by
			// setting the clog_hash/wo_hash and entity
			ChengClientState cs = ChengClientState.getInstance();
			if (cs == null) return;
			cs.successCommitOneEntity();
		}

		ChengClientState.getInstance().r_set.clear();
	}

	// abort might happen outside txn
	public static void doTransactionRollback(long txnid) {
		if (!ChengClientState.inTxn()) return;
		
		ChengLogger logger = ChengLogger.getInstance();
		logger.txnAbort(txnid);
		logger.debug("Txn[" + Long.toHexString(txnid) + "] abort");
		
		ChengClientState.removeTxnId();
		// if Cloud_LOG, need to roll-back the entity and hash
		if (Config.get().CLOUD_LOG) {
			ChengClientState cs = ChengClientState.getInstance();
			if (cs == null) return;
			cs.rollbackLogObj();
		}
		
		ChengClientState.getInstance().r_set.clear();
	}
	
	// ======================
	// === Read and Write ===
	// =======================
			
	public static String doTransactionInsert(KvInterface kvi, long txnid, String key, String val) throws TxnException {
		return doWrite(kvi, txnid, key, val, WRITE_TYPE.INSERT);
	}
	
	public static void doTransactionDelete(KvInterface kvi, long txnid, String key) throws TxnException {
		doWrite(kvi, txnid, key, null, WRITE_TYPE.DELETE);
	}
	
	public static String doTransactionGet(long txnid, String key, String val) throws TxnException {
		return doRead(txnid, key, val, true);
	}
	
	public static String doTransactionSet(KvInterface kvi, long txnid, String key, String val) throws TxnException {
		return doWrite(kvi, txnid, key, val, WRITE_TYPE.UPDATE);
	}
	
	private static String doWrite(KvInterface kvi, long txnid, String key, String val, WRITE_TYPE type) throws TxnException {
		assert ChengClientState.inTxn();
		
		// 1. generate a write id
		// if ADD/UPDATE/PUT, generate a new id
		// if DELETE, use NOP_WRITE_ID (because the next INSERT/PUT will use NOP_WRITE_ID as prev_write)
		// FIXME: this might be a problem here for the delete
		long wid = (type == WRITE_TYPE.DELETE) ? LibConstants.DELETE_WRITE_ID : ChengIdGenerator.genWriteId();
		
		// 2. encode a value (key, txnid, wid) for next read
		// len + "," + (key + txnid + wid) + real_value
		String encoded_val = OpEncoder.encodeCobraValue(val, txnid, wid);

		// 3. record the client log
		long val_hash = LongHashFunction.xx().hashChars(encoded_val);
		long key_hash = LongHashFunction.xx().hashChars(key);
		ChengLogger.getInstance().txnWrite(txnid, wid, key_hash, val_hash);
		
		return encoded_val;
	}

	/**
	 * @return Return decoded value if the input is encoded; return null if there is
	 *         any decode error.
	 */
	private static String doRead(long txnid, String key, String val, boolean istxn) {
		assert txnid != 0 && istxn;
		final String real_val;
		final long write_txnid, write_id, key_hash, value_hash;
		ChengLogger logger = ChengLogger.getInstance();

		if (val != null) {
			OpEncoder op = OpEncoder.decodeCobraValue(val);
			if (op == null) {
				// Tricky part: The values written in initialization period are not encoded, so we mark it
				write_txnid = LibConstants.INIT_TXN_ID;
				write_id = LibConstants.INIT_WRITE_ID;
				real_val = val;
			} else {
				write_txnid = op.txnid;
				write_id = op.wid;
				real_val = op.val;
			}
			value_hash = LongHashFunction.xx().hashChars(val);
		} else {
			// Read nothing, might be reading a non-existing key
			write_txnid = LibConstants.NULL_TXN_ID;
			write_id = LibConstants.NULL_WRITE_ID;
			logger.error("Signature is null for key [" + key + "]");
			real_val = null;
			value_hash = 0; // NOTE: this is the value hash for "null" value
		}

		ChengClientState.getInstance().r_set.add(key);

		key_hash = LongHashFunction.xx().hashChars(key);
		logger.txnRead(txnid, write_txnid, write_id, key_hash, value_hash);
		return real_val;
	}
}