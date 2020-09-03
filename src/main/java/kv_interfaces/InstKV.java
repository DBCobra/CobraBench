package kv_interfaces;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.grpc.Status;
import kv_interfaces.instrument.ChengClientState;
import kv_interfaces.instrument.ChengInstrumentAPI;
import kv_interfaces.instrument.ChengLogger;
import kv_interfaces.instrument.EpochUtils;
import kv_interfaces.instrument.LogObject;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Config;
import main.Profiler;

// a wrapper for real KvInterface (the "kvi")
public class InstKV implements KvInterface {
	public final KvInterface kvi;

	public InstKV(KvInterface orig_kvi) {
		this.kvi = orig_kvi;
		assert !kvi.isInstrumented(); // prevent nested instrument
	}

	@Override
	public Object begin() throws KvException, TxnException {
		Object txn = kvi.begin();
		long txnid = kvi.getTxnId(txn);
		ChengInstrumentAPI.doTransactionBegin(txnid);
		return txn;
	}

	@Override
	public boolean commit(Object txn) throws KvException, TxnException {
		return commit(txn, true);
	}
	
	public boolean commit(Object txn, boolean updateEpoch) throws KvException, TxnException {
		long txnid = kvi.getTxnId(txn);
		ChengClientState status = ChengClientState.getInstance();
		
		// NOTE: commit might fail; we simply cache the read result
		// UTBABUG: much do read before CommitPre()
		String epoch_str = null;
		if (Config.get().USE_NEW_EPOCH_TXN && status.monitoring) {
			epoch_str = get(txn, Config.get().EPOCH_KEY); // read EPOCH
			assert epoch_str != null;
			status.extra_op++;
			status.num_monitoring_txns++;
		}
		
		LogObject lo = ChengInstrumentAPI.doTransactionCommitPre(kvi, txnid);

		if (Config.get().CLOUD_LOG) {
			assert lo != null;
			String clkey = lo.getCLkey();
			String clval = lo.getCLentry();
			boolean success = kvi.set(txn, clkey, clval);
			assert success; // there should be no contention on this
		}

		boolean ret = kvi.commit(txn);

		// [cheng: ??? shouldn't we check "ret"? what if commit fails?
		assert ret;
		
		ChengInstrumentAPI.doTransactionCommitPost(txnid);
		
		if (!updateEpoch) { return ret; }
		
		if (status.epochTxnNum > Config.get().MAX_FZ_TXN_NUM) {
			if (Config.get().USE_NEW_EPOCH_TXN) {
			// in Monitoring: decide whether we should switch back to normal mode
			// out of Monitoring: increase epoch number, issue a new epoch txn
			if (status.monitoring) {
				assert epoch_str != null;
				status.prev_epochs = EpochUtils.decodeEpochs(epoch_str);
				status.monitoring = EpochUtils.isMonitoring(status);
				EpochUtils.checkEpochs(status);
				if (!status.monitoring) {
					status.epochTxnNum = 0; // reset counter when switch back to normal mode
				}

				// System.out.println("[DEBUG]  [" + status.getClientName() + "] was monitoring; stay monitoring?[" + status.monitoring
				//		+ "]; epoch=" + status.curEpoch + " counter=" + status.epochTxnNum);
			} else { // monitoring == false
				if (EpochUtils.issueEpochTxn(this, status)) {
					// isseueEpochTxn() succeed means:
					// 1) my_epoch+1 has been written to DB, so we can make it happen locally as well
					status.curEpoch += 1;
					// 2) check if start monitoring
					status.monitoring = EpochUtils.isMonitoring(status);
					EpochUtils.checkEpochs(status);
					if (!status.monitoring) {
						status.epochTxnNum = 0; // reset counter when switch back to normal mode
					}
					
					//System.out.println("[DEBUG]  [" + status.getClientName() + "] increase epoch to [" + status.curEpoch
					//		+ "]; monitoring=" + status.monitoring + "; counter=" + status.epochTxnNum);
				}
			}
			} else { // not USE_NEW_EPOCH_TXN; use fence txn
				if (updateVersion(status)) {
					status.epochTxnNum = 0;
				}
			}
		}
		
		return ret;
	}

	@Override
	public boolean abort(Object txn) throws KvException, TxnException {
		boolean ret = kvi.abort(txn);
		ChengInstrumentAPI.doTransactionRollback(kvi.getTxnId(txn));
		return ret;
	}

	@Override
	public boolean insert(Object txn, String key, String value) throws KvException, TxnException {
		String encoded_val = ChengInstrumentAPI.doTransactionInsert(kvi, kvi.getTxnId(txn), key, value);
		boolean ret = kvi.insert(txn, key, encoded_val);
		return ret;
	}

	@Override
	public boolean delete(Object txn, String key) throws KvException, TxnException {
		boolean ret = kvi.delete(txn, key);
		ChengInstrumentAPI.doTransactionDelete(kvi, kvi.getTxnId(txn), key);
		return ret;
	}

	@Override
	public String get(Object txn, String key) throws KvException, TxnException {
		String val = kvi.get(txn, key);
		String real_val = ChengInstrumentAPI.doTransactionGet(kvi.getTxnId(txn), key, val);
		return real_val;
	}

	@Override
	public boolean set(Object txn, String key, String value) throws KvException, TxnException {
		
		if (Config.get().USE_NEW_EPOCH_TXN && ChengClientState.getInstance().gc_monitoring) {
			// only read the key when necessary
			if (!ChengClientState.getInstance().r_set.contains(key)) {
				get(txn, key); // in monitoring mode, write => RMW 
			}
			ChengClientState.getInstance().extra_op += 1;
		}
		
		String encoded_val = ChengInstrumentAPI.doTransactionSet(kvi, kvi.getTxnId(txn), key, value);

		boolean ret = kvi.set(txn, key, encoded_val);
		return ret;
	}

	@Override
	public boolean rollback(Object txn) {
		boolean ret = kvi.rollback(txn);
		ChengInstrumentAPI.doTransactionRollback(kvi.getTxnId(txn));
		return ret;
	}

	@Override
	public boolean isalive(Object txn) {
		return kvi.isalive(txn);
	}

	@Override
	public long getTxnId(Object txn) {
		return kvi.getTxnId(txn);
	}

	@Override
	public boolean isInstrumented() {
		return true;
	}

	@Override
	public Object getTxn(long txnid) {
		return kvi.getTxn(txnid);
	}
	
	public void keyaccessed() {
	}
	
	public long getTraceSize() {
		return -1;
	}
	
	// ======== fence txn ========
	
	public boolean updateVersion(ChengClientState status) {
		Object txn = null;
		try {
			txn = begin();
			// op[0]: read from EPOCH_KEY
			String sVersion = get(txn, Config.get().EPOCH_KEY);
			long version = 0;
			if (sVersion != null) {
				version = Long.parseLong(sVersion);
			} else {
				insert(txn, Config.get().EPOCH_KEY, String.valueOf(version + 1));
				status.curEpoch = version + 1;
				return commit(txn, false);
			}
			
			// check if local_epoch is larger
			long localVersion = status.curEpoch;
			if (version > localVersion) {
				status.curEpoch = version;
				return commit(txn, false);
			} else if (version == localVersion) {
				// op[1]: write a new epoch to EPOCH_KEY
				set(txn, Config.get().EPOCH_KEY, String.valueOf(version + 1));
				boolean commitSuccess = commit(txn, false);
				status.curEpoch = version + 1; // this line must be after commit in case of Exception->rollback
				return commitSuccess;
			} else {
				System.out.println("Error: remote fzVersion: " + version + ", local fzVersion: " + localVersion);
				System.exit(-1);
			}
		} catch (KvException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (TxnException e) {
			// System.out.println("update version failed because of txn abort");
			rollback(txn);
			return false;
		}
		return false;
	}

	
}
