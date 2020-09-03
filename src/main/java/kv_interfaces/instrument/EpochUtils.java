package kv_interfaces.instrument;

import java.util.HashMap;
import java.util.Map;

import kv_interfaces.InstKV;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Config;

public class EpochUtils {

	public static boolean issueEpochTxn(InstKV ikv, ChengClientState status) {
		String name = status.getClientName();
		
		Object txn = null;
		try {
			txn = ikv.begin();
			String epoch_str = ikv.get(txn, Config.get().EPOCH_KEY); // OP1
			
			// update my epoch and write to database
			Map<String, Long> db_epochs = decodeEpochs(epoch_str);
			db_epochs.put(name, status.curEpoch + 1); // increase my epoch by 1
			status.prev_epochs = db_epochs;
			ikv.set(txn, Config.get().EPOCH_KEY, encodeEpochs(db_epochs)); // OP2
			// for GC
			String gc_str = ikv.get(txn, Config.get().GC_KEY); // OP3
			boolean succ = ikv.commit(txn, false);
			
			// update status only after successfully committing epoch txn
			assert succ;
			if (gc_str == null || gc_str.equals("false")) {
				status.gc_monitoring = false;
				//System.out.println("gc key[" + Config.get().GC_KEY + "] [off] [" + gc_str + "]");
			} else {
				assert gc_str.equals("true");
				status.gc_monitoring = true;
				System.out.println("gc key[" + Config.get().GC_KEY + "] [on] [" + gc_str + "]");
			}
			return succ;
			
		} catch (KvException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (TxnException e) {
			// System.out.println("update version failed because of txn abort");
			ikv.rollback(txn);
			return false;
		}
		
		return false;
	}
	
	// (1) [bootstrap] when one haven't seen all clients, monitoring = true
	// (2) [normal] check if all clients are in the current epoch
	public static boolean isMonitoring(ChengClientState status) {
		Map<String,Long> prev_epochs = status.prev_epochs;
		String cur_client = status.getClientName();
		long my_epoch = status.curEpoch;

		// (1)
		// FIXME: we assume all machines have the same number of threads
		int all_threads = Config.get().THREAD_NUM * Config.get().CLIENT_NUM;
		if (prev_epochs.size() < all_threads) {
			//System.out.println("[DEBUG]    [" + cur_client + "]  only saw " + prev_epochs.size() + " clients");
			return true;
		}
		
		//System.out.println("[DEBUG]    [" + cur_client + "] what I see: " + ChengClientState.printEpochs(db_epochs));
		//System.out.println("[DEBUG]    [" + cur_client + "] what I know: " + ChengClientState.printEpochs(g_epochs));
		
		// (2)
		boolean monitoring = false;
		assert prev_epochs.keySet().contains(cur_client);
		assert prev_epochs.get(cur_client) == my_epoch; // I issued epoch txn earlier
			
		for (String client : prev_epochs.keySet()) {
			// epoch should be within [my_epoch - 1, my_epoch + 1]
			assert prev_epochs.get(client) >= my_epoch - 1; // TODO: report violation
			assert prev_epochs.get(client) <= my_epoch + 1; // misbehaved client
			
			if (prev_epochs.get(client) == my_epoch - 1) { // means cur_epoch[client] = my_epoch - 1
				monitoring = true;
				//System.out.println("[DEBUG]    [" + cur_client + "]  hmmm...(" + client + ", epoch=" + prev_epochs.get(client) + ") my epoch=" + my_epoch);
			}
		}
	
		return monitoring;
	}
	
	// ===== helper functions =====
	
	private static String encodeEpochs(Map<String,Long> epochs) {
		StringBuilder sb = new StringBuilder();
		for (String c : epochs.keySet()) {
			// NOTE: name might contains weird symbols. We assume ";" is safe here.
			sb.append(c + Config.get().EPOCH_CLIENT_EPOCH_SEP_STR 
					+ Long.toHexString(epochs.get(c)) + Config.get().EPOCH_CLIENTS_SEP_STR);
		}
		return sb.toString();
	}
	
	public static Map<String,Long> decodeEpochs(String str) {
		if (str == null) return new HashMap<String,Long>();
		
		Map<String,Long> epochs = new HashMap<String,Long>();
		String[] elems = str.split(Config.get().EPOCH_CLIENTS_SEP_STR);
		for (String e : elems) {
			String[] client_epoch = e.split(Config.get().EPOCH_CLIENT_EPOCH_SEP_STR);
			assert client_epoch.length == 2;
			String client = client_epoch[0];
			long epoch = Long.parseLong(client_epoch[1], 16);
			assert !epochs.containsKey(client);
			epochs.put(client, epoch);
		}
		return epochs;
	}
	
	private static void checkInitEpochs(ChengClientState status) {
		String client = status.getClientName();
		// if we haven't initialize our own epoch
		if (!status.prev_epochs.containsKey(client)) {
			status.prev_epochs.put(client, 0L); // start with epoch o
		}
	}
	
	
	public static void checkEpochs(ChengClientState status) {
		if (status.prev_epochs.size() < Config.get().THREAD_NUM * Config.get().CLIENT_NUM) { // FIXME: hard coded number here
			assert status.monitoring; // might still be bootstrapping
			return;
		}
		
		assert status.prev_epochs.size() == Config.get().THREAD_NUM * Config.get().CLIENT_NUM; // FIXME: hard coded number here	
		assert status.prev_epochs.get(status.getClientName()) == status.curEpoch;
		
		// in normal mode, we've seen all clients are in the same epoch or one epoch ahead
		if (!status.monitoring) {
			for (String c : status.prev_epochs.keySet()) {
				assert status.prev_epochs.get(c) == status.curEpoch ||
						   status.prev_epochs.get(c) == status.curEpoch + 1;
			}
		}
		// in monitoring mode, all clients should be within [my_epoch - 1, my_epoch]
		else {
			long max = 0;
			long min = Long.MAX_VALUE;
			for (String c : status.prev_epochs.keySet()) {
				max = Math.max(status.prev_epochs.get(c), max);
				min = Math.min(status.prev_epochs.get(c), min);
			}
			assert max - min <= 1;
		}
	}

}
