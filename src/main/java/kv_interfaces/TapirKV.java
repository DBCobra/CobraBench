package kv_interfaces;

import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.Tapir.Client;

public class TapirKV implements KvInterface {
	
	private static TapirKV instance = null;
  
	private final int num_clients;
	private AtomicInteger global_tid;
	
	private List<Client> clients;
	private List<Boolean> ava_clients;
	private Map<Integer, Integer> tid2client;
	

	
	
	private TapirKV() {
		num_clients = Config.get().THREAD_NUM;
		clients = new ArrayList<Client>();
		ava_clients = new ArrayList<Boolean>();
		for (int i=0; i<num_clients; i++) {
			// configPath, nShards, closestReplica
			clients.add(new Client("/tmp/shard", 1, 0));
			ava_clients.add(true);
		}
		global_tid = new AtomicInteger(0);
		tid2client = new ConcurrentHashMap<Integer,Integer>();
	}
	
	
	
	public synchronized static TapirKV getInstance() {
		if (instance == null) {
			instance = new TapirKV();
		}
		return instance;
	}
	
	private synchronized int findAvailableClient() {
		while(true) {
			for (int i=0; i<ava_clients.size(); i++) {
				if (ava_clients.get(i)) {
					ava_clients.set(i, false);
					return i;
				}
			}
			
			try {
				Thread.sleep(200); // sleep a while
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private synchronized void clientDone(int i) {
		assert ava_clients.get(i) == false;
		ava_clients.set(i, true);
	}
	

	@Override
	public Object begin() throws KvException, TxnException {
    Integer tid = global_tid.getAndIncrement();
		int cid = findAvailableClient();
		assert !tid2client.containsKey(tid);
		tid2client.put(tid, cid);
		
		System.out.println("[TAPIR] txn[" + cid + "] begin on C["+cid+"]");
    clients.get(cid).Begin();
    return tid;
	}

	@Override
	public boolean commit(Object txn) throws KvException, TxnException {
		Integer tid = (Integer) txn;
		Integer cid = tid2client.get(tid);
		assert cid != null;
		assert cid < num_clients && cid >= 0;
		
		System.out.print("[TAPIR] txn[" + cid + "] commit on C["+cid+"] .....");
		boolean succ = clients.get(cid).Commit();
		System.out.println("done?[" + succ + "]");
		clientDone(cid);
		assert tid2client.containsKey(tid);
		tid2client.remove(tid);

		if (!succ) {
			throw new TxnException("Commit failure");
		}
		return succ;
	}

	@Override
	public boolean abort(Object txn) throws KvException, TxnException {
		Integer tid = (Integer) txn;
		Integer cid = tid2client.get(tid);
		assert cid < num_clients && cid >= 0;
		
		System.out.println("[TAPIR] txn[" + cid + "] abort on C["+cid+"]");
    clients.get(cid).Abort();
		clientDone(cid);
		tid2client.remove(tid);
		
    return true;
	}

	@Override
	public boolean insert(Object txn, String key, String value) throws KvException, TxnException {
     return set(txn, key, value);
	}

	@Override
	public boolean delete(Object txn, String key) throws KvException, TxnException {
		assert false;
		return false;
	}

	@Override
	public String get(Object txn, String key) throws KvException, TxnException {
		Integer tid = (Integer) txn;
		Integer cid = tid2client.get(tid);
		assert cid < num_clients && cid >= 0;
		String value = clients.get(cid).Get(key);
		System.out.println("[TAPIR] txn[" + cid + "] read (" + key + "=>" + value + ") on C[" + cid + "]");
    if (value.isEmpty()) {
       return null;
    }
    return value;
	}

	@Override
	public boolean set(Object txn, String key, String value) throws KvException, TxnException {
		Integer tid = (Integer) txn;
		Integer cid = tid2client.get(tid);
		assert cid < num_clients && cid >= 0;
		// XXX: what is this?
		System.out.println("[TAPIR] txn[" + cid + "] write (" + key + "<=" + value + ") on C[" + cid + "]");
		int no_idea_what_is_this = clients.get(cid).Put(key, value);
		return true;
	}

	@Override
	public boolean rollback(Object txn) {
		// do nothing
		return true;
	}

	@Override
	public boolean isalive(Object txn) {
		Integer tid = (Integer) txn;
		return tid2client.keySet().contains(tid);
	}

	@Override
	public long getTxnId(Object txn) {
		return ((Integer) txn);
	}

	@Override
	public Object getTxn(long txnid) {
		// look into alive, and find the corresponding Integer object
		for (Integer tid : tid2client.keySet()) {
			if (txnid == (long)tid) {
				return tid;
			}
		}
		assert false;
		return null;
	}

	@Override
	public boolean isInstrumented() {
		return false;
	}

}
