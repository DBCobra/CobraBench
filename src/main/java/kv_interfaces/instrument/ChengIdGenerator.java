package kv_interfaces.instrument;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import main.Config;
import net.openhft.hashing.LongHashFunction;

public class ChengIdGenerator {
	// FIMXE: performance
	private static String clientId = "cid["+Config.get().CLIENT_ID+"]";
	private static Map<Long, AtomicLong> widCounters = Collections.synchronizedMap(new HashMap<Long,AtomicLong>());
	//private static Map<Long, AtomicLong> txnidCounters = Collections.synchronizedMap(new HashMap<Long,AtomicLong>());
	private static Map<Long, Long> threadIdtoKey = new ConcurrentHashMap<Long, Long>();
	private static AtomicLong nextThreadKey = new AtomicLong(0);
	
	// debug: FIXME: remove this later
	private static Set<Long> assigned_wid = ConcurrentHashMap.newKeySet();
 
	// WriteId = "ClientID"-"ThreadID"-"counter"
	public static long genWriteId() {
		long tid = Thread.currentThread().getId();
		if (!widCounters.containsKey(tid)) {
			widCounters.put(tid, new AtomicLong(0L));
		}
		long counter = widCounters.get(tid).incrementAndGet();
		// make sure that this long is positive
		long wid = LongHashFunction.xx().hashChars(clientId + tid +"-" + counter) & 0x0FFFFFFFFFFFFFFFL;
		
		// XXX: not exactly right; but might be OK for now
		if (assigned_wid.contains(wid)) {
			System.err.println("[ERROR] duplicated wid!!!! wid=" + Long.toHexString(wid));
			System.exit(-1);
		}
		assigned_wid.add(wid);
		
		return wid;
	}

	// FIXME: for human-understandable log
	public static String genClientThreadId() {
		/*
		long tid = Thread.currentThread().getId();
		long millis = System.currentTimeMillis();
		return Long.toHexString(LongHashFunction.xx().hashChars("" + clientId + tid + millis));
		*/
		long threadId = Thread.currentThread().getId();
		if(!threadIdtoKey.containsKey(threadId)) {
			threadIdtoKey.put(threadId, nextThreadKey.getAndIncrement());
		}
		long threadKey = threadIdtoKey.get(threadId);
		return clientId+"_T"+threadKey;
	}
}