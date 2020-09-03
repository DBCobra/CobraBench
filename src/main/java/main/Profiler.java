package main;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Iterables;

public class Profiler {
	
	// global vars
	private static ConcurrentHashMap<Long, Profiler> profilers = new ConcurrentHashMap<Long,Profiler>();
	
	// local vars
	private HashMap<String, Long> start_time = new HashMap<String, Long>();
	private HashMap<String, Long> total_time = new HashMap<String, Long>();
	private HashMap<String, Integer> counter = new HashMap<String, Integer>();

	// efficient thread-safe multi-singleton
	public static Profiler getInstance() {
		// each thread has an unique profiler
		long tid = Thread.currentThread().getId();
		if (!profilers.containsKey(tid)) {
			synchronized (Profiler.class) {
				if (!profilers.containsKey(tid)) {
					profilers.put(tid, new Profiler());
				}
			}
		}
		return profilers.get(tid);
	}
	
	private Profiler() {
		
	}
	
	public long startTick(String tag) {
		if (!counter.containsKey(tag)) {
			counter.put(tag, 0);
			total_time.put(tag, 0L);
		}
		
		// if we haven't stop this tick, stop it!!!
		if (start_time.containsKey(tag)) {
			endTick(tag);
		}
		
		// start the tick!
		long cur_time = System.nanoTime();
		start_time.put(tag, cur_time);
		return cur_time;
	}
	
	public long endTick(String tag) {
		long cur_time = System.nanoTime();
		if (start_time.containsKey(tag)) {
			long duration = cur_time - start_time.get(tag);
			
			// update the counter and total_time
			total_time.put(tag, (total_time.get(tag) + duration));
			counter.put(tag, (counter.get(tag) + 1));
			
			// rm the tick
			start_time.remove(tag);
			return cur_time;
		} else {
			System.out.println("profiler error: Trying to end a nonexiting tag");
			Thread.dumpStack();
			System.exit(-1);
			return -1;
		}
	}
	
	public long getTime(String tag) {
		if (total_time.containsKey(tag)) {
			return total_time.get(tag);
		} else {
			return 0;
		}
	}
	
	public int getCounter(String tag) {
		if (counter.containsKey(tag)) {
			return counter.get(tag);
		} else {
			return 0;
		}
	}
	
	public static long getAggTime(String tag) {
		long time = 0;
		for(Profiler p : profilers.values()) {
			time += p.getTime(tag);
		}
		return time;
	}
	
	public static int getAggCount(String tag) {
		int count = 0;
		for(Profiler p : profilers.values()) {
			count += p.getCounter(tag);
		}
		return count;
	}
	
	public static String[] getTags() {
		HashSet<String> tags = new HashSet<String>();
		for(Profiler p : profilers.values()) {
			tags.addAll(p.counter.keySet());
		}
		String[] res = Iterables.toArray(tags, String.class);
		return res;
	}
	
	public static void main(String[] args) {
		Profiler.getInstance().startTick("outer");
		int a = 0;
		for (int i = 0; i < 10000000; i++) {
			Profiler.getInstance().startTick("inner");
			a++;
			Profiler.getInstance().endTick("inner");
		}
		Profiler.getInstance().endTick("outer");
		System.out.println(a);
		System.out.println("outer: " + Profiler.getAggTime("outer"));
		System.out.println("inner: " + Profiler.getAggTime("inner"));
	}
}
