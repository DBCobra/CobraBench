package main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import bench.chengTxn.ChengTxnConstants;

public class Logger {

	private static boolean isInit = false;
	private static FileWriter fwriter = null;
	
	// local log: tid->FileWriter
	private static HashMap<Long, FileWriter> local_loggers = new HashMap<Long, FileWriter>();

	private static void init() {
		// check if folder exists
		File directory = new File(Config.get().COBRA_FD);
		if (!directory.exists()) {
			directory.mkdirs();
		}

		// create the log file
		try {
			fwriter = new FileWriter(Config.get().BENCHMARK_LOG_PATH, Config.get().APPEND_OTHERWISE_RECREATE);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		isInit = true;
	}

	public synchronized static void logln(String prefix, String msg) {
		if (!isInit) init();
		try {
			fwriter.write(prefix + "[" + Thread.currentThread().getId() + "] " + msg + "\n");
			// FIXME: isn't this too expansive?
			fwriter.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized static void logDebug(String msg) {
		if (Config.get().LOG_DEBUG) {
			logln("[DEBUG]",  msg);
		}
	}

	public synchronized static void logInfo(String msg) {
		if (Config.get().LOG_INFO) {
			logln("[INFO]", msg);
		}
	}

	public synchronized static void logError(String msg) {
		if (Config.get().LOG_ERROR) {
			logln("[ERROR]", msg);
		}
	}
	
	// ================
	// Local log functions
	// ================
	
	public synchronized static void startLogger(String name) {
		long tid = Thread.currentThread().getId();	
		String log_name = Config.get().COBRA_FD + name;
		
		try {
			FileWriter fw = new FileWriter(log_name, Config.get().APPEND_OTHERWISE_RECREATE);
			local_loggers.put(tid, fw);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public synchronized static void localwrite(String msg) {
		long tid = Thread.currentThread().getId();	
		assert local_loggers.containsKey(tid);
		FileWriter fw = local_loggers.get(tid);
		try {
			fw.write(msg + "\n");
			// FIXME: isn't this too expansive?
			fw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
