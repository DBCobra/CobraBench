package kv_interfaces.instrument;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.codec.binary.Hex;

import com.google.common.primitives.Longs;

import bench.chengTxn.ChengTxnConstants;
import io.netty.util.internal.ConcurrentSet;
import kvstore.exceptions.TxnException;
import main.Config;
import main.Profiler;
import net.openhft.hashing.LongHashFunction;

// FIXME: this is a logger which combines a FUNCTIONAL LOGGER and a DEBUG/ERROR LOGGER
public class ChengLogger {

	private static final String log_dir = Config.get().COBRA_FD_LOG; // "/tmp/cobra/log/"
	private static Map<Long, ChengLogger> instances = Collections.synchronizedMap(new HashMap<Long, ChengLogger>());

	enum OP_TYPE {
		START_TXN, READ, WRITE, COMMIT_TXN, ABORT_TXN,
	};

	// local vars
	private Path debug_path = null;
	private Path error_path = null;
	private Path log_path = null;
	private long tid = -1;
	
	// socket
	private LogSender log_sender = null;
	private boolean remote_logging = false;
	
	// FIXME: should move this to the client state?
	// local log buffer
	private ArrayList<byte[]> opLogBuffer = new ArrayList<byte[]>();
	
	public ArrayList<byte[]> getOpLogBuffer() {
		return opLogBuffer;
	}

	private ChengLogger(long tid) {
		this.tid = tid;
		Path dir_path = Paths.get(log_dir);
		if (Files.notExists(dir_path)) {
			try {
				Files.createDirectories(dir_path);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		debug_path = Paths.get(log_dir + "T" + this.tid + ".debug");
		error_path = Paths.get(log_dir + "T" + this.tid + ".error");
		log_path = Paths.get(log_dir + "T" + this.tid + ".log");
	}

	public void registerClient() {
			if (Config.get().LOCAL_REMOTE_LOG) {
				remote_logging = true;
				log_sender = new LogSender(Config.get().VERIFIER_HOSTNAME, Config.get().VERIFIER_PORT);
			}
	}
	
	public static ChengLogger getInstance() {
		// one instance for one thread
		long tid = Thread.currentThread().getId();
		if (!instances.containsKey(tid)) {
			ChengLogger one = new ChengLogger(tid);
			instances.put(tid, one);
		}
		return instances.get(tid);
	}
	

	// =======================
	// local log functions
	// =======================

	
	private void write2remoteLog(ArrayList<byte[]> op_buffer) {
		assert remote_logging;
		for (byte[] msg : op_buffer) {
			log_sender.write(msg);
		}
		log_sender.flush();
	}
	
	private void write2clientLog(byte[] msg) {
		try {
			Files.write(log_path, msg, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void debug(String msg) {
		if (LibConstants.DEBUG_LIB_FLAG) {
			msg = "[DEBUG] "+ msg + "\n";
			try {
				Files.write(debug_path, msg.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public void error(String msg) {
		msg = "[ERROR] " + msg + "\n";
		try {
			Files.write(error_path, msg.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// FIXME: should crash here
	}

	// =======================
	// operation recorder
	// =======================

	public void txnStart(long txnid) {
		opLogBuffer.add(LogEntry.toOplogEntry_keyhash(OP_TYPE.START_TXN, txnid, 0, 0, 0, 0));
	}
	
	public void txnCommitPre(long txnid) {
		// add the commit operation to buffer
		opLogBuffer.add(LogEntry.toOplogEntry_keyhash(OP_TYPE.COMMIT_TXN, txnid, 0, 0, 0, 0));
	}
	
	public void txnCommitPost(long txnid) {
		if (Config.get().LOCAL_LOG) {
			// write to the local log
			localTxnCommit(opLogBuffer);
		}

		// clear opLogBuffer
		opLogBuffer.clear();
	}
	
	// dump the log into local files
	private void localTxnCommit(ArrayList<byte[]> opLogBuffer) {
		// commit to opLog
		if (remote_logging) {
			write2remoteLog(opLogBuffer);
		} else {
			for (int i = 0; i < opLogBuffer.size(); i++) {
				write2clientLog(opLogBuffer.get(i));
			}
		}
	}


	public void txnAbort(long txnid) {
		//info(toOplogEntry(OP_TYPE.ABORT_TXN, txnid, 0, 0, 0));
		// abandon the opLogBuffer
		opLogBuffer.clear();
	}

	public void txnRead(long txnid, long write_txnid, long write_id, long key_hash, long value_hash) {
		// NOTE: the sequence of args is not the same
		byte[] entry = LogEntry.toOplogEntry_keyhash(OP_TYPE.READ, txnid, write_id, key_hash, value_hash, write_txnid);
		opLogBuffer.add(entry);
	}

	public void txnWrite(long txnid, long write_id, long key_hash, long value_hash) {
		byte[] entry = LogEntry.toOplogEntry_keyhash(OP_TYPE.WRITE, txnid, write_id, key_hash, value_hash, 0);
		opLogBuffer.add(entry);
	}
}




