package kv_interfaces.instrument;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;

import org.apache.commons.codec.binary.Hex;

import com.google.common.io.BaseEncoding;

import bench.chengTxn.ChengTxnConstants;
import main.Config;
import main.Profiler;

public class LogObject {
	String key = null;
	String prev_cl_hash = null; // this indicates the previous logobject's hash from the same client
	ArrayList<byte[]> clogs = null;
	
	public LogObject(String key, String cl_hash) {
		this.key = key;
		this.prev_cl_hash = cl_hash;
		clogs = new ArrayList<byte[]>();
	}
	
	// we need deep copy!
	public LogObject(LogObject p) {
		this.key = p.key;
		this.prev_cl_hash = p.prev_cl_hash;
		this.clogs = new ArrayList<byte[]>();
		for (byte[] arr : p.clogs) {
			this.clogs.add(arr.clone());
		}
	}
	
	public void appendClientLog(ArrayList<byte[]> logs) {
		for (byte[] log : logs) {
			clogs.add(log);
		}
	}
	
	// === Get functions ===
	
	public String getClientLogHash() {
		byte[] all_clog = mergeArraysHelper(clogs);
		byte[] to_sign = aggregateData(all_clog, prev_cl_hash);
		return getHashOrSign(to_sign);
	}
	
	public String getCLkey() {
		return key + Config.get().KEY_CLIENT_LOG_SUFFIX;
	}
	
	public String getCLentry() {
		byte[] all_clog = mergeArraysHelper(clogs);
		if(Config.get().SIGN_DATA && prev_cl_hash.contains("receipt:")) {
			Profiler.getInstance().startTick("getECDSA");
			int receipt = Integer.parseInt(prev_cl_hash.substring(8));
			prev_cl_hash = getECDSA(receipt);
			Profiler.getInstance().endTick("getECDSA");
		}
		// return new String(Hex.encodeHex(all_clog)); // Base64 is as fast as Hex and can save space.
		// return new String(BaseEncoding.base64().encode(all_clog)); // GUAVA is 2x slower than JDK
		return Base64.getEncoder().encodeToString(all_clog) + "," + prev_cl_hash; // Base64 uses 33% extra space.
	}

	// =====================
	// Helper functions
	// =====================
	
	private static byte[] mergeArraysHelper(ArrayList<byte[]> arrays) {
		int len = 0;
		for (byte[] a : arrays) {
			len += a.length;
		}
		byte[] ret = new byte[len];
		int offset = 0;
		for (byte[] a : arrays) {
			System.arraycopy(a, 0, ret, offset, a.length);
			offset += a.length;
		}
		return ret;
	}

	private static byte[] aggregateData(byte[] log, String hash) {
		byte[] hash_bytes = hash.getBytes();
		byte[] data = new byte[log.length + hash_bytes.length];
		System.arraycopy(log, 0, data, 0, log.length);
		System.arraycopy(hash_bytes, 0, data, log.length, hash_bytes.length);
		return data;
	}
	
	private static String getHashOrSign(byte[] data) {
		String res = null;
		if(Config.get().SIGN_DATA) {
			res = "receipt:"+Integer.toString(ECDSA(data));
		} else {
			res = SHA256(data);
		}
		return res;
	}

	// FIXME: may have performance problem! (After profiling: It's true!)
	private static String SHA256(byte[] data) {
		String ret = null;
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] encodedhash = digest.digest(data);
			ret = Base64.getEncoder().encodeToString(encodedhash);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return ret;
	}
	
	private static int ECDSA(byte[] data) {
		SignatureManager sm = SignatureManager.getInstance();
		int receipt = sm.requestSign(data);
		return receipt;
	}
	
	private static String getECDSA(int receipt) {
		SignatureManager sm = SignatureManager.getInstance();
		String sign = null;
		do {
			sign = sm.getSignature(receipt);
		} while (sign == null);
		return sign;
		
	}
	
}