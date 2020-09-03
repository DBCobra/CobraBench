package kv_interfaces.instrument;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import kv_interfaces.instrument.ChengLogger.OP_TYPE;
import main.Config;


public class LogEntry {

	static byte op2byte(ChengLogger.OP_TYPE op) {
		switch (op) {
		case START_TXN:
			return (byte) 'S';
		case COMMIT_TXN:
			return (byte) 'C';
		case READ:
			return (byte) 'R';
		case WRITE:
			return (byte) 'W';
		case ABORT_TXN:
			return (byte) 'A';
		default:
			System.out.println("wrong type " + op);
			System.exit(-1);
			return (byte) '?';
		}
	}
	
	/**
	 * (startTx, txnId) : 9B <br>
	 * (commitTx, txnId) : 9B <br>
	 * (write, writeId, key, val): 25B <br>
	 * (read, write_TxnId, writeId, key, value) : 33B <br>
	 */
	static byte[] toOplogEntry_keyhash(ChengLogger.OP_TYPE op, long txnid, long write_id, long key_hash, long value_hash,
			long prev_txnid) {
		ByteBuffer entry = null;
		if (op == ChengLogger.OP_TYPE.START_TXN || op == ChengLogger.OP_TYPE.ABORT_TXN
				|| op == ChengLogger.OP_TYPE.COMMIT_TXN) {
			entry = ByteBuffer.allocate(9);
			entry.put(op2byte(op));
			entry.putLong(txnid);
		} else if (op == ChengLogger.OP_TYPE.WRITE) {
			entry = ByteBuffer.allocate(25);
			entry.put(op2byte(op));
			entry.putLong(write_id);
			entry.putLong(key_hash);
			entry.putLong(value_hash);
		} else if (op == ChengLogger.OP_TYPE.READ) {
			entry = ByteBuffer.allocate(33);
			entry.put(op2byte(op));
			entry.putLong(prev_txnid);
			entry.putLong(write_id);
			entry.putLong(key_hash);
			entry.putLong(value_hash);
		}
		assert !entry.hasRemaining();
		return entry.array();
	}

	static void fromOplogEntry_keyhash(byte[] entry) {
		long txnid, write_id, key_hash, value_hash, prev_txnid;
		ByteBuffer buffer = ByteBuffer.wrap(entry);

		byte op = buffer.get();
		switch (op) {
		case 'S':
		case 'C':
		case 'A':
			txnid = buffer.getLong();
			System.out.println((char) op + " " + txnid);
			break;
		case 'R':
			prev_txnid = buffer.getLong();
			write_id = buffer.getLong();
			key_hash = buffer.getLong();
			value_hash = buffer.getLong();
			System.out.println("R " + prev_txnid + ", " + write_id + ", " + key_hash + ", " + value_hash);
			break;
		case 'W':
			write_id = buffer.getLong();
			key_hash = buffer.getLong();
			value_hash = buffer.getLong();
			System.out.println("W " + write_id + ", " + key_hash + ", " + value_hash);
			break;
		default:
			System.out.println("wrong type of op: " + op);
			break;
		}

		if (buffer.hasRemaining()) {
			System.out.println("ERROR");
			System.exit(-1);
		}
	}
	
	/**
	 * (startTx, txnId [, timestamp]) : 9B [+8B] <br>
	 * (commitTx, txnId [, timestamp]) : 9B [+8B] <br>
	 * (write, writeId, key_len, key, val): ?B <br>
	 * (read, write_TxnId, writeId, key_len, key, value) : ?B <br>
	 */
	
	static byte[] toOplogEntry_key(ChengLogger.OP_TYPE op, long txnid, long write_id, String key, long value_hash,
			long prev_txnid) {
		// START/COMMIT
		if (op == ChengLogger.OP_TYPE.START_TXN || op == ChengLogger.OP_TYPE.ABORT_TXN
				|| op == ChengLogger.OP_TYPE.COMMIT_TXN) {
			ByteBuffer entry = ByteBuffer.allocate(Config.get().TIMESTAMP_ON ? 17 : 9);
			entry.put(op2byte(op));
			entry.putLong(txnid);
			if (Config.get().TIMESTAMP_ON) {
				long ts = System.currentTimeMillis();
				entry.putLong(ts);
			}
			assert !entry.hasRemaining();
			return entry.array();
		}
		// READ/WRITE
		ByteArrayDataOutput entry_stream = ByteStreams.newDataOutput(); // from Guava
		if (op == ChengLogger.OP_TYPE.WRITE) {
			entry_stream.write(op2byte(op));
			entry_stream.writeLong(write_id);
			byte[] key_bytes = key.getBytes(StandardCharsets.UTF_8); // key
			int len = key_bytes.length; // key
			entry_stream.writeInt(len); // key
			entry_stream.write(key_bytes); // key
			entry_stream.writeLong(value_hash);
		} else if (op == ChengLogger.OP_TYPE.READ) {
			entry_stream.write(op2byte(op));
			entry_stream.writeLong(prev_txnid);
			entry_stream.writeLong(write_id);
			byte[] key_bytes = key.getBytes(StandardCharsets.UTF_8); // key
			int len = key_bytes.length; // key
			entry_stream.writeInt(len); // key
			entry_stream.write(key_bytes); // key
			entry_stream.writeLong(value_hash);
		} else {
			assert false;
		}
		return entry_stream.toByteArray();
	}

	static void fromOplogEntry_key(byte[] entry) {
		int key_len;
		long txnid, write_id, value_hash, prev_txnid;
		ByteBuffer buffer = ByteBuffer.wrap(entry);

		byte op = buffer.get();
		switch (op) {
		case 'S':
		case 'C':
		case 'A':
			txnid = buffer.getLong();
			System.out.println((char) op + " " + txnid);
			break;
		case 'R': {
			prev_txnid = buffer.getLong();
			write_id = buffer.getLong();
			key_len = buffer.getInt();
			byte[] key = new byte[key_len];
			buffer.get(key);
			value_hash = buffer.getLong();
			System.out.println("R " + prev_txnid + ", " + write_id + ", " + new String(key, StandardCharsets.UTF_8) + ", " + value_hash);
			break;
		  }
		case 'W': {
			write_id = buffer.getLong();
			key_len = buffer.getInt();
			byte[] key = new byte[key_len];
			buffer.get(key);
			value_hash = buffer.getLong();
			System.out.println("W " + write_id + ", " + new String(key, StandardCharsets.UTF_8) + ", " + value_hash);
			break;
		  }
		default:
			System.out.println("wrong type of op: " + op);
			break;
		}

		if (buffer.hasRemaining()) {
			System.out.println("ERROR");
			System.exit(-1);
		}
	}
	
	static void verifierTest(byte[] all) throws IOException {
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(all));

    char op_type;
    int key_len = 0;
		long wid = 0, val_hash = 0, prev_txnid = 0, txnid = 0;
		
		while(true) {
			// break if end (for file)
			try {
				op_type = (char) in.readByte();
			} catch (EOFException e) {
				break;
	    }
			
			switch(op_type) {
			case 'S':
				txnid = in.readLong();
				System.out.println("S[" + txnid + "]");
				break;
			case 'C':
				txnid = in.readLong();
				System.out.println("C[" + txnid + "]");
				break;
			case 'A':
				txnid = in.readLong();
				System.out.println("A[" + txnid + "]");
				break;
			case 'W': {
				 // (write, writeId, key_len, key, val): ?B <br>			
				wid = in.readLong();
				key_len = in.readInt();
				byte[] key = new byte[key_len];
				in.read(key);
				val_hash = in.readLong();
				System.out.println("W " + wid + ", " + new String(key, StandardCharsets.UTF_8) + ", " + val_hash);
				break;
			}
			case 'R': {
				 // (read, write_TxnId, writeId, key_len, key, value) : ?B <br>
				prev_txnid = in.readLong();
				wid = in.readLong();
				key_len = in.readInt();
				byte[] key = new byte[key_len];
				in.read(key);
				val_hash = in.readLong();
				System.out.println("R " + prev_txnid + ", " + wid + ", " + new String(key, StandardCharsets.UTF_8) + ", " + val_hash);
				break;
			}
			default:
				assert false;
			}  // end of switch
		}
	}

	public static void main(String[] args) {
		int num_bytes = 0;
		ArrayList<byte[]> all_bytes = new ArrayList<byte[]>();
		for (int i = 0; i < 100; i++) {
			byte[] tmp = toOplogEntry_keyhash(OP_TYPE.values()[i % 5], i, i + 1, (i + 2), i + 3, i + 4);
			fromOplogEntry_keyhash(tmp);
			all_bytes.add(tmp);
			num_bytes += tmp.length;
		}
		
		System.out.println("=============");
		ByteBuffer buffer = ByteBuffer.allocate(num_bytes);
		for (byte[] b : all_bytes) {
			buffer.put(b);
		}
		try {
			verifierTest(buffer.array());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("success!");
	}
}
