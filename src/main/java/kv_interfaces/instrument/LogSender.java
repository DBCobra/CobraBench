package kv_interfaces.instrument;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class LogSender {
	
	public Socket skt = null;
	public BufferedOutputStream out = null;
	
	public LogSender(String hostname, int port) {
		try {
			System.out.println("[INFO] connect to " + hostname + ":" + port);
			skt = new Socket(hostname, port);
			out = new BufferedOutputStream(skt.getOutputStream());
		} catch (UnknownHostException e) {
			e.printStackTrace();
			assert false; // FIXME: stop entirely for now
		} catch (IOException e) {
			e.printStackTrace();
			assert false; // FIXME: stop entirely for now
		}
		System.out.println("[INFO] successfully connected");
	}
	
	public void write(byte[] b) {
		try {
			out.write(b);
		} catch (IOException e) {
			e.printStackTrace();
			assert false; // FIXME: stop entirely for now
		}
	}
	
	public void flush() {
		try {
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
			assert false; // FIXME: stop entirely for now
		}
	}

}
