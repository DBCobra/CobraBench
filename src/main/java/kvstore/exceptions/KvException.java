package kvstore.exceptions;

public class KvException extends Exception {
	
	private String msg = "KvException: ";

	public KvException(String msg) {
		this.msg += msg;
	}

	public String toString() {
		return msg;
	}

}
