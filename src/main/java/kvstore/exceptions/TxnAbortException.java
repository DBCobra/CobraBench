package kvstore.exceptions;

public class TxnAbortException extends TxnException {
	public TxnAbortException(String msg) {
		super("TxnAbortException:" + msg);
	}
}
