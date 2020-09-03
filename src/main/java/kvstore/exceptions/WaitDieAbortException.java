package kvstore.exceptions;

public class WaitDieAbortException extends TxnAbortException {

	private String msg = "WaitDieAbortException:";
	
	public WaitDieAbortException(String msg) {
		super(msg);
	}
}
