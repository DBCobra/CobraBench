package kvstore.exceptions;

public class NotInTxnException extends TxnException{

	public NotInTxnException(String msg) {
		super("NotInTxnException: " + msg);
	}

}
