package kvstore.exceptions;

public class NestedTxnNotSupportedException extends TxnException{

	public NestedTxnNotSupportedException(String msg) {
		super("NestedTxnNotSupportedException: "+msg);
	}
}
