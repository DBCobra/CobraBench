package kvstore.exceptions;

public class DupInsertException extends KvException {

	private static final long serialVersionUID = -5707849109558374935L;
	
	public DupInsertException(String msg) {
		super("DupInsertException: " + msg);
	}
	
}
