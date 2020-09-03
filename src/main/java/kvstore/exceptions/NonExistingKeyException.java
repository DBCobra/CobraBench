package kvstore.exceptions;

public class NonExistingKeyException extends KvException{
	
	public NonExistingKeyException(String msg) {
		super("NonExistingKeyException: " + msg);
	}

}
