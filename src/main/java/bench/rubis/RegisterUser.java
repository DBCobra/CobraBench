package bench.rubis;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

import java.time.LocalDateTime;

public class RegisterUser extends RubisTransaction {

	private int userId;
	private String firstName, lastName, nickName, email, password, region, creationDate;
	private float balance;
	private int rating;
	private boolean loading;

	public RegisterUser(KvInterface kvi, int userId, boolean loading) {
		super(kvi, "RegisterUser");
		this.userId = userId;
		this.loading = loading;
	}

	@Override
	public void inputGeneration() {
		firstName = "Great" + userId;
		lastName = "User" + userId;
		nickName = "user" + userId;
		email = firstName + "." + lastName + "@rubis.com";
		password = "password" + userId;
		region = "region" + (userId % RubisConstants.NBOFREGIONS); // we have 62 regions
		creationDate = LocalDateTime.now().toString();
		balance = (float) (userId * 2241215.1241);
		rating = userId % 5;
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
//		System.out.println("insert user " + userId);
		beginTxn();
		if(!loading) {
			int Region_id = getRegionId(region);
			if (Region_id == -1) {
				commitTxn();
				return false;
			}
			if (getUserByNick(nickName) != null) {
				// user already exist
				commitTxn();
				return false;
			}
			if (getUser(userId) != null) {
				// userId already exist, which means the id generator has error
				abortTxn();
				assert false;
				return false;
			}
		}
		boolean res = insertUser(userId, firstName, lastName, nickName, password, email, balance, creationDate);
		res &= insertRating(userId, rating, false);
		commitTxn();

		return res;
	}

}
