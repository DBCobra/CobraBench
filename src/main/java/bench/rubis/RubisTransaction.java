package bench.rubis;

import java.util.HashMap;

import com.google.api.client.util.Key;

import bench.Tables;
import bench.Transaction;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public abstract class RubisTransaction extends Transaction {

	public RubisTransaction(KvInterface kvi, String name) {
		super(kvi, name);
	}

	// Table Region_Name2Id
	protected int getRegionId(String RegionName) throws KvException, TxnException {
		check_txn_exists();
		String keys[] = { RegionName };
		String key = Tables.encodeKey(RubisConstants.TBL_Region_Name2Id, keys);
		String res = kvi.get(txn, key);
		if (res == null) {
			return -1;
		}
		return Integer.valueOf((String) Tables.decodeTable(res).get("Region_id"));
	}

	// Table Region_Name2Id
	private boolean insertRegionId(String RegionName, int RegionId) throws KvException, TxnException {
		check_txn_exists();
		String keys[] = { RegionName };
		String key = Tables.encodeKey(RubisConstants.TBL_Region_Name2Id, keys);
		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("Region_id", Integer.toString(RegionId));
		String val = Tables.encodeTable(row);
		return kvi.insert(txn, key, val);
	}

	// This function will insert 2 times: Region table and RegionName2Id table
	protected boolean insertRegion(String RegionName, int RegionId) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { RegionId };
		String key = Tables.encodeKey(RubisConstants.TBL_Region, keys);
		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("Region_name", RegionName);
		String val = Tables.encodeTable(row);
		return kvi.insert(txn, key, val) && insertRegionId(RegionName, RegionId);
	}

	/** Table Region: RegionId -> RegionName */
	protected String getRegionName(int RegionId) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { RegionId };
		String key = Tables.encodeKey(RubisConstants.TBL_Region, keys);
		String res = kvi.get(txn, key);
		return (String) Tables.decodeTable(res).get("Region_name");
	}

	/**
	 * Table User: UserId -> {firstName, lastName, nickName, password, email,
	 * balance, creation_date} <br>
	 * This function will insert 2 tables: User and UserNickName
	 */
	protected boolean insertUser(int userId, String firstName, String lastName, String nickName, String password,
			String email, float balance, String creationDate) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { userId };
		String key = Tables.encodeKey(RubisConstants.TBL_User, keys);
		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("firstName", firstName);
		row.put("lastName", lastName);
		row.put("nickName", nickName);
		row.put("password", password);
		row.put("email", email);
		row.put("balance", balance);
		row.put("creationDate", creationDate);
		String val = Tables.encodeTable(row);
		return kvi.insert(txn, key, val) && insertUserNickName(nickName, userId, password);
	}

	/**
	 * Table rating: UserId->{rating}
	 */
	protected boolean insertRating(int userId, int rating, boolean update) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { userId };
		String key = Tables.encodeKey(RubisConstants.TBL_Rating, keys);
		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("rating", rating);
		String val = Tables.encodeTable(row);
		if(update) {
			return kvi.set(txn, key, val);
		} else {
			return kvi.insert(txn, key, val);
		}
	}
	
	protected HashMap<String, Object> getRating(int userId) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { userId };
		String key = Tables.encodeKey(RubisConstants.TBL_Rating, keys);
		String res = kvi.get(txn, key);
		return Tables.decodeTable(res);
	}

	protected boolean updateUser(int userId, HashMap<String, Object> val) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { userId };
		String key = Tables.encodeKey(RubisConstants.TBL_User, keys);
		String value = Tables.encodeTable(val);
		return kvi.set(txn, key, value);
	}
	
	// Table User
	protected HashMap<String, Object> getUser(int userId) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { userId };
		String key = Tables.encodeKey(RubisConstants.TBL_User, keys);
		String res = kvi.get(txn, key);
		return Tables.decodeTable(res);
	}

	/**
	 * Table UserNickName: nickName -> {userId, password}
	 */
	private boolean insertUserNickName(String nickName, int userId, String password) throws KvException, TxnException {
		check_txn_exists();
		String keys[] = { nickName };
		String key = Tables.encodeKey(RubisConstants.TBL_UserNickName, keys);
		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("password", password);
		row.put("userId", userId);
		String val = Tables.encodeTable(row);
		return kvi.insert(txn, key, val);

	}

	/**
	 * Table userNickName
	 */
	protected HashMap<String, Object> getUserByNick(String nickName) throws KvException, TxnException {
		check_txn_exists();
		String keys[] = { nickName };
		String key = Tables.encodeKey(RubisConstants.TBL_UserNickName, keys);
		String res = kvi.get(txn, key);
		return Tables.decodeTable(res);
	}

	/**
	 * Table Comment: commentId -> {fromId, toId, itemId, rating, date, comment}
	 */
	protected boolean insertComment(int commentId, int fromId, int toId, int itemId, int rating, String date,
			String comment) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { commentId };
		String key = Tables.encodeKey(RubisConstants.TBL_Comment, keys);
		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("commentId", commentId);
		row.put("fromId", fromId);
		row.put("toId", toId);
		row.put("itemId", itemId);
		row.put("rating", rating);
		row.put("comment", comment);
		String val = Tables.encodeTable(row);
		return kvi.insert(txn, key, val);
	}

	protected boolean insertItem(int itemId, String name, String description, float iPrice, int quantity, float rPrice,
			float buyNow, int nbOfBids, float maxBid, int seller, int category) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { itemId };
		String key = Tables.encodeKey(RubisConstants.TBL_Item, keys);

		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("name", name);
		row.put("description", description);
		row.put("iPrice", iPrice);
		row.put("quantity", quantity);
		row.put("rPrice", rPrice);
		row.put("buyNow", buyNow);
		row.put("nbOfBids", nbOfBids);
		row.put("maxBid", maxBid);
		row.put("seller", seller);
		row.put("category", category);
		String val = Tables.encodeTable(row);

		return kvi.insert(txn, key, val);
	}

	protected boolean updateItem(int itemId, HashMap<String, Object> val) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { itemId };
		String key = Tables.encodeKey(RubisConstants.TBL_Item, keys);
		String value = Tables.encodeTable(val);
		return kvi.set(txn, key, value);
	}

	protected HashMap<String, Object> getItem(int itemId) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { itemId };
		String key = Tables.encodeKey(RubisConstants.TBL_Item, keys);
		String res = kvi.get(txn, key);
		return Tables.decodeTable(res);
	}

	protected boolean insertBuyNow(int buyId, int userId, int itemId, int qty) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { buyId };
		String key = Tables.encodeKey(RubisConstants.TBL_BuyNow, keys);

		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("buyId", buyId);
		row.put("userId", userId);
		row.put("itemId", itemId);
		row.put("qty", qty);
		String val = Tables.encodeTable(row);

		return kvi.insert(txn, key, val);
	}

}
