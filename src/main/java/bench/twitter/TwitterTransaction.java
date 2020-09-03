package bench.twitter;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;

import bench.BenchUtils;
import bench.Tables;
import bench.Transaction;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public abstract class TwitterTransaction extends Transaction {

	public TwitterTransaction(KvInterface kvi, String name) {
		super(kvi, name);
	}

	/**
	 * Table User: id->(name, info)
	 */
	protected boolean insertUser(int userId, String userName, String userInfo) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { userId };
		String key = Tables.encodeKey(TwitterConstants.TBL_User, keys);
		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("name", userName);
		row.put("info", userInfo);
		String val = Tables.encodeTable(row);
		return kvi.insert(txn, key, val);
	}

	protected HashMap<String, Object> getUser(int userId) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { userId };
		String key = Tables.encodeKey(TwitterConstants.TBL_User, keys);
		String val = kvi.get(txn, key);
		return Tables.decodeTable(val);
	}

	/**
	 * Table Tweet: id->(author, data)
	 */
	protected boolean insertTweet(int tweetId, int userId, String data) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { tweetId };
		String key = Tables.encodeKey(TwitterConstants.TBL_Tweet, keys);
		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("author", userId);
		row.put("data", data);
		String val = Tables.encodeTable(row);
		return kvi.insert(txn, key, val);
	}

	protected HashMap<String, Object> getTweet(int tweetId) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { tweetId };
		String key = Tables.encodeKey(TwitterConstants.TBL_Tweet, keys);
		String val = kvi.get(txn, key);
		return Tables.decodeTable(val);
	}

	/**
	 * We record the last tweetId of each user!
	 */
	protected boolean setLastTweet(int userId, int tweetId) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { userId };
		String key = Tables.encodeKey(TwitterConstants.TBL_LastTweet, keys);
		return kvi.set(txn, key, Integer.toString(tweetId));
	}

	protected int getLastTweet(int userId) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { userId };
		String key = Tables.encodeKey(TwitterConstants.TBL_LastTweet, keys);
		String val = kvi.get(txn, key);
		return Integer.parseInt(val);
	}

	/**
	 * We simulate the twitter's schema: use two consistent tables to benefit both
	 * directions of queries. However, we only do one direction in this benchmark.
	 */
	protected boolean setFollowing(int srcId, int dstId, String time, boolean follow) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { srcId, dstId };
		String key = Tables.encodeKey(TwitterConstants.TBL_Following, keys);
		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("time", time);
		row.put("follow", follow);
		String val = Tables.encodeTable(row);
		// actually this set should be an 'upsert'
		return kvi.set(txn, key, val);
	}

	protected boolean setFollowers(int srcId, int dstId, String time, boolean follow) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { dstId, srcId };
		String key = Tables.encodeKey(TwitterConstants.TBL_Followers, keys);
		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("time", time);
		row.put("follow", follow);
		String val = Tables.encodeTable(row);
		// actually this set should be an 'upsert'
		return kvi.set(txn, key, val);
	}

	protected boolean insertFollowList(int src) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { src };
		String key = Tables.encodeKey(TwitterConstants.TBL_FollowList, keys);
		byte[] bytes = new byte[2000];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = 0;
		}
		BenchUtils.setBitMapAt(bytes, src);
		String val = Base64.getEncoder().encodeToString(bytes);
		return kvi.insert(txn, key, val);
	}

	/**
	 * Stores a bitmap to save space
	 */
	protected boolean updateFollowList(int srcId, int dstId, boolean follow) throws KvException, TxnException {
		check_txn_exists();
		int keys[] = { srcId };
		String key = Tables.encodeKey(TwitterConstants.TBL_FollowList, keys);
		String val = kvi.get(txn, key);
		assert val != null;
		// Decode to bitmap
		byte[] bytes = Base64.getDecoder().decode(val);
		if(follow) {
			BenchUtils.setBitMapAt(bytes, dstId);
		} else {
			BenchUtils.clearBitMapAt(bytes, dstId);
		}
		// no need to update
		String newVal = Base64.getEncoder().encodeToString(bytes);
		return kvi.set(txn, key, newVal);
	}

	/**
	 * Get the userIds that I follow. This is implemented by a BitMap.
	 * 
	 * @param userId The owner of the timeline.
	 * @return An ArrayList that contains all userIds.
	 * @throws KvException
	 * @throws TxnException
	 */
	protected ArrayList<Integer> getFollowList(int userId) throws KvException, TxnException {
		ArrayList<Integer> ret = new ArrayList<Integer>();
		check_txn_exists();
		int keys[] = { userId };
		String key = Tables.encodeKey(TwitterConstants.TBL_FollowList, keys);
		String val = kvi.get(txn, key);

		if (val != null) {
			byte[] bytes = Base64.getDecoder().decode(val);
			for (int i = 0; i < bytes.length * 8; i++) {
				if (BenchUtils.getBitMapAt(bytes, i)) {
					ret.add(i);
					if (ret.size() >= 20) {
						break; // only look for 20 records
					}
				}
			}
		}
		return ret;
	}

}
