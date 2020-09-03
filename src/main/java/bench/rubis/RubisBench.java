package bench.rubis;

import java.util.HashSet;
import java.util.Set;

import bench.BenchUtils;
import bench.Benchmark;
import bench.Transaction;
import bench.ZipfianGenerator;
import kv_interfaces.KvInterface;
import main.Config;

public class RubisBench extends Benchmark {
	private int nextUserId, nextItemId, nextCommentId, nextBuyId;
	private int cid_pref = Config.get().CLIENT_ID * 100000000;
	private ZipfianGenerator zipf;
	private final int totalItems = Config.get().RUBIS_USERS_NUM * RubisConstants.ItemsPerUser;

	public RubisBench(KvInterface kvi) {
		super(kvi);
		zipf = new ZipfianGenerator(Config.get().RUBIS_USERS_NUM);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Transaction[] preBenchmark() {
		int NbOfItem = Config.get().RUBIS_USERS_NUM * RubisConstants.ItemsPerUser;
		Transaction res[] = new Transaction[RubisConstants.NBOFREGIONS + Config.get().RUBIS_USERS_NUM + NbOfItem * 3];
		int curP = 0;
		for (int i = 0; i < RubisConstants.NBOFREGIONS; i++) {
			res[curP++] = new NewRegion(kvi, i);
		}

		for (int i = 0; i < Config.get().RUBIS_USERS_NUM; i++) {
			res[curP++] = new RegisterUser(kvi, i, true);
		}
		for (int i = 0; i < NbOfItem; i++) {
			int userId = i / RubisConstants.ItemsPerUser;
			res[curP++] = new RegisterItem(kvi, i, userId, true);
		}
		for (int i = 0; i < NbOfItem; i++) {
			int userId = i / RubisConstants.ItemsPerUser;
			int fromId = BenchUtils.getRandomInt(0, Config.get().RUBIS_USERS_NUM + 1);
			res[curP++] = new StoreComment(kvi, i * 2, i, fromId, userId, true);
			fromId = BenchUtils.getRandomInt(0, Config.get().RUBIS_USERS_NUM + 1);
			res[curP++] = new StoreComment(kvi, i * 2 + 1, i, fromId, userId, true);
		}
		assert curP == res.length;
		nextUserId = Config.get().RUBIS_USERS_NUM + 1;
		nextItemId = Config.get().RUBIS_USERS_NUM * RubisConstants.ItemsPerUser + 1;
		nextCommentId = nextItemId * 2;
		nextBuyId = 0;
		return res;
	}

	private int txnMix() {
		int dice = BenchUtils.getRandomInt(0, 100);
		if (dice < 10) {
			return 0;
		} else if (dice < 25) {
			return 1;
		} else if (dice < 40) {
			return 2;
		} else if (dice < 60){
			return 3;
		} else {
			return 4;
		}
	}

	@Override
	public Transaction getNextTxn() {
		int txnType = txnMix();
		final int itemId;
		switch (txnType) {
		case 0:
			return new RegisterUser(kvi, cid_pref+(nextUserId++), false);
		case 1:
			itemId = cid_pref + nextItemId;
			nextItemId++;
			return new RegisterItem(kvi, itemId, BenchUtils.getRandomInt(0, Config.get().RUBIS_USERS_NUM), false);
		case 2:
			int buyId = cid_pref+nextBuyId;
			nextBuyId++;
			itemId = BenchUtils.getRandomInt(0, totalItems);
			return new StoreBuyNow(kvi, buyId, itemId, BenchUtils.getRandomInt(1, 5),
					BenchUtils.getRandomInt(0, Config.get().RUBIS_USERS_NUM));
		case 3:
			int fromId = BenchUtils.getRandomInt(0, Config.get().RUBIS_USERS_NUM);
			int toId = zipf.nextValue().intValue();
			itemId = BenchUtils.getRandomInt(0, totalItems);
			return new StoreComment(kvi, cid_pref+(nextCommentId++), itemId, fromId, toId, false);
		case 4:
			int itemIds[] = new int[5];
			for (int i = 0; i < itemIds.length; i++) {
				itemIds[i] = BenchUtils.getRandomInt(0, totalItems);
			}
			Set<Integer> ratingIds = new HashSet<Integer>();
			while (ratingIds.size() < 3) {
				ratingIds.add(zipf.nextValue().intValue());
			}
			return new RandomBrose(kvi, itemIds, ratingIds);
		default:
			assert false;
			break;
		}
		return null;

	}

	@Override
	public void afterBenchmark() {
		// TODO Auto-generated method stub

	}

	@Override
	public String[] getTags() {
		// TODO Auto-generated method stub
		return new String[] { "RegisterItem", "RegisterUser", "StoreBuyNow", "StoreComment", };
	}

}
