package bench.tpcc;

import bench.Benchmark;
import bench.Transaction;
import bench.chengTxn.ChengTxnConstants;
import bench.tpcc.TPCCConstants.TXN_TYPE;
import kv_interfaces.KvInterface;
import main.Config;

public class TPCCBench extends Benchmark {

	public TPCCBench(KvInterface kvi) {
		super(kvi);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Transaction[] preBenchmark() {
		Transaction ret[] = new Transaction[Config.get().WAREHOUSE_NUM];
		for(int i = 0; i < Config.get().WAREHOUSE_NUM; i++) {
			Transaction load = new LoadData(kvi, i+1);
			ret[i] = load;
		}
		return ret;
	}

	@Override
	public Transaction getNextTxn() {
		Transaction txn = null;
		int w_id = Utils.RandomNumber(1, Config.get().WAREHOUSE_NUM);
		switch (seq_get()) {
		case TXN_NEWORDER:
			txn = new NewOrder(w_id, kvi);
			break;
		case TXN_PAYMENT:
			txn = new Payment(w_id, kvi);
			break;
		case TXN_ORDERSTATUS:
			txn = new OrderStatus(w_id, kvi);
			break;
		case TXN_DELIVERY:
			txn = new Delivery(w_id, kvi);
			break;
		case TXN_STOCKLEVEL:
			txn = new StockLevel(w_id, Utils.RandomNumber(1, 10), kvi);
			break;
		default:
			assert false;
			break;
		}
		return txn;
	}
	
	private TXN_TYPE seq_get(){
		int x = Utils.RandomNumber(1, 100);
		if (x <= 4) { // 4%
			return TXN_TYPE.TXN_STOCKLEVEL;
		} else if (x <= 8) { // 4%
			return TXN_TYPE.TXN_DELIVERY;
		} else if (x <= 12) { // 4%
			return TXN_TYPE.TXN_ORDERSTATUS;
		} else if (x <= 12+43) { // 43%
			return TXN_TYPE.TXN_PAYMENT;
		} else { // 45%
			assert x > 100 - 45; 
			return TXN_TYPE.TXN_NEWORDER;
		}
	}

	@Override
	public void afterBenchmark() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String[] getTags() {
		return new String[] {"TPCC-NewOrder", "TPCC-Payment", "TPCC_OrdStat", "TPCC-Delivery", "TPCC-Slevel", "inputGeneration"};
	}

}
