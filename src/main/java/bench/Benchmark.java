package bench;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;

import kv_interfaces.CockroachDB;
import kv_interfaces.GoogleDataStore;
import kv_interfaces.InstKV;
import kv_interfaces.KvInterface;
import kv_interfaces.RocksDBKV;
import kv_interfaces.SqlKV;
import kv_interfaces.YugaByteDB;
import main.Config;

public abstract class Benchmark {
	protected KvInterface kvi;
	private static String googleKind = null;

	public static KvInterface getKvi(Config.LibType libtype, boolean useInstrument) {
		KvInterface kvi = null;
		if (libtype == Config.LibType.ROCKSDB_ORIG_LIB) {
			kvi = RocksDBKV.getInstance();
		} else if (libtype == Config.LibType.CHENG_ORIG_LIB) {
			assert false;
		} else if (libtype == Config.LibType.GOOGLE_DATASTORE_LIB) {
			// use a random kind to make sure we launch a new kind every time
			// because google limits the operation rate and makes it slow start
			if (googleKind == null) {
				synchronized (Benchmark.class) {
					if (googleKind == null) {
						googleKind = Config.getTableName(Config.get().BENCH_TYPE) + "-" + BenchUtils.getRandomValue(8);
					}
				}
			}
			Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
			kvi = new GoogleDataStore(datastore, googleKind);
		} else if (libtype == Config.LibType.RPC_CLIENT_LIB) {
			assert false;
			//kvi = RpcClient.getInstance();
		} else if (libtype == Config.LibType.POSTGRESQL_LIB) {
			String tableName = Config.getTableName(Config.get().BENCH_TYPE);
			kvi = new SqlKV(tableName);
		} else if (libtype == Config.LibType.COCKROACH_LIB) {
			kvi =  CockroachDB.getInstance();
		} else if (libtype == Config.LibType.YUGABYTE_LIB) {
			kvi =  YugaByteDB.getInstance();
		}else {
			// should not be here
			assert false;
		}

		if(useInstrument) {
			return new InstKV(kvi);
		} else {
			return kvi;
		}
	}

	public Benchmark(KvInterface kvi) {
		this.kvi = kvi;
	}

	// things to do before running benchmark: initialize the database / generate something / ...
	// TODO: is returning type Transaction suitable?
	public abstract Transaction[] preBenchmark();

	// feed the planner
	public abstract Transaction getNextTxn();

	// clean-up after benchmark
	public abstract void afterBenchmark();

	public abstract String[] getTags();
}
