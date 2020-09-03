#!/bin/python
import yaml
import subprocess
import logging

if 'logger' not in globals():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('gen_config')


def read_config(fname):
    with open(fname) as f:
        return yaml.safe_load(f);


def write_config(config, fname):
    with open(fname, 'w') as f:
        yaml.safe_dump(config, f, default_flow_style=False);


def run_bench(fname="config.yaml"):
    my_cmd = 'java -ea -jar target/txnTest-1-jar-with-dependencies.jar local %s' % (fname)
    print(my_cmd)
    subprocess.call(my_cmd, shell=True);
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")


class Config(object):
    """config.yaml"""

    def __init__(self, filename):
        super(Config, self).__init__()
        self.confs = read_config(filename)
        self.all_set = False

    def set_db(self, database):
        if database == "rocksdb":
            self.confs['ENABLE_BARRIER'] = False
            self.confs['LIB_TYPE'] = 2
            self.confs['COBRA_FD'] = '/tmp/cobra'
            self.confs['COBRA_FD_LOG'] = '/tmp/cobra/log'
        elif database == "postgres":
            self.confs['ENABLE_BARRIER'] = True
            self.confs['LIB_TYPE'] = 3
            self.confs['PG_PASSWORD'] = 'Cobra<318'
            self.confs['PG_USERNAME'] = 'cobra'
            # self.confs['DB_URL'] = 'jdbc:postgresql://192.168.1.176:5432/testdb'
        elif database == "google":
            self.confs['ENABLE_BARRIER'] = True
            self.confs['LIB_TYPE'] = 1
        else:
            assert False, "no such db: " + database
        return self

    def set_workload(self, workload, contention):

        if workload == "cheng":
            self.confs['BENCH_TYPE'] = 0
            self.confs['OP_PER_CHENGTXN'] = 8
        elif workload == "tpcc":
            self.confs['BENCH_TYPE'] = 1
            self.confs['REPORT_NEWORDER_ONLY'] = False
        elif workload == "ycsb":
            self.confs['BENCH_TYPE'] = 2
        elif workload == "rubis":
            self.confs['BENCH_TYPE'] = 3
        elif workload == "twitter":
            self.confs['BENCH_TYPE'] = 4
        else:
            assert False, "no such workload: " + workload

        if contention == "high":
            self.confs['NUM_KEYS'] = 1000
            self.confs['WAREHOUSE_NUM'] = 8
            self.confs['RUBIS_USERS_NUM'] = 1000
            self.confs['TWITTER_USERS_NUM'] = 1000
        elif contention == "low":
            self.confs['NUM_KEYS'] = 100000
            self.confs['WAREHOUSE_NUM'] = 64
            self.confs['RUBIS_USERS_NUM'] = 20000
            self.confs['TWITTER_USERS_NUM'] = 16000
        else:
            assert False, "no such contention: " + contention
        return self

    def set_instrument(self, level):
        if level == "no":
            self.confs['USE_INSTRUMENT'] = False
        elif level == "encode":
            self.confs['USE_INSTRUMENT'] = True
            self.confs['CLOUD_LOG'] = False
            self.confs['MAX_FZ_TXN_NUM'] = 10000000
        elif level == "cloudnofz" or level == "cloudnovnofz":
            self.confs['USE_INSTRUMENT'] = True
            self.confs['CLOUD_LOG'] = True
            self.confs['MAX_FZ_TXN_NUM'] = 10000000
            self.confs['NUM_TXN_IN_ENTITY'] = 1
        elif level == "local":
            self.confs['LOCAL_LOG'] = True
            self.confs['USE_INSTRUMENT'] = True
            self.confs['CLOUD_LOG'] = False
            self.confs['MAX_FZ_TXN_NUM'] = 20
            self.confs['NUM_TXN_IN_ENTITY'] = 1
        elif level == "cloud":
            self.confs['USE_INSTRUMENT'] = True
            self.confs['CLOUD_LOG'] = True
            self.confs['MAX_FZ_TXN_NUM'] = 20
            self.confs['NUM_TXN_IN_ENTITY'] = 1
        elif level == "couldsign":
            self.confs['USE_INSTRUMENT'] = True
            self.confs['CLOUD_LOG'] = True
            self.confs['MAX_FZ_TXN_NUM'] = 20
            self.confs['NUM_TXN_IN_ENTITY'] = 1
            self.confs['SIGN_DATA'] = True
        elif level == "ww":
            self.confs['USE_INSTRUMENT'] = True
            self.confs['CLOUD_LOG'] = True
            self.confs['MAX_FZ_TXN_NUM'] = 10000000
            self.confs['NUM_TXN_IN_ENTITY'] = 1
            self.confs['SAMPLE_WRITE_PERCENT'] = 100
            self.confs['LAZY_WW_TRACKING'] = True
        else:
            assert False, "no such level: " + level
        return self

    def set_bench(self, thread, txn_num):
        self.confs['THREAD_NUM'] = thread
        self.confs['TXN_NUM'] = txn_num
        return self

    def set_client_id(self, client_id):
        self.confs['CLIENT_ID'] = client_id
        if client_id == 1:
            self.confs['SKIP_LOADING'] = False
        else:
            self.confs['SKIP_LOADING'] = True

    def set_result_file(self, rfname):
        self.confs['RESULT_FILE_NAME'] = rfname

    def set_latency_folder(self, lfname):
        self.confs['LATENCY_FOLDER'] = lfname

    def set_all(self, clientID, database, workload, contention, inst_level, thread_num, txn_num):
        self.set_db(database)
        self.set_workload(workload, contention)
        self.set_instrument(inst_level)
        self.set_bench(thread_num, txn_num)
        self.set_client_id(clientID)
        self.set_latency_folder("/tmp/cobra/lats/")

        # override the previous setting in set_client_id
        if workload == 'tpcc' and database != 'rocksdb':
            self.confs['SKIP_LOADING'] = True

        trial_name = "{}-{}-{}-{}-{}-{}-{}".format(
            clientID, database, workload, contention, inst_level, thread_num, txn_num)
        self.set_result_file('/tmp/cobra/results/' + trial_name + '.log')

        self.all_set = True
        return trial_name

    def dump_to(self, fname="config.yaml"):
        assert 'LIB_TYPE' in self.confs, "didn't set database"
        assert 'USE_INSTRUMENT' in self.confs, "didn't set instrument"
        assert 'BENCH_TYPE' in self.confs, "didn't set workload"
        assert self.all_set, "you should call set_all() to build config"

        write_config(self.confs, fname)
        logger.info("Dumped config to " + fname)


def main():
    subprocess.call('mkdir -p results', shell=True)

    config = Config("config.yaml.default")
    config.set_all(1, 'postgres', 'tpcc', 'high', 'cloud', 8, 10000)
    config.dump_to();


if __name__ == "__main__":
    main()
