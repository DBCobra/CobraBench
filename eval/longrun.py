#!/usr/bin/python

from fabric import Connection
import subprocess
import sys
import os
from gen_config import Config
import fabfile as fab

# inputs
username = 'ubuntu'
db_host = 'ec2-18-221-101-46.us-east-2.compute.amazonaws.com'
veri_ip = '18.221.245.69'

def set_default(config):
    config.set_db('postgres')
    config.confs['DB_URL'] = 'jdbc:postgresql://' + db_host + ':5432/testdb'
    config.confs['ENABLE_BARRIER'] = False # NOTE: we don't need Redis for this bench
    config.confs['LOCAL_LOG'] = True
    config.confs['CLOUD_LOG'] = False
    config.confs['COBRA_FD'] = "/tmp/cobra/"
    config.confs['COBRA_FD_LOG'] = "/tmp/cobra/log/"



def set_benchmark(config, bench):
    config.confs['SKIP_LOADING'] = False

    if bench == "chengRW":
        config.confs['BENCH_TYPE'] = 0
        config.confs['NUM_KEYS'] = 10000
        config.confs['OP_PER_CHENGTXN'] = 8
        config.confs['RATIO_READ'] = 50
        config.confs['RATIO_UPDATE'] = 50
    elif bench == "chengRM":
        config.confs['BENCH_TYPE'] = 0
        config.confs['NUM_KEYS'] = 10000
        config.confs['OP_PER_CHENGTXN'] = 8
        config.confs['RATIO_READ'] = 90
        config.confs['RATIO_UPDATE'] = 10
    elif bench == "tpcc":
        config.confs['BENCH_TYPE'] = 1
        #config.confs['SKIP_LOADING'] = True
    elif bench == "rubis":
        config.confs['BENCH_TYPE'] = 3
        config.confs['RUBIS_USERS_NUM'] = 20000
    elif bench == "twitter":
        config.confs['BENCH_TYPE'] = 4
        config.confs['TWITTER_USERS_NUM'] = 16000
    else:
        assert False, "no such workload: " + bench

def bench_table(bench):
    if bench == "chengRM" or bench == "chengRW":
        return "chengTxn"
    elif bench == "tpcc":
        return "tpcc"
    elif bench == "ycsb":
        return "ycsb"
    elif bench == "rubis":
        return "rubis"
    elif bench == "twitter":
        return "twitter"
    else:
        assert False, "no such benchmark: " + bench

def reload_db(bench):
    table = bench_table(bench)
    host = Connection(db_host, user=username)
    fab.clear_database(host, table)

def long_run(exps):
    assert len(exps) == 1
    subprocess.call('mkdir -p /tmp/cobra/log', shell=True)

    for bench in exps:
        reload_db(bench)
        for txn_num in exps[bench]:
            # set up different config
            config = Config("../config.yaml.default")
            set_default(config)
            config.confs['MAX_FZ_TXN_NUM'] = 20 # 100*24=2.4k
            config.confs['TXN_NUM'] = txn_num
            # remote verifier
            config.confs['LOCAL_REMOTE_LOG'] = True  # remote log
            config.confs['WAIT_BETWEEN_TXNS'] = 0 #100
            config.confs['THROUGHPUT_PER_WAIT'] = 200 # 2k throughput
            config.confs['THREAD_NUM'] = 24
            config.confs['VERIFIER_HOSTNAME'] = veri_ip
            # config.confs['DEBUG_LIB_FLAG'] = True
            set_benchmark(config, bench)
            config.all_set = True # hacky way
            # dump as config
            config.dump_to()

            # run the benchmarks
            subprocess.call('java -ea -jar ../target/txnTest-1-jar-with-dependencies.jar local', shell=True)



if __name__ == "__main__":
    exps = {
        #'twitter' : [100000008],
        'chengRM' : [100000007], #10M
    }
    long_run(exps)
