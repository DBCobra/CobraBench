#!/usr/bin/python

import subprocess
import sys
import os
from gen_config import Config



def set_default(config):
    config.set_db('rocksdb')
    config.confs['THREAD_NUM'] = 24
    config.confs['MAX_FZ_TXN_NUM'] = 20
    config.confs['LOCAL_LOG'] = True
    config.confs['CLOUD_LOG'] = False
    config.confs['NUM_KEYS'] = 1000
    config.confs['COBRA_FD'] = "/tmp/cobra/"
    config.confs['COBRA_FD_LOG'] = "/tmp/cobra/log/"

def set_benchmark(config, bench):
    config.confs['SKIP_LOADING'] = False

    if bench == "chengRW":
        config.confs['BENCH_TYPE'] = 0
        config.confs['OP_PER_CHENGTXN'] = 8
        config.confs['RATIO_READ'] = 50
        config.confs['RATIO_UPDATE'] = 50
    elif bench == "chengRM":
        config.confs['BENCH_TYPE'] = 0
        config.confs['OP_PER_CHENGTXN'] = 8
        config.confs['RATIO_READ'] = 90
        config.confs['RATIO_UPDATE'] = 10
    elif bench == "tpcc":
        config.confs['BENCH_TYPE'] = 1
        #config.confs['SKIP_LOADING'] = True
    elif bench == "ycsb":
        config.confs['BENCH_TYPE'] = 2
    elif bench == "rubis":
        config.confs['BENCH_TYPE'] = 3
    elif bench == "twitter":
        config.confs['BENCH_TYPE'] = 4
    else:
        assert False, "no such workload: " + bench


def main(dst_path):
    # a loop of all different configs
    size=[10000]
    benchmark = ['chengRW', 'chengRM', 'rubis', 'twitter']

    subprocess.call('mkdir -p ' + dst_path, shell=True)

    for txn_num in size:
        for bench in benchmark:
            # clear database, old traces
            subprocess.call('rm -r /tmp/cobra/log; rm -r /tmp/rocksdb/', shell=True)
            # set up different config
            config = Config("../config.yaml.default")
            set_default(config)
            config.confs['TXN_NUM'] = txn_num
            set_benchmark(config, bench)
            config.all_set = True # hacky way
            # dump as config
            config.dump_to()

            # run the benchmarks
            subprocess.call('java -ea -jar ../target/txnTest-1-jar-with-dependencies.jar local', shell=True)

            # save the traces
            subprocess.call('mv /tmp/cobra/log/ ' + dst_path + "/" + bench + "-" + str(txn_num), shell=True)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: gen_locl_traces.py <location>")
        exit(1)
    tpath = sys.argv[1]
    if not os.listdir(tpath) :
        print("Target %s is empty" % tpath)
    else:
        print("Target %s is not empty!" % tpath)
        exit(1)
    main(sys.argv[1])
