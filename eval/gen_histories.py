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
    config.confs['COBRA_FD'] = "/tmp/cobra/"
    config.confs['COBRA_FD_LOG'] = "/tmp/cobra/log/"
    config.confs['USE_NEW_EPOCH_TXN'] = False

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
        config.confs['TWITTER_USERS_NUM'] = 1000
    else:
        assert False, "no such workload: " + bench


def decide_experiments(bench_type):
    ks = []
    for i in range(200):
        ks.append(i*1000)

    exp1 = {
        'tpcc' : [ks[10]],
        'rubis' : [ks[10]],
        'chengRM' : [ks[10]],
        'twitter' : [ks[10]],
        'chengRW' : [100, 200, 300, 400, 500, ks[1], ks[2], ks[4], ks[6], ks[8], ks[10], ks[12], ks[14], ks[16]]
    }

    exp2 = {
        'tpcc' : [ks[100]],
        'rubis' : [ks[100]],
        'chengRM' : [ks[100]],
        'twitter' : [ks[100]],
        'chengRW' : [ks[100]]
    }

    if bench_type == "one-shot":
        return exp1
    elif bench_type == "scaling":
        return exp2
    assert False



def long_run(dst_path, exps):
    assert len(exps) == 1
    subprocess.call('mkdir -p ' + dst_path, shell=True)

    for bench in exps:
        for txn_num in exps[bench]:
            # clear database, old traces
            subprocess.call('rm -r /tmp/cobra/log; rm -r /tmp/rocksdb/', shell=True)
            # re-construct folders
            subprocess.call('mkdir -p /tmp/cobra/log; mkdir /tmp/rocksdb/', shell=True)
            # set up different config
            config = Config("../config.yaml.default")
            set_default(config)
            config.confs['MAX_FZ_TXN_NUM'] = 100 # 100*24=2.4k
            config.confs['TXN_NUM'] = txn_num
            # remote verifier
            config.confs['LOCAL_REMOTE_LOG'] = True
            config.confs['WAIT_BETWEEN_TXNS'] = 100
            config.confs['THROUGHPUT_PER_WAIT'] = 200 # 2k throughput
            config.confs['THREAD_NUM'] = 24
            config.confs['VERIFIER_HOSTNAME'] = "13.59.213.34"
            # config.confs['DEBUG_LIB_FLAG'] = True
            set_benchmark(config, bench)
            config.all_set = True # hacky way
            # dump as config
            config.dump_to()

            # run the benchmarks
            subprocess.call('java -ea -jar ../target/txnTest-1-jar-with-dependencies.jar local', shell=True)

            # save the traces
            subprocess.call('mv /tmp/cobra/log/ ' + dst_path + "/" + bench + "-" + str(txn_num), shell=True)



def gen_hist(dst_path, exps):
    # a loop of all different configs
    #size=[1000, 2000, 4000, 6000, 8000, 10000, 100000, 1000000]
    #benchmark = ['tpcc', 'chengRW', 'chengRM', 'rubis', 'twitter']

    subprocess.call('mkdir -p ' + dst_path, shell=True)

    for bench in exps:
        for txn_num in exps[bench]:
            # clear database, old traces
            subprocess.call('rm -r /tmp/cobra/log; rm -r /tmp/rocksdb/', shell=True)
            # re-construct folders
            subprocess.call('mkdir -p /tmp/cobra/log; mkdir /tmp/rocksdb/', shell=True)
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
    if len(sys.argv) != 3:
        print("Usage: gen_histories.py [one-shot|scaling|longrun] <location>")
        exit(1)
    mode = sys.argv[1]
    tpath = sys.argv[2]
    if not os.listdir(tpath) :
        print("Target %s is empty" % tpath)
    else:
        print("Target %s is not empty!" % tpath)
        exit(1)

    if mode == "longrun":
        exps = {
            #'twitter' : [100000008],
            'chengRM' : [100000007], #10M
        }
        long_run(tpath, exps)
    else:
        exps = decide_experiments(mode)
        print(exps)
        gen_hist(tpath, exps)
