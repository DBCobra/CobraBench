#!/bin/python
import yaml
import subprocess
import logging
import fabric
import sys
import redis
import time
import datetime

import fabfile as fab
from utils import *
from gen_config import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('main')
redis_ip = '192.168.1.176'
db_host = 'localhost'
nic_device = ''
client_machine = ['localhost']

def get_bench_num(workload):
    if workload == "cheng":
        return "0"
    elif workload == "tpcc":
        return "1"
    elif workload == "ycsb":
        return "2"
    elif workload == "rubis":
        return "3"
    elif workload == "twitter":
        return "4"
    else:
        assert False, "no such workload: " + workload

def workload_table(workload):
    if workload == "cheng":
        return "chengTxn"
    elif workload == "tpcc":
        return "tpcc"
    elif workload == "ycsb":
        return "ycsb"
    elif workload == "rubis":
        return "rubis"
    elif workload == "twitter":
        return "twitter"
    else:
        assert False, "no such workload: " + workload


def reload_db(database, workload, contention):
    logger.info("reseting db[{}] for {}-{}".format(database, workload, contention))
    table = workload_table(workload)

    if database == "rocksdb":
        subprocess.run('rm -rf /tmp/rocksdb || true', shell=True)
    elif database == "postgres":
        if workload == "tpcc":
            suffix = '8w' if contention == "high" else '64w'
            db_backup = workload + suffix + ".dump"
            fab._reset_database(db_host, db_backup)
        else:
            fab._clear_database(db_host, table)
    elif database == "google":
        pass
    else:
        assert False, "no such db: " + database


def delete_traces(database, workload):
    table = workload_table(workload)

    if database == "postgres":
        fab.delete_traces(db_host, table)
    elif database == "google":
        pass
    else:
        assert False, 'not implemented'


def run_one_trial(database, workload, contention, inst_level, thread, txn_num):
    if database == 'google':
        return run_one_google_trial(workload, contention, inst_level, thread, txn_num)
    elif database == 'rocksdb':
        return run_one_rocksdb_trial(workload, contention, inst_level, thread, txn_num)

    trial_name = '{}-{}-{}-{}-{}'.format(database, workload, contention, inst_level, thread)
    printB("[{}] Starting {}".format(datetime.datetime.now(), trial_name))

    # set barrier
    r = redis.Redis(host=redis_ip, port=6379, db=0)
    r.set("cobra_clients", 0)

    # clear workspace
    clients = fabric.ThreadingGroup(*client_machine)
    for c in clients:
        fab._stop_all(c)

    trial_names = []

    # prepare for config.yaml
    for i in range(1, len(client_machine) + 1):
        config = Config("config.yaml.default")
        config.set_all(i, database, workload, contention, inst_level, thread, txn_num)
        config.dump_to("conf{}.yaml".format(i));
        trial_names.append("{}-{}".format(i, trial_name))

    if workload == 'tpcc':
        # clear traces in database (we don't reload database because it takes a long time)
        delete_traces(database, workload)
    else:
        reload_db(database, workload, contention)

    # start the benchmark
    fab.restart_l(clients)

    # wait for clients and release the barrier
    printG("Waiting for loading to release barrier")
    while int(r.get("cobra_clients")) < len(client_machine):
        time.sleep(1)
    r.set("cobra_clients", "start")

    # start monitoring traffic
    monitor_net = (database == 'postgres')
    if monitor_net:
        fab._monitor_network(db_host, 'netstats/netstats-{}.log'.format(trial_name), nic_device)

    # wait for warming up
    time.sleep(5)

    # start verifier
    # if inst_level != 'no' and inst_level != 'cloudnovnofz':
    #     fab._restart_v(verifier_machine, get_bench_num(workload))


    printG("Waiting for the end of the trial")
    subprocess.run('mkdir -p eval/trials || true', shell=True)
    # wait for end & clean up
    for i in range(len(clients)):
        fab.wait_for_bench_finish(clients[i])
        fab.mv_cobra_tmp(clients[i], "trials", trial_names[i])
        subprocess.run('scp -r {}:~/trials/{} eval/trials/ || true'.format(client_machine[i], trial_names[i]), shell=True)

    if monitor_net:
        fab._stop_monitor(db_host, 'netstats/netstats-{}.log'.format(trial_name), nic_device)

    r.delete("cobra_clients")
    for c in clients:
        fab._stop_all(c)


def run_one_series(database, workload, contention, inst_level):
    reload_db(database, workload, contention)
    threads = [128, 96, 64, 1, 2, 4, 8, 12, 16, 24, 32, 48]
    if database == 'google':
        threads = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    elif database == 'rocksdb':
        threads = [1, 2, 3, 4, 6, 8, 10, 12, 14, 16, 24]
    #for thread in range(1, 17):
    #for thread in [128, 96, 48, 64]:
    for thread in threads:
        txn_num = int((80000 - 2000) / (32 - 1) * (thread - 1) + 2000)
        if thread >= 48:
            txn_num = 160000

        if database == 'google':
            txn_num = thread * 200
            if thread > 64:
                txn_num = 64*200
        elif database == 'rocksdb':
            txn_num = int((800000 - 100000) / (32 - 1) * (thread - 1) + 100000)

        if workload != 'tpcc':
            txn_num *= 3

        run_one_trial(database, workload, contention, inst_level, thread, txn_num)
        #run_locally(database, workload, contention, inst_level, thread, txn_num)

def run_locally(database, workload, contention, inst_level, thread, txn_num):
    if database == 'rocksdb':
        subprocess.run('rm -rf /tmp/rocksdb || true', shell=True)
    subprocess.run('rm -rf /tmp/cobra', shell=True)

    config = Config("config.yaml.default")
    config.set_all(1, database, workload, contention, inst_level, thread, txn_num)
    config.dump_to("conf_local.yaml");
    subprocess.run('java -ea -jar target/txnTest-1-jar-with-dependencies.jar local conf_local.yaml', shell=True)

def run_one_google_trial(workload, contention, inst_level, thread, txn_num):
    database = 'google'

    trial_name = '{}-{}-{}-{}-{}'.format(database, workload, contention, inst_level, thread)
    printB("[{}] Starting {}".format(datetime.datetime.now(), trial_name))

    # set barrier
    r = redis.Redis(host=redis_ip, port=6379, db=0)
    r.set("cobra_clients", 0)

    # clear workspace
    client_machine = ['localhost']
    clients = fabric.ThreadingGroup(*client_machine)

    trial_names = []

    # prepare for config.yaml
    for i in range(1, len(client_machine) + 1):
        config = Config("config.yaml.default")
        config.set_all(i, database, workload, contention, inst_level, thread, txn_num)
        config.dump_to("conf{}.yaml".format(i))
        trial_names.append("{}-{}".format(i, trial_name))

    # start the benchmark
    fab.restart_l(clients)

    # wait for clients and release the barrier
    printG("Waiting for loading to release barrier")
    while int(r.get("cobra_clients")) < len(client_machine):
        time.sleep(1)
    r.set("cobra_clients", "start")

    printG("Waiting for the end of the trial")
    # wait for end & clean up
    for i in range(len(clients)):
        fab.wait_for_bench_finish(clients[i])
        fab.mv_cobra_tmp(clients[i], "trials", trial_names[i])

    r.delete("cobra_clients")

def run_one_rocksdb_trial(workload, contention, inst_level, thread, txn_num):
    database = 'rocksdb'

    trial_name = '{}-{}-{}-{}-{}'.format(database, workload, contention, inst_level, thread)
    printB("[{}] Starting {}".format(datetime.datetime.now(), trial_name))

    client_machine = ['localhost']
    clients = fabric.ThreadingGroup(*client_machine)

    trial_names = []

    # prepare for config.yaml
    for i in range(1, len(client_machine) + 1):
        config = Config("config.yaml.default")
        config.set_all(i, database, workload, contention, inst_level, thread, txn_num)
        config.dump_to("conf{}.yaml".format(i));
        trial_names.append("{}-{}".format(i, trial_name))

    if workload == 'tpcc':
        # clear traces in database (we don't reload database because it takes a long time)
        delete_traces(database, workload)
    else:
        reload_db(database, workload, contention)

    # start the benchmark
    fab.restart_l(clients)

    printG("Waiting for the end of the trial")
    # wait for end & clean up
    for i in range(len(clients)):
        fab.wait_for_bench_finish(clients[i])
        fab.mv_cobra_tmp(clients[i], "trials", trial_names[i])

def main(mvninstall):
    if mvninstall:
        subprocess.run('mvn install', shell=True)

    # for database in ['rocksdb', 'postgres']:
    # for workload in ['cheng', 'twitter', 'rubis', 'tpcc', 'ycsb']:
    # for inst_level in ['no', 'local']:
    workload = 'twitter'
    database = 'rocksdb'
    inst_level = 'no'
    run_one_series(database, workload, 'low', inst_level)

if __name__ == '__main__':
    mvninstall = len(sys.argv) == 2
    main(mvninstall)
