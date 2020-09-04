import pandas
import numpy as np
import math
import os
import sys
import re

from utils import *

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
percentiles = [ 10, 25, 50, 75, 90, 95, 99, 99.9 ]
DATA_FOLDER = DIR_PATH + '/data'

def getResult(trial_string, thread, client_num=2):
    print("thread: {}".format(thread))
    datas = []
    end_times = []

    for i in range(1, client_num+1):
        first_start_time = math.inf
        last_start_time = 0
        first_end_time = math.inf
        last_end_time = 0

        trial_name = DIR_PATH + '/./trials/{}-{}-{}'.format(i, trial_string, thread)
        lats_folder = trial_name + '/cobra/lats'
        if not os.path.exists(lats_folder):
            continue
        files = os.listdir(lats_folder)
        for fname in files:
            fpath = lats_folder + '/' + fname
            data = pandas.read_csv(fpath, sep=' ').values

            start_time = np.min(data[:, 0])
            end_time = np.max(data[:, 1])
            first_start_time = min(first_start_time, start_time)
            last_start_time = max(last_start_time, start_time)
            first_end_time = min(first_end_time, end_time)
            last_end_time = max(last_end_time, end_time)

            end_times.append(first_end_time - first_start_time)


            data -= first_start_time
            datas.append(data)
        print("{}: start time gap: {}, end time gap: {}".format(i, (last_start_time - first_start_time) / 1e9,
                                                            (last_end_time - first_end_time) / 1e9))

    print("total end time gap of all clients: {}s".format((max(end_times)-min(end_times))/1e9))
    count_start = 0
    count_end = min(end_times) - 0

    count_time = count_end - count_start

    print("total time: {}s".format((last_end_time - first_start_time)/1e9))
    print("counted time: {}s".format(count_time/1e9))

    res = []
    res.append(thread)

    lats = []
    before_trimming = 0
    for data in datas:
        before_trimming += data.shape[0]
        data = data[np.where(data[:,1] > count_start)]
        data = data[np.where(data[:,1] < count_end)]
        lats += (data[:,1]-data[:,0]).tolist()
    print("Data size before trimming: {}, after trimming: {}".format(before_trimming, len(lats)))

    tps = len(lats)/count_time*1e9
    res.append(tps)
    print('TPS: {}'.format(tps))

    lats = np.array(lats)
    lats.sort()
    print('Latencies:')
    for per in percentiles:
        latency_value = np.percentile(lats, per)/1e6
        print('{}%(ms) : {}'.format(per, latency_value))
        res.append(latency_value)

    # plt.hist(lats[:-int(0.001*len(lats))], bins="auto")
    # plt.show()

    return res

def get_report(trial_string, client_num):
    thread_tps_lats = []

    threads = {}
    dir_names = os.listdir('trials')
    for s in dir_names:
        if '-'+trial_string+'-' in s:
            threads[int(s.split('-')[-1])] = True
    if len(threads.keys()) == 0:
        return

    for thread in sorted(threads.keys()):
        res = getResult(trial_string, thread, client_num)
        thread_tps_lats.append(res)

    df = pandas.DataFrame(thread_tps_lats)
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    fname = DATA_FOLDER + '/{}.data'.format(trial_string)

    df.to_csv(fname, sep=' ', header=['#thread', 'tps']+percentiles, index=False, float_format="%.5f")
    printG("FINISHED: " + trial_string)

def get_network_old(fname):
    net_thpt_rx = []
    net_thpt_tx = []
    with open(fname) as f:
        for sline in f:
            line = sline.split()
            net_thpt_tx.append(float(line[1]))
            net_thpt_rx.append(float(line[2]))

    net_thpt_rx = np.array(net_thpt_rx)
    net_thpt_tx = np.array(net_thpt_tx)

    net_thpt_rx.sort()
    net_thpt_tx.sort()

    # print('receive peak: {}, send peak: {}'.format(net_thpt_rx[-1], net_thpt_tx[-1]))
    top10p = int(len(net_thpt_rx) *100 / 30)
    avg_rx = net_thpt_rx[-top10p: -1].mean()
    avg_tx = net_thpt_tx[-top10p: -1].mean()
    # print('avg of top 10% rx: {}'.format(avg_rx))
    # print('avg of top 10% tx: {}'.format(avg_tx))

    return avg_rx, avg_tx

def get_num_op(trial_string):
    threads = {}
    dir_names = os.listdir('trials')
    for s in dir_names:
        if trial_string in s:
            threads[int(s.split('-')[-1])] = True
    if len(threads.keys()) == 0:
        printB('not found: ' + trial_string)
        return

    thread = 24
    trial_name = DIR_PATH + '/./trials/{}-{}-{}/client.txt'.format(1, trial_string, thread)
    result = ''
    with open(trial_name) as f:
        for line in f:
            if re.search(r'NumOp: [0-9]+', line):
                result = line
                break
    result = result.split()[1]
    return result


def get_network(fname):
    lines = []
    with open(fname) as f:
        for sline in f:
            line = sline.split()
            lines.append(line)
    rx = int(lines[2][4]) - int(lines[0][4])
    tx = int(lines[3][4]) - int(lines[1][4])
    return (rx, tx)

def get_trace_size(trial_string):
    threads = {}
    dir_names = os.listdir('trials')
    for s in dir_names:
        if trial_string in s:
            threads[int(s.split('-')[-1])] = True
    if len(threads.keys()) == 0:
        printB('not found: ' + trial_string)
        return

    thread = 24
    trial_name = DIR_PATH + '/./trials/{}-{}-{}/client.txt'.format(1, trial_string, thread)
    result = ''
    with open(trial_name) as f:
        for line in f:
            if re.search(r'SizeOfTrace: [0-9]+', line):
                result = line
                break
    result = result.split()[1]
    return result


def main():
    if len(sys.argv) == 1:
        databases = ['rocksdb', 'postgres', 'google']
        workload = 'cheng'
        inst_level = 'cloud'
        for database in databases:
            for contention in ['low', 'high']:
                for workload in ['cheng', 'tpcc', 'twitter', 'ycsb', 'rubis']:
                    for inst_level in ['no', 'ww', 'cloud', 'cloudnovnofz', 'cloudnofz', 'local']:
                        trial_string = '{}-{}-{}-{}'.format(database, workload, contention, inst_level)
                        get_report(trial_string, 10 if database == 'postgres' else 1)

    elif sys.argv[1] == 'net':
        database = 'postgres'
        workloads = ['cheng', 'ycsb', 'twitter', 'rubis', 'tpcc']
        inst_levels = ['no', 'local']

        result_str = 'workload ' + ' '.join(inst_levels) + '\n'

        for contention in ['low']:
            for workload in workloads:
                result_row = workload
                for inst_level in inst_levels:
                    trial_string = '{}-{}-{}-{}'.format(database, workload, contention, inst_level)
                    thread = 24
                    rx, tx = get_network('netstats/netstats-'+trial_string + '-{}.log'.format(thread))
                    print('{}-{}: {}, {}'.format(workload, inst_level, rx, tx))
                    result_row += ' {}'.format(tx)
                result_str += result_row + '\n'

        print(result_str)
        return

    elif sys.argv[1] == 'numop':
        inst_levels = ['no', 'cloud', 'ww']
        result_str = 'workload ' + ' '.join(inst_levels) + '\n'
        for contention in ['low']:
            for workload in ['cheng', 'tpcc', 'twitter', 'ycsb', 'rubis']:
                result_str += workload
                for inst_level in inst_levels:
                    trial_string = '{}-{}-{}-{}'.format(database, workload, contention, inst_level)
                    numop = get_num_op(trial_string)
                    result_str += ' {}'.format(numop)
                result_str += '\n'
        print(result_str)

    elif sys.argv[1] == 'tracesize':
        database = 'rocksdb'
        inst_levels = ['cloud', 'ww']
        result_str = 'workload ' + ' '.join(inst_levels) + '\n'
        for contention in ['low']:
            for workload in ['cheng', 'ycsb', 'twitter', 'rubis', 'tpcc']:
                result_str += workload
                for inst_level in inst_levels:
                    trial_string = '{}-{}-{}-{}'.format(database, workload, contention, inst_level)
                    numop = get_trace_size(trial_string)
                    result_str += ' {}'.format(numop)
                result_str += '\n'
        print(result_str)


if __name__ == "__main__":
    main()
