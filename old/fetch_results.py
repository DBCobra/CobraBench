import sys,os,re

def decodeBench(bench_str):
    args = bench_str.strip().split()
    assert len(args) == 7
    # local nosign none offline 1000 10 RO
    return {
        "id" : bench_str,
        "log" : args[0],
        "sign" : args[1],
        "ww" : args[2],
        "verifier" : args[3],
        "sleep_time" : args[4],
        "num_txn_in_entity" : args[5],
        "workload" : args[6]}



def fetchResults(log):
    if not os.path.exists(log):
        return []

    with open(log, 'r') as f:
        content = f.read()
    f.close()

    lines = content.splitlines()

    ret = {}
    cur_bench = {}
    for line in lines:
        # fetch the benchmark
        if line.strip().startswith("****"):
            m = re.search("\*\*\*\* (.+?) \*\*\*\*", line)
            assert m
            if len(cur_bench) > 0:
                # there should not be duplicated bench
                assert cur_bench["bench"]["id"] not in ret
                id = cur_bench["bench"]["id"]
                ret[id] = cur_bench
            # new a bench
            cur_bench = {"bench" : decodeBench(m.group(1))}
        # fetch the success%
        if line.strip().startswith("Online verifier"):
            m = re.search("success%=(.+?)%", line)
            assert m
            assert "succ" not in cur_bench
            cur_bench["succ"] = float(m.group(1))
        # fetch the throughput
        if line.strip().startswith("Throughput:"):
            m = re.search("Throughput: (.+?) txn/sec", line)
            assert m
            assert "throughput" not in cur_bench
            cur_bench["throughput"] = float(m.group(1))
    # commit the last bench
    if len(cur_bench) > 0:
        # there should not be duplicated bench
        assert cur_bench["bench"]["id"] not in ret
        id = cur_bench["bench"]["id"]
        ret[id] = cur_bench

    return ret


def usage():
    print("Usage: script <result_log>")

def main(argv):
    if len(argv) != 2:
        usage()
        exit(1)

    log = argv[1]
    results = fetchResults(log)
    # baseline = "local nosign none offline 1000 10"
    baseline_tmp = "local nosign none offline 1000 10 "
    cobra_tmp = "cloud sign lazy online 1000 10 "
    naive_tmp = "cloud sign naive online 1000 10 "

    print("%-8s %-8s : %8s %8s => %-8s %-8s  [%s] " %
          ("SYS", "WKLD", "TPUT", "SUCC%", "OVHD", "SPDUP", "ARGS")
          )
    for key in results:
        bench = key
        baseline = baseline_tmp + results[bench]["bench"]["workload"]
        cobra = cobra_tmp + results[bench]["bench"]["workload"]
        naive = naive_tmp + results[bench]["bench"]["workload"]
        # calculate slowdown, speedup
        if bench == baseline:
            slowdown = "--"
            speedup = "--"
            sys = "baseline"
        else:
            base_tput = results[baseline]["throughput"]
            my_tput = results[bench]["throughput"]
            slowdown = '%.2f' % ((base_tput - my_tput) / base_tput)
            if bench == cobra:
                if naive in results:
                    naive_tput = results[naive]["throughput"]
                    speedup = '%.2f' % ((my_tput - naive_tput) / naive_tput)
                else:
                    speedup = "--"
                sys = "cobra"
            else:
                speedup = "--"
                sys = "naive"

        print("%-8s %-8s : %8.2f %8.2f => %-8s %-8s  [%s] " %
              (sys, results[bench]["bench"]["workload"],
               results[bench]["throughput"], results[bench]["succ"], slowdown, speedup,
               bench))

if __name__ == "__main__":
    main(sys.argv)
