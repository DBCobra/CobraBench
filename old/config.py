import sys, os

# parameter vector:
#   1: [local|cloud]
#   2: [sign|nosign]
#   3: [none|naive|lazy]
#   4: [online|offline]
#   5: (0-N)  // sleep period
#   6: (1-N)  // #txn per entity
#   7: [RO|WO|RMW|50-50|90-10]

symbols = {
    # lib
    "clog" : "public static final boolean LOCAL_LOG =", #1
    "sign" : "public static final boolean SIGN_DATA =", #2
    "ww_lazy" : "public static final boolean LAZY_WW_TRACKING =", #3
    "ww_sample" : "public static final int SAMPLE_WRITE_PERCENT =", #3
    "num_txn_entity" : "public static final int NUM_TXN_IN_ENTITY =",   #6
    # verifier
    "verifier_sleep" : "public static final long SLEEP_TIME_PER_ROUND =", #5
    # benchmark
    "read_ratio" : "public final static int RATIO_READ =",   # 7
    "write_ratio" : "public final static int RATIO_UPDATE =", #7
    "rmw_ratio" : "public final static int RATIO_RMW =", #7
}



def UpdateLibConf(lib_file, is_local_log, is_sign, is_lazy, sample_write_percent, num_txn_in_entity):
    repline = symbols["clog"] + str(is_local_log).lower() + ";"
    assert ReplaceLineInFile(lib_file, symbols["clog"], repline)
    repline = symbols["sign"] + str(is_sign).lower() + ";"
    assert ReplaceLineInFile(lib_file, symbols["sign"], repline)
    repline = symbols["ww_lazy"] + str(is_lazy).lower() + ";"
    assert ReplaceLineInFile(lib_file, symbols["ww_lazy"], repline)
    repline = symbols["ww_sample"] + str(sample_write_percent) + ";"
    assert ReplaceLineInFile(lib_file, symbols["ww_sample"], repline)
    repline = symbols["num_txn_entity"] + str(num_txn_in_entity) + ";"
    assert ReplaceLineInFile(lib_file, symbols["num_txn_entity"], repline)

def UpdateVeriConf(veri_file, is_online_verifier, sleep_ms):
    repline = symbols["verifier_sleep"] + str(sleep_ms) + ";"
    assert ReplaceLineInFile(veri_file, symbols["verifier_sleep"], repline)


def UpdateBenchConf(bench_file, read_ratio, write_ratio, rmw_ratio):
    repline = symbols["read_ratio"] + str(read_ratio) + ";"
    assert ReplaceLineInFile(bench_file, symbols["read_ratio"], repline)
    repline = symbols["write_ratio"] + str(write_ratio) + ";"
    assert ReplaceLineInFile(bench_file, symbols["write_ratio"], repline)
    repline = symbols["rmw_ratio"] + str(rmw_ratio) + ";"
    assert ReplaceLineInFile(bench_file, symbols["rmw_ratio"], repline)


# change the line start with "prefix" to "repline"
def ReplaceLineInFile(tex_path, prefix, repline):

    if not os.path.exists(tex_path):
        return False

    replace_happen = False
    with open(tex_path, 'r') as f:
        content = f.read()
    f.close()

    lines = content.splitlines()

    newlines = []
    for line in lines:
        # this is our var
        if line.strip().startswith(prefix):
            newlines.append(repline)
            replace_happen = True
        else:
            newlines.append(line)

    # construct new content
    newcontent = "\n".join(newlines)

    # write the newcontent back
    with open(tex_path, 'w') as f:
        f.write(newcontent)
    f.close()
    return replace_happen


def usage():
    print("Usage: script <ChengConstants> <VeriConstants> <Constants> [local|cloud]\
          [sign|nosign] [none|naive|lazy] [online|offline] <sleep>(in ms) \
          <#txn_in_entity>(1-N) [RO|WO|RMW|50-50|90-10]")


def main(argv):
    if len(argv) != 11:
        print("wrong number of arguments: " + str(len(argv)))
        print("current args:")
        print(argv)
        usage()
        exit(1)

    lib_file = argv[1]
    veri_file = argv[2]
    bench_file = argv[3]

    is_local_log = True if argv[4] == "local" else False
    is_sign = True if argv[5] == "sign" else False
    # type: (lazy, sample)
    #   none:  (False,0)
    #   naive: (False, 100)
    #   lazy:  (True,100)
    is_lazy = False
    sample_write_percent = 100
    if argv[6] == "lazy":
        is_lazy = True
    elif argv[6] == "none":
        sample_write_percent = 0
    else:
        assert argv[6] == "naive"

    is_online_verifier = True if argv[7]=="online" else False
    sleep_ms = int(argv[8])
    num_txn_in_entity = int(argv[9])

    # to see the detailed operations
    workload = argv[10]
    if workload == "RO":
        read_ratio = 100
        write_ratio = 0
        rmw_ratio = 0
    elif workload == "WO":
        read_ratio = 0
        write_ratio = 100
        rmw_ratio = 0
    elif workload == "RMW":
        read_ratio = 0
        write_ratio = 0
        rmw_ratio = 100
    elif workload == "50-50":
        read_ratio = 50
        write_ratio = 50
        rmw_ratio = 0
    elif workload == "90-10":
        read_ratio = 90
        write_ratio = 10
        rmw_ratio = 0
    else:
        print("wrong workload: " + workload)
        usage()
        exit(1)

    UpdateLibConf(lib_file, is_local_log, is_sign, is_lazy, sample_write_percent, num_txn_in_entity)
    UpdateVeriConf(veri_file, is_online_verifier, sleep_ms)
    UpdateBenchConf(bench_file, read_ratio, write_ratio, rmw_ratio)



if __name__ == "__main__":
    main(sys.argv)
