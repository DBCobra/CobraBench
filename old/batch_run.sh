#! /bin/bash

if [ "$COBRA_HOME" == "" ]; then
  echo "COBRA_HOME hasn't been set"
  exit 1
fi

source $COBRA_HOME/env.sh

# ==== variables ====

WORKLOADS=( "RO" "WO" "50-50" "90-10" "RMW" )

# ==== helper functions ====

function usage {
  echo "Usage: batch_run.sh"
}

function linkChengLib {
  echo "LINK CHENG LIB"
  cd $COBRA_HOME
  set -x
  mvn install:install-file -Dfile="$GOOGLE_LIB_HOME/google-cloud-datastore/target/google-cloud-datastore-1.14.1-SNAPSHOT.jar" \
    -DgroupId=com.google.cloud-cheng \
    -DartifactId=google-cloud-datastore \
    -Dversion=1.14.1-SNAPSHOT \
    -Dpackaging=jar \
    -DpomFile="$CONFIG_DIR/pom.xml" || fail "mvn install"
  cd -
  set +x
}

function rebuildChengLib {
  echo "rebuild cheng lib"
  cd $GOOGLE_DATASTORE_LIB_HOME
  set -x
  mvn clean || fail "lib clean fail"
  mvn install || fail "lib rebuild fail"
  cd -
  set +x
}

function buildCheng {
  linkChengLib
  echo "BUILD WITH CHENG LIB"
  cp ./pom_cheng_lib.xml ./pom.xml || fail "copy pom_cheng"
  mvn install || fail "mvn install"
}

function buildOrig {
  echo "BUILD WITH ORIG LIB"
  cp ./pom_orig_lib.xml ./pom.xml || fail "copy pom_orig"
  mvn install || fail "mvn install"
}

function runWithVerifier {
  time java -ea -jar target/chengTxn-1-jar-with-dependencies.jar hybrid >> $COBRA_SCRIPT_MISC_LOG || fail "FAIL: java benchmark"
}

function runWithoutVerifier {
  time java -ea -jar target/chengTxn-1-jar-with-dependencies.jar benchmark txn >> $COBRA_SCRIPT_MISC_LOG || fail "FAIL: java benchmark"
}

function runJar {
  if [ "$1" == "online" ]; then
    runWithVerifier
  elif [ "$1" == "offline" ]; then
    runWithoutVerifier
  else
    echo "runJar the arg is not [online|offline], but [$1]"
    exit 1
  fi
}

function autoConfig {
  if [ $# != 7 ]; then
    echo "config <ChengConstants> <VeriConstants> <Constants> [local|cloud] [sign|nosign]
    [none|naive|lazy] [online|offline] <sleep>(in ms) <#txn_in_entity>(1-N) <workload>"
    exit 0
  fi
  echo "**** $1 $2 $3 $4 $5 $6 $7 ****" >> $COBRA_SCRIPT_MISC_LOG
  python ./config.py $LIB_CONFIG_FILE $VERI_CONFIG_FILE $BENCH_CONFIG_FILE $1 $2 $3 $5 $5 $6 $7
}

# ==== intermedia functions ====

# should do: auto config -> build the bench -> run
function origAllWorkloadHelper {
  if [ $# != 6 ]; then
    echo "origAllWorkloadHelper arg problem"
    exit 1
  fi
  # iterate all the workloads
  for workload in "${WORKLOADS[@]}"; do
    autoConfig $1 $2 $3 $4 $5 $6 $workload
    buildOrig
    runWithoutVerifier
  done
}

# NOTE: $4 is [online|offline]
function chengAllWorkloadHelper {
  if [ $# != 6 ]; then
    echo "chengAllWorkloadHelper arg problem"
    exit 1
  fi
  # iterate all the workloads
  for workload in "${WORKLOADS[@]}"; do
    autoConfig $1 $2 $3 $4 $5 $6 $workload
    ClearLog
    rebuildChengLib
    buildCheng
    runJar $4
  done
}

# ==== main logic ====
# config 
# build
# run bench

# clear the log
rm $COBRA_SCRIPT_MISC_LOG

# config <ChengConstants> <VeriConstants> <Constants> [local|cloud] [sign|nosign]
#         [none|naive|lazy] [online|offline] <sleep>(in ms) <#txn_in_entity>(1-N) <workload>

# ---baseline
# a1  baseline:RO:--
# a2  baseline:WO:--
# a3  baseline:50-50:--
# a4  baseline:90-10:--
# a5  baseline:RMW:--
origAllWorkloadHelper "local" "nosign" "none" "offline" "1000" "10"

# should be little differences
#chengAllWorkloadHelper "local" "nosign" "none" "offline" "1000" "10"

# ---cobra
# a6  cobra(10):RO:1s
# a7  cobra(10):WO:1s
# a8  cobra(10):50-50:1s
# a9  cobra(10):90-10:1s
# a10 cobra(10):RMW:1s
chengAllWorkloadHelper "cloud" "sign" "lazy" "online" "1000" "10"

# ---naive cobra
# a11 naive-cobra(10):RO:1s
# a12 naive-cobra(10):WO:1s
# a13 naive-cobra(10):50-50:1s
# a14 naive-cobra(10):90-10:1s
# a15 naive-cobra(10):RMW:1s
chengAllWorkloadHelper "cloud" "sign" "naive" "online" "1000" "10"

