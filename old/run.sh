#! /bin/bash

if [ "$COBRA_HOME" == "" ]; then
  echo "COBRA_HOME hasn't been set"
  exit 1
fi

source $COBRA_HOME/env.sh

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

function usage {
  echo "Usage: run.sh run_bench [cheng|orig]"
  echo "       run.sh verifier [log|cloud]"
  echo "       run.sh hybrid"
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

function buildJNI {
  ./jni.sh || fail "build jni"
}

function buildCheng {
  buildJNI
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

if [ "$1" != "verifier" ] &&
   [ "$1" != "hybrid" ] &&
   [ "$1" != "run_bench" ]; then
  usage
  exit 1
fi

if [ "$1" == "run_bench" ]; then
  ClearLog
  if [ "$2" != "cheng" ] && [ "$2" != "orig" ]; then
    usage
    exit 1
  fi

  if [ "$2" == "cheng" ]; then
    buildCheng
  elif [ "$2" == "orig" ]; then
    buildOrig
  fi

  # run the real bench
  time java -ea -jar target/chengTxn-1-jar-with-dependencies.jar benchmark txn || fail "FAIL: java benchmark"

  # clear
  if [ "$2" == "cheng" ]; then
    # FIXME: remove the clear T1 logs
    echo "FXIME: remove the T1 logs, which is the clear procedure"
    rm $COBRA_LOG_DIR/T1.*
  fi
elif [ "$1" == "hybrid" ]; then
  ClearLog
  buildCheng
  # run the bench with online verifier
  time java -ea -jar target/chengTxn-1-jar-with-dependencies.jar hybrid || fail "FAIL: java benchmark"
elif [ "$1" == "verifier" ]; then
  if [ "$2" != "cloud" ] && [ "$2" != "log" ]; then
    usage
    exit 1
  fi
  echo "VERIFIER MUST RUN UNDER ORIG LIB"
  buildOrig
  time java -ea -Djava.library.path=$SCRIPT_DIR/include/ -jar \
    target/chengTxn-1-jar-with-dependencies.jar verifier $2 || fail "FAIL: java benchmark"
else 
  usage
  exit 1
fi

echo "DONE"
