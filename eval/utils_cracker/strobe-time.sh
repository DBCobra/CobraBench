#!/bin/bash

if [ "$#" != "3" ]; then
    echo "need three arguments"
    exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}"  )" >/dev/null && pwd  )"
($SCRIPT_DIR/strobe-time $1 $2 $3 &)
echo "DONE"
