#!/bin/bash
#
# mya.sh - bash script for transmitting sql requests to the opsmya
#          servers at Jefferson Lab, retrieving the results, and
#          sending them to stdout.
#
# Usage: mya.sh <n> <statement>
#    where <n> is 0..8 for server opsmya0..opsmya8
#    and <statement> is an sql statement to execute on the server
#
# author: richard.t.jones at uconn.edu
# version: february 5, 2018

if [[ $# -lt 2 ]]; then
    echo "Usage: mya.sh <n> <statement>"
    exit 1
fi

port=`expr 63306 + $1`
shift

mysql -u myapi -pMYA -A -P $port -h gluey.phys.uconn.edu archive \
-e "$*"
