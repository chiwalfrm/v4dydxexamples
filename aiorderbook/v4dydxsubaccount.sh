#!/bin/sh
WORKINGDIR=`dirname $0`
while true
do
        python3 -u $WORKINGDIR/v4dydxsubaccount.py $*
done
