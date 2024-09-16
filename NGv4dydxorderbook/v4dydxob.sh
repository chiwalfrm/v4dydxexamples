#!/bin/sh
WORKINGDIR=`dirname $0`
if [ $# = 0 ]
then
        market=BTC-USD
else
        market=$1
fi
while true
do
        python3 -u $WORKINGDIR/v4dydxob.py $1
done
