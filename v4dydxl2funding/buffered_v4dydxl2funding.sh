#!/bin/sh
outputfile=/tmp/v4dydxl2funding.out$$
pauseminutes=1

trap ctrl_c HUP INT

ctrl_c()
{
        rm -f $outputfile
        exit
}

while true
do
        clear
        if [ -f $outputfile ]
        then
                if [ ! -s $outputfile ]
                then
                        if [ ! -f $outputfile.bak ]
                        then
                                rm $outputfile
                                continue
                        fi
                        cat $outputfile.bak
                        echo "*** OLD OUTPUT ***"
                else
                        cat $outputfile
                        cp -p $outputfile $outputfile.bak
                fi
                dontpause=0
        else
                echo "Generating report.  Please wait..."
                dontpause=1
        fi
##### PUT COMMANDS HERE #####
        scriptcolors=1 ./v4dydxl2funding.sh `grep -v '^#' v4dydxl2funding.txt` > $outputfile 2>&1 &
#############################
        if [ "$dontpause" -eq 0 ]
        then
                ./pause.sh $pauseminutes
                pidsleft=`pstree -p $$`
                pidsleft=`echo $pidsleft | grep -o "([[:digit:]]*)" | grep -o "[[:digit:]]*" | grep -v "^$$$"`
                if [ ! -z "$pidsleft" ]
                then
                        kill -TERM $pidsleft
                fi
        fi
        pgrep -P $$
        wait
done
