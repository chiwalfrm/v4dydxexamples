#!/bin/sh
refresha()
{
        i1=0
        while [ $i1 -lt 15 ]
        do
                printf '\b|'
                sleep 1
                printf '\b/'
                sleep 1
                printf '\b-'
                sleep 1
                printf '\b\\'
                sleep 1
                i1=$((i1+1))
        done
}

refreshb()
{
        i1=0
        printf \
        "     |    1    |    2    |    3    |    4    |    5    |    6]\r["
        while [ $i1 -lt 6 ]
        do
                for i2 in . . . . \| . . . . $((i1+1))
                do
                        printf "$i2"
                        sleep 1
                done
                i1=$((i1+1))
        done
}

count=${1:-1}
i3=0
while [ $i3 -lt $count ]
do
        refreshb
        i3=$((i3+1))
        printf "] $i3\r"
done
echo
