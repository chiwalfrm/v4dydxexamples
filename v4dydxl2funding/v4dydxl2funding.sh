#!/bin/bash
#below is >50% annualized
red1hr=0.00570385
red8hr=0.04563084
red24hr=0.1368925
#below is >75% annualized
deepred1hr=0.0085557
deepred8hr=0.0684462
deepred24hr=0.2053388
starttimestamp=`date +%s`
#below is >100% annualized
maxred1hr=0.0114077
maxred8hr=0.0912616
maxred24hr=0.2737850
if [ -t 1 -o "$scriptcolors" = "1" ]
then
        RED='\033[0;31m'
        GREEN='\033[0;32m'
        YELLOW='\033[0;33m'
        CYAN='\033[0;36m'
        NC='\033[0m' # No Color
        color2='\033[0;31m'
        color10='\033[1;31m'
        color12='\033[1;33m'
else
        RED=''
        GREEN=''
        YELLOW=''
        CYAN=''
        NC=''
        color2=''
        color10=''
        color12=''
fi
for s1 in $*
do
        market=`echo $s1 | cut -d : -f 1`
        python3 v4dydxl2funding.py $market > /tmp/v4dydxl2funding$market$$ &
done
wait
for s1 in $*
do
        market=`echo $s1 | cut -d : -f 1`
        highlight=`echo $s1 | cut -d : -f 2`
        if [ \( -t 1 -o "$scriptcolors" = "1" \) -a "$highlight" = 1 ]
        then
                backgroundcolor='\e[1;46m'
                backgroundcoloroff='\e[0m'
        else
                backgroundcolor=
                backgroundcoloroff=
        fi
        fundingarray=( `cat /tmp/v4dydxl2funding$market$$` )
        counter=0
        fundingsum=0
        while [ $counter -lt ${#fundingarray[@]} ]
        do
                if [ $counter -eq 0 ]
                then
                        funding1hr=`echo ${fundingarray[$counter]} '*' 100 | bc -l`
                fi
                fundingsum=`echo $fundingsum + ${fundingarray[$counter]} '*' 100 | bc -l`
                if [ $counter -eq 7 ]
                then
                        funding8hr=$fundingsum
                fi
                counter=$((counter+1))
        done
        /usr/bin/printf "${backgroundcolor}%-12s ${backgroundcoloroff}" $market

        warning=`echo $funding1hr '>' $maxred1hr | bc -l`
        if [ "$warning" -eq 1 ]
        then
                LC_NUMERIC=en_US.utf8 /usr/bin/printf "${color2}%'9.6f (1hr)${NC} " $funding1hr
        else
                warning=`echo $funding1hr '>' $deepred1hr | bc -l`
                if [ "$warning" -eq 1 ]
                then
                        LC_NUMERIC=en_US.utf8 /usr/bin/printf "${color10}%'9.6f (1hr)${NC} " $funding1hr
                else
                        warning=`echo $funding1hr '>' $red1hr | bc -l`
                        if [ "$warning" -eq 1 ]
                        then
                                LC_NUMERIC=en_US.utf8 /usr/bin/printf "${color12}%'9.6f (1hr)${NC} " $funding1hr
                        else
                                LC_NUMERIC=en_US.utf8 /usr/bin/printf "${GREEN}%'9.6f (1hr)${NC} " $funding1hr
                        fi
                fi
        fi

        warning=`echo $funding8hr '>' $red8hr | bc -l`
        if [ "$warning" -eq 1 ]
        then
                LC_NUMERIC=en_US.utf8 /usr/bin/printf "${color12}%'9.6f (8hr)${NC} " $funding8hr
        else
                warning=`echo $funding8hr '>' $deepred8hr | bc -l`
                if [ "$warning" -eq 1 ]
                then
                        LC_NUMERIC=en_US.utf8 /usr/bin/printf "${color10}%'9.6f (8hr)${NC} " $funding8hr
                else
                        warning=`echo $funding8hr '>' $maxred8hr | bc -l`
                        if [ "$warning" -eq 1 ]
                        then
                                LC_NUMERIC=en_US.utf8 /usr/bin/printf "${color2}%'9.6f (8hr)${NC} " $funding8hr
                        else
                                LC_NUMERIC=en_US.utf8 /usr/bin/printf "${GREEN}%'9.6f (8hr)${NC} " $funding8hr
                        fi
                fi
        fi

        warning=`echo $fundingsum '>' $red24hr | bc -l`
        if [ "$warning" -eq 1 ]
        then
                LC_NUMERIC=en_US.utf8 /usr/bin/printf "${color12}%'9.6f (24hr)${NC} v4dydx\n" $fundingsum
        else
                warning=`echo $fundingsum '>' $deepred24hr | bc -l`
                if [ "$warning" -eq 1 ]
                then
                        LC_NUMERIC=en_US.utf8 /usr/bin/printf "${color10}%'9.6f (24hr)${NC} v4dydx\n" $fundingsum
                else
                        warning=`echo $fundingsum '>' $maxred24hr | bc -l`
                        if [ "$warning" -eq 1 ]
                        then
                                LC_NUMERIC=en_US.utf8 /usr/bin/printf "${color2}%'9.6f (24hr)${NC} v4dydx\n" $fundingsum
                        else
                                LC_NUMERIC=en_US.utf8 /usr/bin/printf "${GREEN}%'9.6f (24hr)${NC} v4dydx\n" $fundingsum
                        fi
                fi
        fi
        rm /tmp/v4dydxl2funding$market$$
done
endtimestamp=`date +%s`
echo "Generated: `date` / Runtime $((endtimestamp - starttimestamp)) seconds"
vmware@fileshare:/mnt/repository/v4github$
