#!/bin/bash
pretty()
{
items=($*)
# Get the number of items
num_items=${#items[@]}

if [ "$num_items" -eq 0 ]; then
    result=""
elif [ "$num_items" -eq 1 ]; then
    result="${items[0]}"
elif [ "$num_items" -eq 2 ]; then
    result="${items[0]} and ${items[1]}"
else
    # Join all but the last item with commas
    printf -v comma_separated_part "%s, " "${items[@]:0:$((num_items-1))}"
    # Remove the trailing comma and space
    comma_separated_part="${comma_separated_part%, }"

    # Combine with "and" and the last item
    result="${comma_separated_part} and ${items[$((num_items-1))]}"
fi
echo $result
}

marketlist=()
for s1 in $*
do
        s2=`echo $s1 | cut -d : -f 1`
        marketlist+=($s2)
done
prettylist=`pretty ${marketlist[@]}`
cat <<EOF
{
  "title": "Upgrade $prettylist markets from ISOLATED to CROSS and/or change liquidity_tiers",
  "messages": [
EOF
for s1 in $*
do
        s2=`echo $s1 | cut -d : -f 1`
        s3=`echo $s1 | cut -d : -f 2`
        s4=`echo $s1 | cut -d : -f 3`
        ./upgradecross.sh $s2 $s3 $s4
        if [ $s1 = "${*: -1}" ]
        then
                echo "    }"
        else
                echo "    },"
        fi
done
cat <<EOF
  ],
  "deposit": "5000000000000000000adv4tnt",
  "expedited": true,
  "summary": "Upgrade $prettylist markets from ISOLATED to CROSS and/or change liquidity_tiers"
}
EOF
