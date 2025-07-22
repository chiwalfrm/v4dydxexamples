#!/bin/sh
cat <<EOF
{
  "title": "Wind down $# markets",
  "messages": [
EOF
for s1 in $*
do
        ./delist.sh $s1
done
cat <<EOF
  ],
  "deposit": "5000000000000000000adv4tnt",
  "expedited": true,
  "summary": "Wind down $# markets"
}
EOF
