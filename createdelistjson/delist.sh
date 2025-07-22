#!/bin/sh
echo "            }," > /tmp/c
market=$1
clobid=`python3 delist1.py $market`
cat <<EOF
    {
      "@type": "/dydxprotocol.clob.MsgUpdateClobPair",
      "authority": "dydx10d07y265gmmuvt4z0w9aw880jnsr700jnmapky",
      "clob_pair": {
EOF
curl -s --max-time 5 https://dydx-ops-rest.kingnodes.com/dydxprotocol/clob/clob_pair/$clobid | python3 -m json.tool | tail -n +3 | head -n -3
cat <<EOF
        "status": "STATUS_FINAL_SETTLEMENT"
      }
    },
    {
      "@type": "/slinky.marketmap.v1.MsgUpdateMarkets",
      "authority": "dydx10d07y265gmmuvt4z0w9aw880jnsr700jnmapky",
      "update_markets": [
        {
EOF
curl -s --max-time 5 https://dydx-ops-rest.kingnodes.com/slinky/marketmap/v1/marketmap | python3 -m json.tool > /tmp/a
l1=`grep -n "^            \"\`echo $market | tr - /\`\": {$" /tmp/a | cut -d : -f 1`
l2=$l1
while true
do
        sed -n "$l2,${l2}p" /tmp/a > /tmp/b
        cmp /tmp/b /tmp/c >> /dev/null
        if [ $? -eq 0 ]
        then
                break
        fi
        l2=$((l2+1))
done
sed -n "$((l1+1)),$((l2-1))p" /tmp/a | sed 's/ "enabled": true,$/ "enabled": false,/' | cut -c 7-
cat <<EOF
        }
      ]
    }
EOF
rm /tmp/[abc]
