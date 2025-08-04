#!/bin/sh
market=$1
liquiditytier=$2
upgradetocross=$3
clobid=`python3 upgradecross1.py $market`
if [ "$upgradetocross" = y ]
then
        cat <<EOF
    {
      "@type": "/dydxprotocol.listing.MsgUpgradeIsolatedPerpetualToCross",
      "authority": "dydx10d07y265gmmuvt4z0w9aw880jnsr700jnmapky",
      "perpetual_id": $clobid
    },
EOF
fi
cat <<EOF
    {
      "@type": "/dydxprotocol.perpetuals.MsgUpdatePerpetualParams",
      "authority": "dydx10d07y265gmmuvt4z0w9aw880jnsr700jnmapky",
      "perpetual_params": {
EOF
curl -s --max-time 5 https://dydx-ops-rest.kingnodes.com/dydxprotocol/perpetuals/perpetual/$clobid | python3 -m json.tool | tail -n +4 | head -n -4 > /tmp/a
head -5 /tmp/a
cat <<EOF
            "liquidity_tier": $liquiditytier,
            "market_type": "PERPETUAL_MARKET_TYPE_CROSS"
        },
EOF
rm /tmp/a
