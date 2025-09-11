import sys
from requests import get

market=sys.argv[1]
r = get(url = 'https://indexer.dydx.trade/v4/perpetualMarkets?ticker='+market)
print(r.json()['markets'][market]['clobPairId'])
