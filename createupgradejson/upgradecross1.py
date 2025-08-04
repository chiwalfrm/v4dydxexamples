import sys
from requests import get

r = get(url = 'https://indexer.dydx.trade/v4/perpetualMarkets')
for key, value in r.json()['markets'].items():
        if key == sys.argv[1]:
                print(value['clobPairId'])
