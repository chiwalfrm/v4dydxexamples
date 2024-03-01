from requests import get
from sys import argv

import pprint
pp = pprint.PrettyPrinter(width = 41, compact = True)

try:
        r = get(url = 'https://ipinfo.io/')
        r.raise_for_status()
        if r.status_code == 200:
                if r.json()['country'] == 'US':
#                       INDEXERURL = 'https://indexer.dydx.trade/v4'
#                       INDEXERURL = 'https://indexer.v4testnet.dydx.exchange/v4'
#                       INDEXERURL = 'https://indexer.v4staging.dydx.exchange/v4'
                        print('ERROR: You can not use this in the USA')
                        exit()
                else:
                        INDEXERURL = 'https://indexer.dydx.trade/v4'
#                       INDEXERURL = 'https://indexer.v4testnet.dydx.exchange/v4'
#                       INDEXERURL = 'https://indexer.v4staging.dydx.exchange/v4'
        else:
                print('Bad requests status code:', r.status_code)
                exit()
except Exception as error:
        print("Error: api query failed (%s)" % error)
        exit()
if len(argv) < 1:
        print('Error: Must specify market')
        exit()
market=argv[1]
r = get(url = INDEXERURL+'/historicalFunding/'+market+'?limit=24')
r.raise_for_status()
if r.status_code == 200:
        for item in r.json()['historicalFunding']:
                print(item['rate'])
else:
        print('Bad requests status code:', r.status_code)
