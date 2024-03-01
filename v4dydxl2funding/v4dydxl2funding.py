from requests import get
from sys import argv

import pprint
pp = pprint.PrettyPrinter(width = 41, compact = True)

INDEXERURL = 'https://indexer.dydx.trade/v4'

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
