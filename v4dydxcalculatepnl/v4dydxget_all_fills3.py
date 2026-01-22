import sys
from datetime import datetime
from dateutil.parser import isoparse
from os import path, popen
from random import randrange
from requests import get
from sys import argv, maxsize

INDEXERURL = 'https://indexer.dydx.trade/v4'
#INDEXERURL = 'https://indexer.v4testnet.dydx.exchange/v4'
#INDEXERURL = 'https://indexer.v4staging.dydx.exchange/v4'

#counterlimit is the number of iterations it would pull records per subaccount per order-type (short-term and long-term).  The indexer API limit is 1000 records per iteration.
#counterlimit = 100 means '100'(counterlimit) x number of subaccounts x 2(types of orders) interactios x 1000 maximum records per iteraction
counterlimit = 100

#walletaddress
def getsubaccounts():
        subaccountslist = []
        r = get(INDEXERURL+'/addresses/'+walletaddress)
        try:
                r.raise_for_status()
                if r.status_code == 200:
                        for item in r.json()['subaccounts']:
                                subaccountslist.append(item['subaccountNumber'])
                        return subaccountslist
                else:
                        print('Bad requests status code:', r.status_code)
        except Exception as error:
                print('getsubaccounts() Address not found', walletaddress)
                return None

#walletaddress
def findfills():
        counter = 0
        subaccountlist = getsubaccounts()
        for subaccountnumber in subaccountlist:
                if len(subaccountlist) > 1:
#                       print('Searching subaccount', str(subaccountnumber)+'...')
                        pass
#               height = maxsize
#               height limited to 2147483647 for 32-bit OS, equivalent to 2038-01-19T03:14Z
                height = 2147483647
                newheight = startblock
                while newheight < height:
                        if counter > counterlimit:
                                #reached counterlimit, move to next subaccount
                                break
                        if market == 'all':
                                r = get(INDEXERURL+'/fills', params = {
                                        'address': walletaddress,
                                        'subaccountNumber': subaccountnumber,
                                        'createdBeforeOrAtHeight': height,
                                })
                        else:
                                r = get(INDEXERURL+'/fills', params = {
                                        'address': walletaddress,
                                        'subaccountNumber': subaccountnumber,
                                        'market': market,
                                        'marketType': 'PERPETUAL',
                                        'createdBeforeOrAtHeight': height,
                                })
                        try:
                                r.raise_for_status()
                                if r.status_code == 200:
                                        if len(r.json()) > 0:
                                                if len(r.json()['fills']) > 1:
                                                        topheight = r.json()['fills'][0]['createdAtHeight']
                                                        newheight = r.json()['fills'][-1]['createdAtHeight']
                                                        if topheight == newheight:
                                                                print('Error: more than 1000 records with the same createdAtHeight', topheight)
                                                                #move to next subaccount
                                                                break
                                                for item in r.json()['fills']:
                                                        print(f"{item['createdAt']},{item['market']},{item['side']},{item['size']},{item['price']},{item['fee']},{item['id']}")
                                                if len(r.json()['fills']) > 999 and int(newheight) < height:
                                                        height = int(newheight) + 1
                                                        newheight = startblock
                                                        counter += 1
                                                else:
                                                        #end of results, move to next subaccount
                                                        break
                                        else:
                                                #no results, move to next subaccount
                                                break
                                else:
                                        print('findorder2a() Bad requests status code:', r.status_code)
                                        return None
                        except Exception as error:
                                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "findorder2a() api query failed (%s)" % error)
                                return None

walletaddress = argv[1]
if len(argv) > 2:
        market = argv[2]
        if len(argv) > 3:
                startblock = int(argv[3])
        else:
                startblock = 0
else:
        market = 'all'
findfills()
