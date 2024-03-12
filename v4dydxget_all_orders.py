from datetime import datetime
from dateutil.parser import isoparse
from os import path, popen
from random import randrange
from requests import get
from sys import argv, maxsize
import sys

INDEXERURL = 'https://indexer.dydx.trade/v4'

#counterlimit is the number of iterations it would pull records per subaccount per order-type (short-term and long-term).  The indexer API limit is 100 records per iteration.
#counterlimit = 100 means '100'(counterlimit) x number of subaccounts x 2(types of orders) interactios x 100 maximum records per iteraction
counterlimit = 100

#walletaddress
def findorder2():
        order = findorder2a()
        if order == None:
                order = findorder2b()
        return order

#walletaddress
def findorder2a():
        counter = 0
        print('findorder2a() Showing short-term orders...')
        subaccountlist = getsubaccounts()
        for subaccountnumber in subaccountlist:
                if len(subaccountlist) > 1:
                        print('Showing subaccount', str(subaccountnumber)+'...')
#               height = maxsize
#               height limited to 2147483647 for 32-bit OS, equivalent to 2038-01-19T03:14Z
                height = 2147483647
                newheight = 0
                while newheight < height:
                        if counter > counterlimit:
                                #reached counterlimit, move to next subaccount
                                break
                        r = get(INDEXERURL+'/orders', params = {
                                'address': walletaddress,
                                'subaccountNumber': subaccountnumber,
                                'return_latest_orders': True,
                                'goodTilBlockBeforeOrAt': height,
                        })
                        try:
                                r.raise_for_status()
                                if r.status_code == 200:
                                        if len(r.json()) > 0:
                                                if len(r.json()) > 1:
                                                        topheight = r.json()[0]['createdAtHeight']
                                                        newheight = r.json()[-1]['createdAtHeight']
                                                        if topheight == newheight:
                                                                print('Error: more than 100 records with the same createdAtHeight', topheight)
                                                                #move to next subaccount
                                                                break
                                                for item in r.json():
                                                        print(item)
                                                if len(r.json()) > 99 and int(newheight) < height:
                                                        height = int(newheight) + 1
                                                        newheight = 0
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

#walletaddress
def findorder2b():
        counter = 0
        print('findorder2b() Showing long-term orders...')
        subaccountlist = getsubaccounts()
        for subaccountnumber in subaccountlist:
                if len(subaccountlist) > 1:
                        print('Showing subaccount', str(subaccountnumber)+'...')
#               height = isoparse('9999-12-31T23:59:59.999Z').timestamp()
#               height limited to 2147483647 for 32-bit OS, equivalent to 2038-01-19T03:14Z
                height = isoparse('2038-01-19T03:14Z').timestamp()
                newheight = 0
                while newheight < height:
                        if counter > counterlimit:
                                #reached counterlimit, move to next subaccount
                                break
                        r = get(INDEXERURL+'/orders', params = {
                                'address': walletaddress,
                                'subaccountNumber': subaccountnumber,
                                'return_latest_orders': True,
                                'goodTilBlockTimeBeforeOrAt': datetime.utcfromtimestamp(height).isoformat()[:-3]+'Z',
                        })
                        try:
                                r.raise_for_status()
                                if r.status_code == 200:
                                        if len(r.json()) > 0:
                                                if len(r.json()) > 1:
                                                        topheight = isoparse(r.json()[0]['goodTilBlockTime']).timestamp()
                                                        newheight = isoparse(r.json()[-1]['goodTilBlockTime']).timestamp()
                                                        if topheight == newheight:
                                                                print('Error: more than 100 records with the same goodTilBlockTime', topheight)
                                                                #move to next subaccount
                                                                break
                                                for item in r.json():
                                                        print(item)
                                                if len(r.json()) > 99 and newheight < height:
                                                        height = newheight + 0.001
                                                        newheight = 0
                                                        counter += 1
                                                else:
                                                        #end of results, move to next subaccount
                                                        break
                                        else:
                                                #no results, move to next subaccount
                                                break
                                else:
                                        print('findorder2b() Bad requests status code:', r.status_code)
                                        return None
                        except Exception as error:
                                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "findorder2b() api query failed (%s)" % error)
                                return None

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

walletaddress = argv[1]
findorder2()
