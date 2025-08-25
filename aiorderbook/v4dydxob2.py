import os
import psycopg
import sys
from datetime import datetime
from requests import get
from time import sleep
remove_crossed_prices = True

INDEXERURL = 'https://indexer.dydx.trade/v4'
#INDEXERURL = 'https://indexer.v4testnet.dydx.exchange/v4'
#INDEXERURL = 'https://indexer.v4staging.dydx.exchange/v4'

if os.environ.get('ORDERBOOKSERVER') != None and os.environ.get('ORDERBOOKSERVER') != '':
        mysqlhost = os.environ.get('ORDERBOOKSERVER')
else:
        mysqlhost = 'localhost'

conn = psycopg.connect("dbname=orderbook user=vmware password='orderbook' host='"+mysqlhost+"'")

widthmarketstats = 24
#widthprice = 10
#widthsize = 10
widthprice = 13
widthsize = 10
widthoffset = 11

#dydxmarket
def getticksize():
        if os.path.isfile(ramdiskpath+"/"+dydxmarket+"/tickSize") == True:
                return os.popen("tail -1 "+ramdiskpath+"/"+dydxmarket+"/tickSize | awk '{print $1}'").read()[:-1]
        elif os.path.isfile(ramdiskpath+"/v4dydxmarketdata/"+dydxmarket+"/tickSize") == True:
                return os.popen("tail -1 "+ramdiskpath+"/v4dydxmarketdata/"+dydxmarket+"/tickSize | awk '{print $1}'").read()[:-1]
        count = 0
        try:
                r = get(url = INDEXERURL+'/perpetualMarkets', params = {
                        'ticker': dydxmarket
                })
                r.raise_for_status()
                if r.status_code == 200:
                        return r.json()['markets'][dydxmarket]['tickSize']
                else:
                        print('Bad requests status code:', r.status_code)
                        count += 1
                        if count > 9:
                                print('getticksize() Market not found', dydxmarket)
                                return None
        except Exception as error:
                count += 1
                print('Error:', datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "getticksize() api query failed (%s)" % error)
                print('getticksize() exception, will retry... count='+str(count))
                if count > 9:
                        print('Error: getticksize() Market not found', dydxmarket)
                        return None

if sys.platform == "linux" or sys.platform == "linux2":
        # linux
        ramdiskpath = '/mnt/ramdisk5'
elif sys.platform == "darwin":
        # OS X
        ramdiskpath = '/Volumes/RAMDisk5'
#Note: regular output needs 103 columns, compact 67, ultracompact 39

RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[0;33m'
CYAN = '\033[0;36m'
NC = '\033[0m' # No Color
REDWHITE = '\033[0;31m\u001b[47m'
GREENWHITE = '\033[0;32m\u001b[47m'

dydxticksize = 0
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+' v4dydxob2.py')
sep = " "
if len(sys.argv) < 2:
        market = 'BTC-USD'
else:
        market = sys.argv[1]
if len(sys.argv) < 3:
        depth = 10
else:
        depth = int(sys.argv[2])
marketarray = market.split('-')
market1 = marketarray[0]
if ',' in market1:
        splitmarket1 = market1.split(',')
        market1 = splitmarket1[0]
market2 = marketarray[1]
while True:
        starttime = datetime.now()
        mycursor = conn.execute("SELECT * FROM v4trades"+market1+'_'+market2+" ORDER BY datetime DESC LIMIT 1;")
        record = mycursor.fetchone()
        conn.commit()
        id = record[0]
        fsize = record[1]
        fprice = record[2]
        fside = record[3]
        fcreatedat = record[4]
        ftype = record[5]
        fcreatedatheight = record[6]
        datetime = record[7]
        askarray = []
        bidarray = []
        print('Table:', 'v4'+market1+'_'+market2)
        mycursor = conn.execute("SELECT * FROM v4"+market1+'_'+market2+";")
        for member in mycursor:
                type1 = member[0]
                price = member[1]
                size = member[2]
                offset = member[3]
                datetime1 = member[4]
                if type1 == 'ask':
                        askarray.append([price, size, offset, datetime1])
                elif type1 == 'bid':
                        bidarray.append([price, size, offset, datetime1])
        conn.commit()
        askarray.sort()
        bidarray.sort(reverse=True)
        if len(bidarray) == 0 or len(askarray) == 0:
                print('Warning: bids or asks empty', str(len(bidarray)), str(len(askarray)), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                if os.access(ramdiskpath+'/'+market, os.W_OK):
                        fp = open(ramdiskpath+'/'+market+'/TRAPemptyarrays', "a")
                        fp.write(str(len(bidarray))+','+str(len(askarray))+',0,'+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'\n')
                        fp.close()
                if os.environ.get('OB2LOOP') != None and os.environ.get('OB2LOOP').lower() == 'x':
                        if os.path.isfile(os.path.dirname(os.path.abspath(__file__))+'/'+market+'/EXITFLAG'):
                                sys.exit()
                        elif os.path.isfile(ramdiskpath+'/'+market+'/EXITFLAG') == True:
                                os.system('rm '+ramdiskpath+'/'+market+'/EXITFLAG')
                                sys.exit()
                        else:
                                sleep(1)
                                continue
                else:
                        sys.exit()
        if remove_crossed_prices == True:
                highestbidprice = 0
                lowestaskprice = 0
                while len(bidarray) > 0 and len(askarray) > 0 and ( highestbidprice == 0 or highestbidprice >= lowestaskprice ):
                        highestbid = bidarray[0]
                        lowestask = askarray[0]
                        highestbidprice = float(highestbid[0])
                        lowestaskprice = float(lowestask[0])
                        highestbidsize = float(highestbid[1])
                        lowestasksize = float(lowestask[1])
                        highestbidoffset = int(highestbid[2])
                        lowestaskoffset = int(lowestask[2])
                        if highestbidprice >= lowestaskprice:
                                if highestbidoffset < lowestaskoffset:
                                        bidarray.pop(0)
                                elif highestbidoffset > lowestaskoffset:
                                        askarray.pop(0)
                                else:
                                        if os.access(ramdiskpath+'/'+market, os.W_OK):
                                                fp = open(ramdiskpath+'/'+market+'/TRAPsameoffset', "a")
                                                fp.write(str(highestbidprice)+','+str(highestbidsize)+','+str(lowestaskprice)+','+str(lowestasksize)+','+str(highestbidoffset)+','+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'\n')
                                                fp.close()
                                        if highestbidsize > lowestasksize:
                                                askarray.pop(0)
                                                bidarray[0] = [ str(highestbidprice), str(highestbidsize - lowestasksize), str(highestbidoffset), highestbid[3] ]
                                        elif highestbidsize < lowestasksize:
                                                askarray[0] = [ str(lowestaskprice), str(lowestasksize - highestbidsize), str(lowestaskoffset), lowestask[3] ]
                                                bidarray.pop(0)
                                        else:
                                                askarray.pop(0)
                                                bidarray.pop(0)
                if len(bidarray) == 0 or len(askarray) == 0:
                        print('Warning: bids or asks empty', str(len(bidarray)), str(len(askarray)), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        if os.access(ramdiskpath+'/'+market, os.W_OK):
                                fp = open(ramdiskpath+'/'+market+'/TRAPemptyarrays', "a")
                                fp.write(str(len(bidarray))+','+str(len(askarray))+',1,'+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'\n')
                                fp.close()
                        if os.environ.get('OB2LOOP') != None and os.environ.get('OB2LOOP').lower() == 'x':
                                if os.path.isfile(os.path.dirname(os.path.abspath(__file__))+'/'+market+'/EXITFLAG'):
                                        sys.exit()
                                elif os.path.isfile(ramdiskpath+'/'+market+'/EXITFLAG') == True:
                                        os.system('rm '+ramdiskpath+'/'+market+'/EXITFLAG')
                                        sys.exit()
                                else:
                                        sleep(1)
                                        continue
                        else:
                                sys.exit()
        count = 0
        highestoffset = 0
        lowestoffset = 0
        bidsizetotal = 0
        asksizetotal = 0
        if len(sys.argv) > 3 and ( sys.argv[3] == 'compact' or sys.argv[3] == 'ultracompact' ):
                if sys.argv[3] == 'compact':
                        if fcreatedat != 0:
                                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), fcreatedat, fcreatedatheight, fprice, fside, fsize)
                        print('Bid'+' '.ljust(widthprice+widthsize+26)+'| Ask')
                elif sys.argv[3] == 'ultracompact':
                        if fcreatedat != 0:
                                print(fcreatedat[5:], fcreatedatheight, fprice, fside, fsize)
                        print('Bid'+' '.ljust(widthprice+widthsize+12)+'| Ask')
        else:
                if fcreatedat != 0:
                        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'Last trade:', str(fcreatedat)[0:19], fcreatedatheight, fprice, fside, fsize)
                print('Bid'+' '.ljust(widthprice+widthsize+widthoffset+21)+'| Ask')
        while count < min(depth, max(len(bidarray), len(askarray))):
                if count < len(bidarray):
                        biditem = bidarray[count]
                        biditemprice = float(biditem[0])
                        biditemsize = float(biditem[1])
                        biditemoffset = int(biditem[2])
                        biditemdatetime = ' '+str(biditem[3])[0:19]
                else:
                        biditemprice = ''
                        biditemsize = ''
                        biditemoffset = 0
                        biditemdatetime = ''
                if count < len(askarray):
                        askitem = askarray[count]
                        askitemprice = float(askitem[0])
                        askitemsize = float(askitem[1])
                        askitemoffset = int(askitem[2])
                        askitemdatetime = ' '+str(askitem[3])[0:19]
                else:
                        askitemprice = ''
                        askitemsize = ''
                        askitemoffset = 0
                        askitemdatetime = ''
                highestoffset = max(biditemoffset, askitemoffset, highestoffset)
                if biditemsize != '':
                        bidsizetotal += biditemsize
                        biditemsizet = '('+str(biditemsize)+')'
                        biditemoffsett = str(biditemoffset)
                else:
                        biditemsizet = ''
                        biditemoffsett = ''
                if askitemsize != '':
                        asksizetotal += askitemsize
                        askitemsizet = '('+str(askitemsize)+')'
                        askitemoffsett = str(askitemoffset)
                else:
                        askitemsizet = ''
                        askitemoffsett = ''
                if count == 0:
                        highestbidprice = biditemprice
                        lowestaskprice = askitemprice
                        lowestoffset = min(biditemoffset, askitemoffset)
                else:
                        lowestoffset = min(biditemoffset, askitemoffset, lowestoffset)
                if len(sys.argv) > 3 and ( sys.argv[3] == 'compact' or sys.argv[3] == 'ultracompact' ):
                        if sys.argv[3] == 'compact':
                                biditemoffset = ''
                                askitemoffset = ''
                                biditemdatetime = biditemdate[6:]
                                askitemdatetime = askitemdate[6:]
                        elif sys.argv[3] == 'ultracompact':
                                biditemoffset = ''
                                askitemoffset = ''
                                biditemdatetime = ''
                                askitemdatetime = ''
                else:
                        biditemoffset = ' '+str(biditemoffset).ljust(widthoffset)
                        askitemoffset = ' '+str(askitemoffset).ljust(widthoffset)
                if biditemprice == '':
                        padding=' '.ljust(20)
                else:
                        padding=''
                if dydxticksize == 0:
                        dydxmarket = market
                        dydxticksize = getticksize()
                        if dydxticksize == None:
                                print('Error: No such market', dydxmarket)
                                exit()
                        decimals = dydxticksize.count('0')
                if type(biditemprice) == float:
                        biditempricestr = f'{biditemprice:.{decimals}f}'
                else:
                        biditempricestr = biditemprice
                if type(askitemprice) == float:
                        askitempricestr = f'{askitemprice:.{decimals}f}'
                else:
                        askitempricestr = askitemprice
                print(biditempricestr.ljust(widthprice), biditemsizet.ljust(widthsize+2)+biditemoffsett.rjust(widthoffset)+biditemdatetime+padding+' | '+askitempricestr.ljust(widthprice), askitemsizet.ljust(widthsize+2)+askitemoffsett.rjust(widthoffset)+askitemdatetime, end = '\r')
                if sys.argv[-1] != 'noansi' and fcreatedat != 0:
                        if biditemprice == float(fprice):
                                print(f"{REDWHITE}{biditempricestr}{NC}", end = '\r')
                        elif askitemprice == float(fprice):
                                print(biditempricestr.ljust(widthprice), biditemsizet.ljust(widthsize+2)+biditemoffsett.rjust(widthoffset)+biditemdatetime+padding+' | '+GREENWHITE+str(askitempricestr)+NC, end = '\r')
                print()
                count += 1
        print('maxbid   :', f'{highestbidprice:.{decimals}f}')
        if '{0:.4f}'.format(lowestaskprice - highestbidprice)[:1] != '-':
                plussign = '+'
                crossmsg = ''
        else:
                plussign = ''
                crossmsg = ' *** CROSSED PRICES ***'
        print('minask   :', f'{lowestaskprice:.{decimals}f}', '('+plussign+'{0:.4f}'.format(lowestaskprice - highestbidprice)+')', '{0:.4f}'.format((lowestaskprice - highestbidprice) / highestbidprice * 100)+'%'+crossmsg)
        print('bidvolume:', bidsizetotal)
        print('askvolume:', asksizetotal)
        print('minoffset:', lowestoffset)
        print(f"maxoffset: {highestoffset} (+{highestoffset - lowestoffset})")
        mycursor = conn.execute(f"SELECT * FROM v4markets WHERE market_id = '{market}';")
        record = mycursor.fetchone()
        conn.commit()
        market_id = record[0]
        clobpairid = record[1]
        ticker = record[2]
        status = record[3]
        oracleprice = record[4]
        pricechange24h = record[5]
        volume24h = record[6]
        trades24h = record[7]
        nextfundingrate = record[8]
        initialmarginfraction = record[9]
        maintenancemarginfraction = record[10]
        openinterest = record[11]
        atomicresolution = record[12]
        quantumconversionexponent = record[13]
        ticksize = record[14]
        stepsize = record[15]
        stepbasequantums = record[16]
        subtickspertick = record[17]
        markettype = record[18]
        openinterestlowercap = record[19]
        openinterestuppercap = record[20]
        baseopeninterest = record[21]
        defaultfundingrate1h = record[22]
        effectiveat = record[23]
        effectiveatheight = record[24]
        marketid = record[25]
        datetime = record[26]
        print('priceChange24H'.ljust(17)+':', str(pricechange24h)[:widthmarketstats].ljust(widthmarketstats)+str(datetime)[0:19])
        print('nextFundingRate'.ljust(17)+':', "{:.8f}".format(nextfundingrate)[:widthmarketstats].ljust(widthmarketstats)+str(datetime)[0:19])
        print('openInterest'.ljust(17)+':', str(openinterest)[:widthmarketstats].ljust(widthmarketstats)+str(datetime)[0:19])
        print('trades24H'.ljust(17)+':', str(trades24h)[:widthmarketstats].ljust(widthmarketstats)+str(datetime)[0:19])
        print('volume24H'.ljust(17)+':', str(volume24h)[:widthmarketstats].ljust(widthmarketstats)+str(datetime)[0:19])
        print('effectiveAt'.ljust(17)+':', str(effectiveat)[0:19][:widthmarketstats].ljust(widthmarketstats)+str(datetime)[0:19])
        print('effectiveAtHeight'.ljust(17)+':', str(effectiveatheight)[:widthmarketstats].ljust(widthmarketstats)+str(datetime)[0:19])
        print('marketId'.ljust(17)+':', str(marketid)[:widthmarketstats].ljust(widthmarketstats)+str(datetime)[0:19])
        print('oraclePrice'.ljust(17)+':', str(oracleprice)[:widthmarketstats].ljust(widthmarketstats)+str(datetime)[0:19])
        endtime = datetime.now()
        print('Runtime          :' , endtime - starttime)
        if os.environ.get('OB2LOOP') != None and os.environ.get('OB2LOOP').lower() == 'x':
                if os.path.isfile(os.path.dirname(os.path.abspath(__file__))+'/'+market+'/EXITFLAG'):
                        sys.exit()
                elif os.path.isfile(ramdiskpath+'/'+market+'/EXITFLAG') == True:
                        os.system('rm '+ramdiskpath+'/'+market+'/EXITFLAG')
                        sys.exit()
                else:
                        sleep(1)
        else:
                sys.exit()
