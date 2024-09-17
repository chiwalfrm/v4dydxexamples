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

def checkmarketdata(file):
        if os.path.isfile(ramdiskpath+'/'+market+'/'+file) == True:
                fname = []
                while len(fname) != 3:
                        fp = open(ramdiskpath+'/'+market+'/'+file)
                        line = fp.readline()
                        fname = line.strip('\n\r').split(sep)
                        fp.close()
                element0 = fname[0]
                if len(sys.argv) > 3 and sys.argv[3] == 'ultracompact':
                        element1 = ''
                else:
                        element1 = ' '+fname[1]+' '+fname[2]
                if file == 'effectiveAtHeight':
                        filet = 'effectiveAtHeig'
                elif file == 'price':
                        filet = 'oraclePrice'
                else:
                        filet = file
                print(filet.ljust(15)+':', element0[:widthmarketstats].ljust(widthmarketstats)+element1)

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
if os.path.isfile(ramdiskpath+'/'+market+'/lasttrade') == False:
        print('Warning: lasttrade file', ramdiskpath+'/'+market+'/lasttrade', 'not found')
marketarray = market.split('-')
market1 = marketarray[0]
if market1 == '1INCH':
        market1 = 'ONEINCH'
elif market1 == 'BUFFI,UNISWAP_V3,0X4C1B1302220D7DE5C22B495E78B72F2DD2457D45':
        market1 = 'BUFFI'
market2 = marketarray[1]
index1 = None
while index1 == None:
        mycursor = conn.execute("SELECT index1 FROM v4orderbookindex WHERE market1 = '"+market+"';")
        index1 = mycursor.fetchone()[0]
        conn.commit()
        sleep(1)
while True:
        starttime = datetime.now()
        if os.path.isfile(ramdiskpath+'/'+market+'/lasttrade') == True:
                fname = []
                while len(fname) != 5:
                        fp = open(ramdiskpath+'/'+market+'/lasttrade')
                        line = fp.readline()
                        fname = line.strip('\n\r').split(sep)
                        fp.close()
                fcreatedat = fname[0]
                fcreatedatheight = fname[1]
                fprice = fname[2]
                fside = fname[3]
                fsize = fname[4]
        else:
                fcreatedat = 0
        askarray = []
        bidarray = []
        mycursor = conn.execute("SELECT index1 FROM v4orderbookindex WHERE market1 = '"+market+"';")
        index1 = mycursor.fetchone()[0]
        conn.commit()
        print('Table:', 'V4'+market1+'_'+market2+'_'+str(index1))
        mycursor = conn.execute("SELECT * FROM V4"+market1+'_'+market2+'_'+str(index1)+";")
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
                if os.access(ramdiskpath+'/'+market+'/TRAPemptyarrays', os.W_OK):
                        fp = open(ramdiskpath+'/'+market+'/TRAPemptyarrays', "a")
                        fp.write(str(len(bidarray))+','+str(len(askarray))+',0,'+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'\n')
                        fp.close()
                if os.path.isfile(os.path.dirname(os.path.abspath(__file__))+'/'+market+'/EXITFLAG'):
                        sys.exit()
                elif os.path.isfile(ramdiskpath+'/'+market+'/EXITFLAG') == True:
                        os.system('rm '+ramdiskpath+'/'+market+'/EXITFLAG')
                        sys.exit()
                else:
                        sleep(1)
                        continue
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
                                        if os.access(ramdiskpath+'/'+market+'/TRAPsameoffset', os.W_OK):
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
                        if os.access(ramdiskpath+'/'+market+'/TRAPemptyarrays', os.W_OK):
                                fp = open(ramdiskpath+'/'+market+'/TRAPemptyarrays', "a")
                                fp.write(str(len(bidarray))+','+str(len(askarray))+',1,'+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'\n')
                                fp.close()
                        if os.path.isfile(os.path.dirname(os.path.abspath(__file__))+'/'+market+'/EXITFLAG'):
                                sys.exit()
                        elif os.path.isfile(ramdiskpath+'/'+market+'/EXITFLAG') == True:
                                os.system('rm '+ramdiskpath+'/'+market+'/EXITFLAG')
                                sys.exit()
                        else:
                                sleep(1)
                                continue
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
                        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'Last trade:', fcreatedat, fcreatedatheight, fprice, fside, fsize)
                print('Bid'+' '.ljust(widthprice+widthsize+widthoffset+21)+'| Ask')
        while count < min(depth, max(len(bidarray), len(askarray))):
                if count < len(bidarray):
                        biditem = bidarray[count]
                        biditemprice = float(biditem[0])
                        biditemsize = float(biditem[1])
                        biditemoffset = int(biditem[2])
                        biditemdatetime = ' '+biditem[3]
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
                        askitemdatetime = ' '+askitem[3]
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
#       checkmarketdata('indexPrice')
#       checkmarketdata('oraclePrice')
        checkmarketdata('priceChange24H')
        checkmarketdata('nextFundingRate')
#       checkmarketdata('nextFundingAt')
        checkmarketdata('openInterest')
        checkmarketdata('trades24H')
        checkmarketdata('volume24H')
        checkmarketdata('effectiveAt')
        checkmarketdata('effectiveAtHeight')
        checkmarketdata('marketId')
        checkmarketdata('price')
        endtime = datetime.now()
        print('Runtime        :' , endtime - starttime)
        if os.path.isfile(os.path.dirname(os.path.abspath(__file__))+'/'+market+'/EXITFLAG'):
                sys.exit()
        elif os.path.isfile(ramdiskpath+'/'+market+'/EXITFLAG') == True:
                os.system('rm '+ramdiskpath+'/'+market+'/EXITFLAG')
                sys.exit()
        else:
                sleep(1)
