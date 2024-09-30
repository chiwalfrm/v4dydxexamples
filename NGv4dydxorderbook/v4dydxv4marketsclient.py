import json
import logging, logging.handlers
import os
import psycopg
import sys
from datetime import datetime
from time import time

conn = psycopg.connect("dbname=orderbook user=vmware")

def checkwidth(
        framdiskpath,
        fmarket,
        felementname,
        felementsize
):
#       global maxwidthindexPrice
#       global maxwidthnextFundingAt
        global maxwidthnextFundingRate
        global maxwidthopenInterest
        global maxwidthoraclePrice
        global maxwidthpriceChange24H
        global maxwidthtrades24H
        global maxwidthvolume24H
        global maxwidtheffectiveAt
        global maxwidtheffectiveAtHeight
        global maxwidthmarketId
        global maxwidthatomicResolution
        global maxwidthbaseAsset
        global maxwidthbasePositionNotional
        global maxwidthbasePositionSize
        global maxwidthclobPairId
        global maxwidthincrementalPositionSize
        global maxwidthinitialMarginFraction
        global maxwidthlastPrice
        global maxwidthmaintenanceMarginFraction
        global maxwidthmaxPositionSize
        global maxwidthminOrderBaseQuantums
        global maxwidthquantumConversionExponent
        global maxwidthquoteAsset
        global maxwidthstatus
        global maxwidthstepBaseQuantums
        global maxwidthstepSize
        global maxwidthsubticksPerTick
        global maxwidthticker
        global maxwidthtickSize
        global maxtime5
        time1=time()
        if felementsize == 0:
                return None
#       elif felementname == 'indexPrice' and felementsize > maxwidthindexPrice:
#               fp = open(framdiskpath+'/maxwidth'+felementname, "w")
#               fp.write(str(felementsize)+'\n')
#               fp.close()
#               maxwidthindexPrice = felementsize
#       elif felementname == 'nextFundingAt' and felementsize > maxwidthnextFundingAt:
#               fp = open(framdiskpath+'/maxwidth'+felementname, "w")
#               fp.write(str(felementsize)+'\n')
#               fp.close()
#               maxwidthnextFundingAt = felementsize
        elif felementname == 'nextFundingRate' and felementsize > maxwidthnextFundingRate:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthnextFundingRate = felementsize
        elif felementname == 'openInterest' and felementsize > maxwidthopenInterest:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthopenInterest = felementsize
        elif felementname == 'oraclePrice' and felementsize > maxwidthoraclePrice:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthoraclePrice = felementsize
        elif felementname == 'priceChange24H' and felementsize > maxwidthpriceChange24H:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthpriceChange24H = felementsize
        elif felementname == 'trades24H' and felementsize > maxwidthtrades24H:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthtrades24H = felementsize
        elif felementname == 'volume24H' and felementsize > maxwidthvolume24H:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthvolume24H = felementsize
        elif felementname == 'effectiveAt' and felementsize > maxwidtheffectiveAt:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidtheffectiveAt = felementsize
        elif felementname == 'effectiveAtHeight' and felementsize > maxwidtheffectiveAtHeight:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidtheffectiveAtHeight = felementsize
        elif felementname == 'marketId' and felementsize > maxwidthmarketId:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthmarketId = felementsize
        elif felementname == 'atomicResolution' and felementsize > maxwidthatomicResolution:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'baseAsset' and felementsize > maxwidthbaseAsset:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'basePositionNotional' and felementsize > maxwidthbasePositionNotional:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'basePositionSize' and felementsize > maxwidthbasePositionSize:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'clobPairId' and felementsize > maxwidthclobPairId:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'incrementalPositionSize' and felementsize > maxwidthincrementalPositionSize:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'initialMarginFraction' and felementsize > maxwidthinitialMarginFraction:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'lastPrice' and felementsize > maxwidthlastPrice:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'maintenanceMarginFraction' and felementsize > maxwidthmaintenanceMarginFraction:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'maxPositionSize' and felementsize > maxwidthmaxPositionSize:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'minOrderBaseQuantums' and felementsize > maxwidthminOrderBaseQuantums:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'quantumConversionExponent' and felementsize > maxwidthquantumConversionExponent:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'quoteAsset' and felementsize > maxwidthquoteAsset:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'status' and felementsize > maxwidthstatus:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'stepBaseQuantums' and felementsize > maxwidthstepBaseQuantums:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'stepSize' and felementsize > maxwidthstepSize:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'subticksPerTick' and felementsize > maxwidthsubticksPerTick:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'ticker' and felementsize > maxwidthticker:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'tickSize' and felementsize > maxwidthtickSize:
                fp = open(framdiskpath+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname not in [
                'price',
                'size',
                'nextFundingRate',
                'openInterest',
                'priceChange24H',
                'trades24H',
                'volume24H',
                'effectiveAt',
                'effectiveAtHeight',
                'marketId',
                'oraclePrice',
                'atomicResolution',
                'baseAsset',
                'basePositionNotional',
                'basePositionSize',
                'clobPairId',
                'incrementalPositionSize',
                'initialMarginFraction',
                'lastPrice',
                'maintenanceMarginFraction',
                'maxPositionSize',
                'minOrderBaseQuantums',
                'quantumConversionExponent',
                'quoteAsset',
                'status',
                'stepBaseQuantums',
                'stepSize',
                'subticksPerTick',
                'ticker',
                'tickSize',
        ]:
                fp = open(framdiskpath+'/maxwidthgeneric', "a")
                fp.write(felementname+' '+str(felementsize)+'\n')
                fp.close()
        time2=time()
        delta = round(time2 - time1, 2)
        if delta > maxtime5:
                maxtime5 = delta
                print('DEBUG:checkwidth(1): new maximum elapsed time:', maxtime5)


def processcontentsdict(
        framdiskpath,
        fcontentsdict,
        fenvelope
):
        for market, marketdata in fcontentsdict.items():
                if os.path.isdir(framdiskpath+'/'+market) == False:
                        os.system('mkdir -p '+framdiskpath+'/'+market)
                for marketdataelement, marketdatavalue in marketdata.items():
                        if fenvelope == 'oraclePrices' and marketdataelement == 'price':
                                marketdataelement = 'oraclePrice'
                        fp = open(framdiskpath+'/'+market+'/'+marketdataelement, "w")
                        fp.write(str(marketdatavalue)+' '+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'\n')
                        fp.close()
                        checkwidth(
                                framdiskpath = framdiskpath,
                                fmarket = market,
                                felementname = marketdataelement,
                                felementsize = len(str(marketdatavalue))
                        )

def process_message(message):
        api_data2 = json.loads(message)
        message_id = api_data2['message_id']
        mycursor = conn.execute("UPDATE v4mclient SET messageid = "+str(message_id)+" WHERE market1 = 'current';")
        conn.commit()
        if api_data2['type'] == 'error':
                print('DEBUG:wserror:', api_data2)
        else:
                if message_id == 0:
                        print(api_data2)
                else:
                        if isinstance(api_data2['contents'], dict):
                                if 'markets' in api_data2['contents'].keys():
                                        processcontentsdict(
                                                framdiskpath = ramdiskpath,
                                                fcontentsdict = api_data2['contents']['markets'],
                                                fenvelope = 'markets'
                                        )
                                        logger.info("{'timestamp': '"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'} (markets)")
                                        logger.info(api_data2['contents']['markets'])
                                elif 'trading' in api_data2['contents'].keys():
                                        processcontentsdict(
                                                framdiskpath = ramdiskpath,
                                                fcontentsdict = api_data2['contents']['trading'],
                                                fenvelope = 'trading'
                                        )
                                        logger.info("{'timestamp': '"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'} (trading)")
                                        logger.info(api_data2['contents']['trading'])
                                elif 'oraclePrices' in api_data2['contents'].keys():
                                        processcontentsdict(
                                                framdiskpath = ramdiskpath,
                                                fcontentsdict = api_data2['contents']['oraclePrices'],
                                                fenvelope = 'oraclePrices'
                                        )
                                        logger.info("{'timestamp': '"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'} (oraclePrices)")
                                        logger.info(api_data2['contents']['oraclePrices'])
                                else:
                                        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" === v4_markets key not handled ===")
                                        print(api_data2['contents'].keys)
                                        logger.info("{'timestamp': '"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'} (OTHER)")
                                        logger.info(api_data2['contents'])
                                logger.info("{'timestamp': '"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'} (DICT)")
                                logger.info(api_data2)
                        elif isinstance(api_data2['contents'], list):
                                for item in api_data2['contents']:
                                        if 'markets' in item.keys():
                                                processcontentsdict(
                                                        framdiskpath = ramdiskpath,
                                                        fcontentsdict = item['markets'],
                                                        fenvelope = 'markets'
                                                )
                                                logger.info("{'timestamp': '"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'} (markets)")
                                                logger.info(item['markets'])
                                        if 'trading' in item.keys():
                                                processcontentsdict(
                                                        framdiskpath = ramdiskpath,
                                                        fcontentsdict = item['trading'],
                                                        fenvelope = 'trading'
                                                )
                                                logger.info("{'timestamp': '"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'} (trading)")
                                                logger.info(item['trading'])
                                        elif 'oraclePrices' in item.keys():
                                                processcontentsdict(
                                                        framdiskpath = ramdiskpath,
                                                        fcontentsdict = item['oraclePrices'],
                                                        fenvelope = 'trading'
                                        )
                                                logger.info("{'timestamp': '"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'} (oraclePrices)")
                                                logger.info(item['oraclePrices'])
                                        else:
                                                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" === v4_markets key not handled ===")
                                                print(item.keys())
                                                logger.info("{'timestamp': '"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'} (OTHER)")
                                                logger.info(item)

maxtime5=0
maxtime7=0
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+' v4dydxv4markets.py')
logger = logging.getLogger("Rotating Log")
logger.setLevel(logging.INFO)
if sys.platform == "linux" or sys.platform == "linux2":
        # linux
        ramdiskpath = '/mnt/ramdisk5'
elif sys.platform == "darwin":
        # OS X
        ramdiskpath = '/Volumes/RAMDisk5'

handler = logging.handlers.RotatingFileHandler(ramdiskpath+'/v4dydxv4markets.log',
        maxBytes = 2097152,
        backupCount = 4
)
logger.addHandler(handler)

if os.path.isdir(ramdiskpath) == False:
        print('Error: Ramdisk', ramdiskpath, 'not mounted')
        sys.exit()
if os.path.ismount(ramdiskpath) == False:
        print('Warning:', ramdiskpath, 'is not a mount point')
time1=time()
mycursor = conn.execute("DELETE FROM v4mclient;")
conn.commit()
mycursor = conn.execute("INSERT INTO v4mclient VALUES ('current', -1);")
conn.commit()
time2=time()
delta = round(time2 - time1, 2)
if delta > maxtime7:
        maxtime7 = delta
        print('DEBUG:main(client): new maximum elapsed time:', maxtime7)

#logging.basicConfig(
#       format="%(message)s",
#       level=logging.DEBUG,
#)

#maxwidthindexPrice = 0
#maxwidthnextFundingAt = 0
maxwidthnextFundingRate = 0
maxwidthopenInterest = 0
maxwidthoraclePrice = 0
maxwidthpriceChange24H = 0
maxwidthtrades24H = 0
maxwidthvolume24H = 0
maxwidtheffectiveAt = 0
maxwidtheffectiveAtHeight = 0
maxwidthmarketId = 0
maxwidthatomicResolution = 0
maxwidthbaseAsset = 0
maxwidthbasePositionNotional = 0
maxwidthbasePositionSize = 0
maxwidthclobPairId = 0
maxwidthincrementalPositionSize = 0
maxwidthinitialMarginFraction = 0
maxwidthlastPrice = 0
maxwidthmaintenanceMarginFraction = 0
maxwidthmaxPositionSize = 0
maxwidthminOrderBaseQuantums = 0
maxwidthquantumConversionExponent = 0
maxwidthquoteAsset = 0
maxwidthstatus = 0
maxwidthstepBaseQuantums = 0
maxwidthstepSize = 0
maxwidthsubticksPerTick = 0
maxwidthticker = 0
maxwidthtickSize = 0
