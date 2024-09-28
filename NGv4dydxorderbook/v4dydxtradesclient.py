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
        global maxwidthtradeprice
        global maxwidthtradesize
        global maxtime5
        time1=time()
        if felementname == 'tradeprice' and felementsize > maxwidthtradeprice:
                fp = open(framdiskpath+'/'+fmarket+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthtradeprice = felementsize
        elif felementname == 'tradesize' and felementsize > maxwidthtradesize:
                fp = open(framdiskpath+'/'+fmarket+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthtradesize = felementsize
        time2=time()
        delta = round(time2 - time1, 2)
        if delta > maxtime5:
                maxtime5 = delta
                print('DEBUG:checkwidth(1): new maximum elapsed time:', maxtime5)

def process_message(message):
        api_data2 = json.loads(message)
        message_id = api_data2['message_id']
        mycursor = conn.execute("UPDATE v4tclient SET messageid = "+str(message_id)+" WHERE market1 = '"+market+"';")
        conn.commit()
        if api_data2['type'] == 'error':
                print('DEBUG:wserror:', api_data2)
        else:
                if message_id == 0:
                        print(api_data2)
                else:
                        trades = []
                        if isinstance(api_data2['contents'], dict):
                                tradelist = api_data2['contents']['trades']
                                for tradeitem in tradelist:
                                        trades.append(tradeitem)
                        elif isinstance(api_data2['contents'], list):
                                for trade in api_data2['contents']:
                                        for tradeitem in trade['trades']:
                                                trades.append(tradeitem)
                        for trade in trades:
                                tradecreatedat = trade['createdAt']
                                if 'createdAtHeight' in trade.keys():
                                        tradecreatedatheight = trade['createdAtHeight']
                                else:
                                        tradecreatedatheight = 'N/A'
                                tradeid = trade['id']
                                tradeprice = trade['price']
                                tradeside = trade['side']
                                tradesize = trade['size']
                                if 'liquidation' in trade.keys():
                                        tradeliquidation = trade['liquidation']
                                else:
                                        tradeliquidation = False
                                if tradeliquidation == True:
                                        liquidationstring = 'L'
                                        fp = open(ramdiskpath+'/'+market+'/liquidations', "a")
                                        fp.write(tradecreatedat+' '+tradecreatedatheight+' '+tradeprice+' '+tradeside+' ('+tradesize+')L\n')
                                        fp.close()
                                else:
                                        liquidationstring = ''
                                fp = open(ramdiskpath+'/'+market+'/lasttrade', "w")
                                fp.write(tradecreatedat+' '+tradecreatedatheight+' '+tradeprice+' '+tradeside+' ('+tradesize+')'+liquidationstring+'\n')
                                fp.close()
                                logger.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+' '+market+' '+tradecreatedat+' '+tradecreatedatheight+' '+tradeprice+' '+tradeside.ljust(4)+' ('+tradesize+')'+liquidationstring)
                                checkwidth(
                                        framdiskpath = ramdiskpath,
                                        fmarket = market,
                                        felementname = 'tradeprice',
                                        felementsize = len(tradeprice)
                                )
                                checkwidth(
                                        framdiskpath = ramdiskpath,
                                        fmarket = market,
                                        felementname = 'tradesize',
                                        felementsize = len(tradesize)
                                )

maxtime5=0
maxtime7=0
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+' v4dydxtrades.py')
logger = logging.getLogger("Rotating Log")
logger.setLevel(logging.INFO)
if sys.platform == "linux" or sys.platform == "linux2":
        # linux
        ramdiskpath = '/mnt/ramdisk5'
elif sys.platform == "darwin":
        # OS X
        ramdiskpath = '/Volumes/RAMDisk5'

if len(sys.argv) < 2:
        market = 'BTC-USD'
else:
        market = sys.argv[1]
handler = logging.handlers.RotatingFileHandler(ramdiskpath+'/v4dydxtrades'+market+'.log',
        maxBytes = 2097152,
        backupCount = 4
)
logger.addHandler(handler)

if os.path.isdir(ramdiskpath) == False:
        print('Error: Ramdisk', ramdiskpath, 'not mounted')
        sys.exit()
if os.path.ismount(ramdiskpath) == False:
        print('Warning:', ramdiskpath, 'is not a mount point')
if os.path.isdir(ramdiskpath+'/'+market) == False:
        os.system('mkdir -p '+ramdiskpath+'/'+market)
time1=time()
mycursor = conn.execute("DELETE FROM v4tclient WHERE market1 = '"+market+"';")
conn.commit()
mycursor = conn.execute("INSERT INTO v4tclient VALUES ('"+market+"', -1);")
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

maxwidthtradeprice = 0
maxwidthtradesize = 0
