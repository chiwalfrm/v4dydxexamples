import asyncio
import json
import logging, logging.handlers
import os
import psycopg
import sys
import websockets
from datetime import datetime
from time import time, sleep

conn = psycopg.connect("dbname=orderbook user=vmware")

import pprint
pp = pprint.PrettyPrinter(width = 41, compact = True)

def checkaskfiles(
        fmarket,
        faskprice,
        fasksize,
        faskoffset,
):
        global zeroaskoffset
        global maxtime1
        global maxtime2
        mycursor = conn.execute("SELECT EXISTS(SELECT offset1 FROM V4"+market1+'_'+market2+'_'+str(index1)+" WHERE type='ask' and price="+faskprice+");")
        conn.commit()
        recordexists = mycursor.fetchone()[0]
        if recordexists == 1:
                mycursor = conn.execute("SELECT offset1 FROM V4"+market1+'_'+market2+'_'+str(index1)+" WHERE type='ask' and price="+faskprice+";")
                conn.commit()
                ffaskoffset = mycursor.fetchone()[0]
        else:
                ffaskoffset = 0
        if ( recordexists == 0 and int(faskoffset) > zeroaskoffset ) or int(faskoffset) > int(ffaskoffset):
                datetime1=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if fasksize == '0':
                        if recordexists == 1:
                                time1=time()
                                mycursor = conn.execute("DELETE FROM V4"+market1+'_'+market2+'_'+str(index1)+" WHERE type='ask' and price="+faskprice+";")
                                conn.commit()
                                time2=time()
                                if time2 - time1 > maxtime1:
                                        maxtime1 = time2 - time1
                                        print('checkaskfiles(1): mv new maximum elapsed time:', '{:.2f}'.format(maxtime1))
                                zeroaskoffset = int(faskoffset)
                else:
                        time1=time()
                        mycursor = conn.execute("INSERT INTO V4"+market1+'_'+market2+'_'+str(index1)+" VALUES ('ask', "+faskprice+", "+fasksize+", "+str(faskoffset)+", '"+datetime1+"') ON CONFLICT (type, price) DO UPDATE SET size = "+fasksize+", offset1 = "+str(faskoffset)+", datetime = '"+datetime1+"';")
                        conn.commit()
                        time2=time()
                        if time2 - time1 > maxtime2:
                                maxtime2 = time2 - time1
                                print('checkaskfiles(2): mv new maximum elapsed time:', '{:.2f}'.format(maxtime2))
                logger.info(datetime1+' Updated REDIS: '+fmarket+':asks:'+faskprice+': '+str('('+fasksize+')').ljust(10)+' '+str(faskoffset))

def checkbidfiles(
        fmarket,
        fbidprice,
        fbidsize,
        fbidoffset,
):
        global zerobidoffset
        global maxtime3
        global maxtime4
        mycursor = conn.execute("SELECT EXISTS(SELECT offset1 FROM V4"+market1+'_'+market2+'_'+str(index1)+" WHERE type='bid' and price="+fbidprice+");")
        conn.commit()
        recordexists = mycursor.fetchone()[0]
        if recordexists == 1:
                mycursor = conn.execute("SELECT offset1 FROM V4"+market1+'_'+market2+'_'+str(index1)+" WHERE type='bid' and price="+fbidprice+";")
                conn.commit()
                ffbidoffset = mycursor.fetchone()[0]
        else:
                ffbidoffset = 0
        if ( recordexists == 0 and int(fbidoffset) > zerobidoffset ) or int(fbidoffset) > int(ffbidoffset):
                datetime1=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if fbidsize == '0':
                        if recordexists == 1:
                                time1=time()
                                mycursor = conn.execute("DELETE FROM V4"+market1+'_'+market2+'_'+str(index1)+" WHERE type='bid' and price="+fbidprice+";")
                                conn.commit()
                                time2=time()
                                if time2 - time1 > maxtime3:
                                        maxtime3 = time2 - time1
                                        print('checkbidfiles(1): mv new maximum elapsed time:', '{:.2f}'.format(maxtime3))
                                zerobidoffset = int(fbidoffset)
                else:
                        time1=time()
                        mycursor = conn.execute("INSERT INTO V4"+market1+'_'+market2+'_'+str(index1)+" VALUES ('bid', "+fbidprice+", "+fbidsize+", "+str(fbidoffset)+", '"+datetime1+"') ON CONFLICT (type, price) DO UPDATE SET size = "+fbidsize+", offset1 = "+str(fbidoffset)+", datetime = '"+datetime1+"';")
                        conn.commit()
                        time2=time()
                        if time2 - time1 > maxtime4:
                                maxtime4 = time2 - time1
                                print('checkbidfiles(2): mv new maximum elapsed time:', '{:.2f}'.format(maxtime4))
                logger.info(datetime1+' Updated REDIS: '+fmarket+':bid:'+fbidprice+': '+str('('+fbidsize+')').ljust(10)+' '+str(fbidoffset))

def checkwidth(
        framdiskpath,
        fmarket,
        felementname,
        felementsize
):
        global maxwidthprice
        global maxwidthsize
        global maxtime5
        global maxtime6
        if felementname == 'price' and felementsize > maxwidthprice:
                time1=time()
                fp = open(framdiskpath+'/'+fmarket+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                time2=time()
                if time2 - time1 > maxtime5:
                        maxtime5 = time2 - time1
                        print('checkwidth(1): mv new maximum elapsed time:', '{:.2f}'.format(maxtime5))
                maxwidthprice = felementsize
        elif felementname == 'size' and felementsize > maxwidthsize:
                time1=time()
                fp = open(framdiskpath+'/'+fmarket+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                time2=time()
                if time2 - time1 > maxtime6:
                        maxtime6 = time2 - time1
                        print('checkwidth(2): mv new maximum elapsed time:', '{:.2f}'.format(maxtime6))
                maxwidthsize = felementsize

def process_message(message):
        global maxtime7
        global index1
        api_data2 = json.loads(message)
        offset = api_data2['message_id']
        if offset == 0:
                print(api_data2)
        elif offset == -1:
                time1=time()
                index1 += 1
                print('Table:', 'V4'+market1+market2+str(index1))
                mycursor = conn.execute("CREATE TABLE V4"+market1+'_'+market2+'_'+str(index1)+" (type varchar(3) NOT NULL, price float NOT NULL, size float NOT NULL, offset1 bigint NOT NULL, datetime varchar(19) NOT NULL, PRIMARY KEY (type, price));")
                conn.commit()
                mycursor = conn.execute("INSERT INTO v4orderbookindex VALUES ('"+market+"', "+str(index1)+") ON CONFLICT (market1) DO UPDATE SET index1 = "+str(index1)+";")
                conn.commit()
                if index1 > 10:
                        mycursor = conn.execute("DROP TABLE V4"+market1+'_'+market2+'_'+str(index1 - 10)+";")
                        conn.commit()
                time2=time()
                if time2 - time1 > maxtime7:
                        maxtime7 = time2 - time1
                        print('wsrun(): mv new maximum elapsed time:', '{:.2f}'.format(maxtime7))
                api_data2 = json.loads(message)
                print(api_data2)
        else:
                for item in api_data2['contents'].items():
                        type1 = item[0]
                        list1 = item[1]
                        for item2 in list1:
                                if isinstance(item2, dict):
                                        price = item2['price']
                                        size = item2['size']
                                elif isinstance(item2, list):
                                        price = item2[0]
                                        size = item2[1]
                                if type1 == 'asks':
                                        checkaskfiles(
                                                fmarket = market,
                                                faskprice = price,
                                                fasksize = size,
                                                faskoffset = offset,
                                        )
                                elif type1 == 'bids':
                                        checkbidfiles(
                                                fmarket = market,
                                                fbidprice = price,
                                                fbidsize = size,
                                                fbidoffset = offset,
                                        )
                                else:
                                        print('Error: unable to handle type:', type1)
                                        exit()
                                checkwidth(
                                        framdiskpath = ramdiskpath,
                                        fmarket = market,
                                        felementname = 'price',
                                        felementsize = len(price)
                                )
                                checkwidth(
                                        framdiskpath = ramdiskpath,
                                        fmarket = market,
                                        felementname = 'size',
                                        felementsize = len(size)
                                )

maxtime1=0
maxtime2=0
maxtime3=0
maxtime4=0
maxtime5=0
maxtime6=0
maxtime7=0
maxtime8=0
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+' v4dydxob.py')
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
handler = logging.handlers.RotatingFileHandler(ramdiskpath+'/v4dydxob'+market+'.log',
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
marketarray = market.split('-')
market1 = marketarray[0]
if market1 == '1INCH':
        market1 = 'ONEINCH'
elif market1 == 'BUFFI,UNISWAP_V3,0X4C1B1302220D7DE5C22B495E78B72F2DD2457D45':
        market1 = 'BUFFI'
market2 = marketarray[1]
mycursor = conn.execute("SELECT EXISTS(SELECT index1 FROM v4orderbookindex WHERE market1 = '"+market+"');")
conn.commit()
if mycursor.fetchone()[0] == 0:
        index1 = 1
else:
        mycursor = conn.execute("SELECT index1 FROM v4orderbookindex WHERE market1 = '"+market+"';")
        conn.commit()
        index1 = mycursor.fetchone()[0] + 1
print('Table:', 'V4'+market1+market2+str(index1))
mycursor = conn.execute("CREATE TABLE V4"+market1+'_'+market2+'_'+str(index1)+" (type varchar(3) NOT NULL, price float NOT NULL, size float NOT NULL, offset1 bigint NOT NULL, datetime varchar(19) NOT NULL, PRIMARY KEY (type, price));")
conn.commit()
mycursor = conn.execute("INSERT INTO v4orderbookindex VALUES ('"+market+"', "+str(index1)+") ON CONFLICT (market1) DO UPDATE SET index1 = "+str(index1)+";")
conn.commit()
time2=time()
if time2 - time1 > maxtime8:
        maxtime8 = time2 - time1
        print('main(): mv new maximum elapsed time:', '{:.2f}'.format(maxtime8))

#logging.basicConfig(
#       format="%(message)s",
#       level=logging.DEBUG,
#)

maxwidthprice = 0
maxwidthsize = 0
zeroaskoffset = 0
zerobidoffset = 0
