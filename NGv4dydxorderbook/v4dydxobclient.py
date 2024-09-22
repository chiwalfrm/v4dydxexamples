import json
import logging, logging.handlers
import os
import psycopg
import sys
from datetime import datetime
from time import time

conn = psycopg.connect("dbname=orderbook user=vmware")

def checkaskfiles(
        fmarket,
        faskprice,
        fasksize,
        faskoffset,
):
        global zeroaskoffset
        global maxtime1
        global maxtime2
        mycursor = conn.execute("SELECT EXISTS(SELECT 1 FROM V4"+market1+'_'+market2+'_'+str(index1)+" WHERE type='ask' and price="+faskprice+");")
        recordexists = mycursor.fetchone()[0]
        conn.commit()
        if recordexists == True:
                mycursor = conn.execute("SELECT offset1 FROM V4"+market1+'_'+market2+'_'+str(index1)+" WHERE type='ask' and price="+faskprice+";")
                ffaskoffset = mycursor.fetchone()[0]
                conn.commit()
        else:
                ffaskoffset = 0
        if ( recordexists == False and int(faskoffset) > zeroaskoffset ) or int(faskoffset) > int(ffaskoffset):
                datetime1=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if fasksize == '0':
                        if recordexists == True:
                                time1=time()
                                mycursor = conn.execute("DELETE FROM V4"+market1+'_'+market2+'_'+str(index1)+" WHERE type='ask' and price="+faskprice+";")
                                conn.commit()
                                time2=time()
                                delta = round(time2 - time1, 2)
                                if delta > maxtime1:
                                        maxtime1 = delta
                                        print('DEBUG:checkaskfiles(1): new maximum elapsed time:', maxtime1)
                                zeroaskoffset = int(faskoffset)
                else:
                        time1=time()
                        mycursor = conn.execute("INSERT INTO V4"+market1+'_'+market2+'_'+str(index1)+" VALUES ('ask', "+faskprice+", "+fasksize+", "+str(faskoffset)+", '"+datetime1+"') ON CONFLICT (type, price) DO UPDATE SET size = "+fasksize+", offset1 = "+str(faskoffset)+", datetime = '"+datetime1+"';")
                        conn.commit()
                        time2=time()
                        delta = round(time2 - time1, 2)
                        if delta > maxtime2:
                                maxtime2 = delta
                                print('DEBUG:checkaskfiles(2): new maximum elapsed time:', maxtime2)
                logger.info(datetime1+' Updated: '+fmarket+':asks:'+faskprice+': '+str('('+fasksize+')').ljust(10)+' '+str(faskoffset))

def checkbidfiles(
        fmarket,
        fbidprice,
        fbidsize,
        fbidoffset,
):
        global zerobidoffset
        global maxtime3
        global maxtime4
        mycursor = conn.execute("SELECT EXISTS(SELECT 1 FROM V4"+market1+'_'+market2+'_'+str(index1)+" WHERE type='bid' and price="+fbidprice+");")
        recordexists = mycursor.fetchone()[0]
        conn.commit()
        if recordexists == True:
                mycursor = conn.execute("SELECT offset1 FROM V4"+market1+'_'+market2+'_'+str(index1)+" WHERE type='bid' and price="+fbidprice+";")
                ffbidoffset = mycursor.fetchone()[0]
                conn.commit()
        else:
                ffbidoffset = 0
        if ( recordexists == False and int(fbidoffset) > zerobidoffset ) or int(fbidoffset) > int(ffbidoffset):
                datetime1=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if fbidsize == '0':
                        if recordexists == True:
                                time1=time()
                                mycursor = conn.execute("DELETE FROM V4"+market1+'_'+market2+'_'+str(index1)+" WHERE type='bid' and price="+fbidprice+";")
                                conn.commit()
                                time2=time()
                                delta = round(time2 - time1, 2)
                                if delta > maxtime3:
                                        maxtime3 = delta
                                        print('DEBUG:checkbidfiles(1): new maximum elapsed time:', maxtime3)
                                zerobidoffset = int(fbidoffset)
                else:
                        time1=time()
                        mycursor = conn.execute("INSERT INTO V4"+market1+'_'+market2+'_'+str(index1)+" VALUES ('bid', "+fbidprice+", "+fbidsize+", "+str(fbidoffset)+", '"+datetime1+"') ON CONFLICT (type, price) DO UPDATE SET size = "+fbidsize+", offset1 = "+str(fbidoffset)+", datetime = '"+datetime1+"';")
                        conn.commit()
                        time2=time()
                        delta = round(time2 - time1, 2)
                        if delta > maxtime4:
                                maxtime4 = delta
                                print('DEBUG:checkbidfiles(2): new maximum elapsed time:', maxtime4)
                logger.info(datetime1+' Updated: '+fmarket+':bid:'+fbidprice+': '+str('('+fbidsize+')').ljust(10)+' '+str(fbidoffset))

def checkwidth(
        framdiskpath,
        fmarket,
        felementname,
        felementsize
):
        global maxwidthprice
        global maxwidthsize
        global maxtime5
        time1=time()
        if felementname == 'price' and felementsize > maxwidthprice:
                fp = open(framdiskpath+'/'+fmarket+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthprice = felementsize
        elif felementname == 'size' and felementsize > maxwidthsize:
                fp = open(framdiskpath+'/'+fmarket+'/maxwidth'+felementname, "w")
                fp.write(str(felementsize)+'\n')
                fp.close()
                maxwidthsize = felementsize
        time2=time()
        delta = round(time2 - time1, 2)
        if delta > maxtime5:
                maxtime5 = delta
                print('DEBUG:checkwidth(1): new maximum elapsed time:', maxtime5)

def process_message(message):
        api_data2 = json.loads(message)
        offset = api_data2['message_id']
        mycursor = conn.execute("UPDATE v4client SET messageid = "+str(offset)+" WHERE market1 = '"+market+"';")
        conn.commit()
        if api_data2['type'] == 'error':
                print('DEBUG:wserror:', api_data2)
        else:
                if offset == 0:
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
maxtime7=0
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
mycursor = conn.execute("DELETE FROM v4client WHERE market1 = '"+market+"';")
conn.commit()
mycursor = conn.execute("INSERT INTO v4client VALUES ('"+market+"', -1);")
conn.commit()
mycursor = conn.execute("SELECT EXISTS(SELECT 1 FROM v4orderbookindex WHERE market1 = '"+market+"');")
recordexists = mycursor.fetchone()[0]
conn.commit()
if recordexists == True:
        mycursor = conn.execute("SELECT index1 FROM v4orderbookindex WHERE market1 = '"+market+"';")
        index1 = mycursor.fetchone()[0] + 1
        conn.commit()
else:
        index1 = 1
print('Table:', 'V4'+market1+'_'+market2+'_'+str(index1))
mycursor = conn.execute("CREATE TABLE V4"+market1+'_'+market2+'_'+str(index1)+" (type varchar(3) NOT NULL, price float NOT NULL, size float NOT NULL, offset1 bigint NOT NULL, datetime varchar(19) NOT NULL, PRIMARY KEY (type, price));")
conn.commit()
mycursor = conn.execute("INSERT INTO v4orderbookindex VALUES ('"+market+"', "+str(index1)+") ON CONFLICT (market1) DO UPDATE SET index1 = "+str(index1)+";")
conn.commit()
if index1 > 10:
        mycursor = conn.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name='V4"+market1+'_'+market2+'_'+str(index1 - 10)+"');")
        recordexists = mycursor.fetchone()[0]
        conn.commit()
        if recordexists == True:
                mycursor = conn.execute("DROP TABLE V4"+market1+'_'+market2+'_'+str(index1 - 10)+";")
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

maxwidthprice = 0
maxwidthsize = 0
zeroaskoffset = 0
zerobidoffset = 0
