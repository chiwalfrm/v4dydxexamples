import json
import logging, logging.handlers
import os
import sys
from datetime import datetime

WSINDEXERURL = 'wss://indexer.dydx.trade/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4testnet.dydx.exchange/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4staging.dydx.exchange/v4/ws'

def process_message(message):
        api_data2 = json.loads(message)
        if api_data2['type'] == 'error':
                print('DEBUG:wserror:', api_data2)
        else:
                if api_data2['message_id'] == 0:
                        print(api_data2)
                else:
                        if isinstance(api_data2['contents'], dict):
                                logger.info("{'timestamp': '"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'} (DICT)")
                                logger.info(api_data2['contents'])
                        elif isinstance(api_data2['contents'], list):
                                for item in api_data2['contents']:
                                        logger.info("{'timestamp': '"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"'} (LISTITEM)")
                                        logger.info(item)

if len(sys.argv) < 2:
        print("ERROR: Must specify address in this form <address>/<subaccountnumber>.")
        exit()

address = sys.argv[1]
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+' v4dydxsubaccount.py')
logger = logging.getLogger("Rotating Log")
logger.setLevel(logging.INFO)
if sys.platform == "linux" or sys.platform == "linux2":
        # linux
        ramdiskpath = '/mnt/ramdiskv4dydxl2bot2'
        if os.access(ramdiskpath, os.W_OK) != True:
                print('Warning:', ramdiskpath, 'is not writable.  Using /tmp')
                ramdiskpath = '/tmp'

elif sys.platform == "darwin":
        # OS X
        ramdiskpath = '/Volumes/RAMDiskv4dydxl2bot2'
        if os.access(ramdiskpath, os.W_OK) != True:
                print('Warning:', ramdiskpath, 'is not writable.  Using /tmp')
                ramdiskpath = '/tmp'

handler = logging.handlers.RotatingFileHandler(ramdiskpath+'/v4dydxsubaccount_'+address.replace('/', '_')+'.log',
        maxBytes = 2097152,
        backupCount = 4
)
logger.addHandler(handler)

if os.path.isdir(ramdiskpath) == False:
        print('Error: Ramdisk', ramdiskpath, 'not mounted')
        sys.exit()
if os.path.ismount(ramdiskpath) == False:
        print('Warning:', ramdiskpath, 'is not a mount point')
