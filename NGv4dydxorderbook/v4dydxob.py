import asyncio
import json
import sys
import websockets
from datetime import datetime
from multiprocessing import Pool, cpu_count
from time import time

from v4dydxobclient import process_message

WSINDEXERURL = 'wss://indexer.dydx.trade/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4testnet.dydx.exchange/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4staging.dydx.exchange/v4/ws'

if len(sys.argv) < 2:
        market = 'BTC-USD'
else:
        market = sys.argv[1]
api_data = {
        "type": "subscribe",
        "channel": "v4_orderbook",
        "id": market,
}

async def wsrun(uri, pool, restartflag):
        async for websocket in websockets.connect(uri):
                await websocket.send(json.dumps(api_data))
                if restartflag == 1:
                        pool.apply_async(process_message, args=(json.dumps({'message_id': -1}),))
                while True:
                        pool.apply_async(process_message, args=(await websocket.recv(),))

def main():
        maxtime9 = 0
        restartflag = 0
        pool = Pool(1)
        while True:
                try:
                        asyncio.run(wsrun(WSINDEXERURL, pool, restartflag))
                except Exception as error:
                        time1=time()
                        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "WebSocket message failed (%s).  Clearing orderbook..." % error)
                        restartflag = 1
                        time2=time()
                        if time2 - time1 > maxtime9:
                                maxtime9 = time2 - time1
                                print('main(server): mv new maximum elapsed time:', '{:.2f}'.format(maxtime9))

if __name__ == '__main__': # Required by Windows, for example
        main()
