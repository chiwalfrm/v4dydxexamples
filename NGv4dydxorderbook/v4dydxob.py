import asyncio
import json
import sys
import websockets
from datetime import datetime
from multiprocessing import Pool, cpu_count

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

async def wsrun(uri, pool):
        async for websocket in websockets.connect(uri):
                try:
                        await websocket.send(json.dumps(api_data))
                        while True:
                                pool.apply_async(process_message, args=(await websocket.recv(),))
                except Exception as error:
                        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "WebSocket message failed (%s).  Clearing orderbook..." % error)
                        pool.apply_async(process_message, args=(json.dumps({'message_id': -1}),))
                        continue

def main():
        pool = Pool(1)
        asyncio.run(wsrun(WSINDEXERURL, pool))

if __name__ == '__main__': # Required by Windows, for example
        main()
