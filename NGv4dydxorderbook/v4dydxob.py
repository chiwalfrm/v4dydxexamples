import asyncio
import json
import psycopg
import sys
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado import gen
from tornado.websocket import websocket_connect

from v4dydxobclient import process_message

WSINDEXERURL = 'wss://indexer.dydx.trade/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4testnet.dydx.exchange/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4staging.dydx.exchange/v4/ws'

conn = psycopg.connect("dbname=orderbook user=vmware")

market = sys.argv[1]
api_data = {
        "type": "subscribe",
        "channel": "v4_orderbook",
        "id": market,
}

class Client(object):
        def __init__(self, url, timeout):
                self.url = url
                self.timeout = timeout
                self.ioloop = IOLoop.instance()
                self.ws = None
                self.connect()
                PeriodicCallback(self.keep_alive, 20000).start()
                self.ioloop.start()

        @gen.coroutine
        def connect(self):
                print("DEBUG:trying to connect")
                try:
                        self.ws = yield websocket_connect(self.url)
                except Exception as e:
                        print("DEBUG:connection error")
                else:
                        yield self.ws.write_message(json.dumps(api_data))
                        print("DEBUG:connected")
                        self.run()

        @gen.coroutine
        def run(self):
                loop = asyncio.get_running_loop()
                while True:
                        msg = yield self.ws.read_message()
                        if msg is None:
                                self.ws = None
                                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "WebSocket message failed (connection closed).  Clearing orderbook...")
                                break
                        else:
                                mycursor = conn.execute("UPDATE v4server SET messageid = "+str(json.loads(msg)['message_id'])+" WHERE market1 = '"+market+"';")
                                conn.commit()
                                loop.run_in_executor(pool, process_message, msg)
                pool.shutdown(wait=False, cancel_futures=True)
                exit()

        def keep_alive(self):
                if self.ws is None:
                        self.connect()
                else:
                        self.ws.ping()

def main():
        global pool
        mycursor = conn.execute("DELETE FROM v4server WHERE market1 = '"+market+"';")
        conn.commit()
        mycursor = conn.execute("INSERT INTO v4server VALUES ('"+market+"', -1);")
        conn.commit()
        pool = ProcessPoolExecutor(1)
        client = Client(WSINDEXERURL, 5)

if __name__ == '__main__': # Required by Windows, for example
        main()
