import asyncio
import json
import websockets
import argparse
import requests
from typing import Dict
from decimal import Decimal
from aiohttp import web
from datetime import datetime, timezone
import os
import psutil
import redis

WSINDEXERURL = 'wss://indexer.dydx.trade/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4testnet.dydx.exchange/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4staging.dydx.exchange/v4/ws'
INDEXERURL = 'https://indexer.dydx.trade/v4'
#INDEXERURL = 'https://indexer.v4testnet.dydx.exchange/v4'
#INDEXERURL = 'https://indexer.v4staging.dydx.exchange/v4'

bids: Dict[str, str] = {}  # Global for simplicity; price_str: size_str
asks: Dict[str, str] = {}  # Global for simplicity; price_str: size_str

def parse_args():
    parser = argparse.ArgumentParser(description="dYdX v4 Orderbook WebSocket Server")
    parser.add_argument("--market", type=str, default="BTC-USD", help="Market to subscribe to (e.g., BTC-USD, PEPE-USD) (default: BTC-USD)")
    return parser.parse_args()

def get_clob_pair_id(market: str) -> int:
    #first, check redis
    r = redis.Redis(host='localhost', port=6379, db=0)
    if r.exists(f"{market}-clobpairid"):
        clob_pair_id = int(r.get(f"{market}-clobpairid"))
        print(f"Got clob_pair_id {clob_pair_id} from redis")
        return clob_pair_id
    try:
        # Using synchronous requests for this initial call, as it's a one-time setup
        import requests
        response = requests.get(f"{INDEXERURL}/perpetualMarkets", timeout=5)
        response.raise_for_status()
        data = response.json()
        markets = data.get("markets", {})
        if market not in markets:
            raise ValueError(f"Market {market} not found in API response")
        clob_pair_id = int(markets[market]["clobPairId"])
        r.set(f"{market}-clobpairid", int(clob_pair_id))
        print(f"Got clob_pair_id {clob_pair_id} from indexer")
        return clob_pair_id
    except (requests.exceptions.RequestException, ValueError, KeyError) as e:
        print(f"Error fetching clobPairId for {market}: {e}")
        raise

async def websocket_task(market: str):
    url = WSINDEXERURL

    while True:  # Reconnection loop
        first_message = True
        now_utc = datetime.now(timezone.utc) # Get current UTC time
        zulu_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # Format with milliseconds (3 decimal places) and Zulu suffix
        print(f"{zulu_time} Running updated dYdX WebSocket client with v4_orderbook for {market}")
        try:
            async with websockets.connect(url, ping_interval=None, ping_timeout=20) as ws:
                subscribe_msg = {
                    "type": "subscribe",
                    "channel": "v4_orderbook",
                    "id": market
                }
                await ws.send(json.dumps(subscribe_msg))

                global bids, asks
                while True:
                    try:
                        msg = await ws.recv()
                        data = json.loads(msg)

                        if first_message:
                            print(data)
                            first_message = False

                        if data["type"] == "connected":
                            continue

                        if data["type"] == "error":
                            print(data)
                            raise msgerror("msgerror")

                        contents = data.get("contents", {})
                        if data["type"] == "subscribed" or data["type"] == "channel_data":
                            for side, book in [("bids", bids), ("asks", asks)]:
                                if side in contents:
                                    for entry in contents[side]:
                                        if isinstance(entry, dict):
                                            price_str = entry["price"]
                                            size_str = entry["size"]
                                        else:  # list or tuple
                                            price_str = entry[0]
                                            size_str = entry[1]
                                            # Ignore offset if present (entry[2])

                                        if Decimal(size_str) == 0:
                                            book.pop(price_str, None)
                                        else:
                                            book[price_str] = size_str

                    except json.JSONDecodeError:
                        print(f"Failed to parse message: {msg}")
                    except websockets.exceptions.ConnectionClosed:
                        print("WebSocket connection closed, attempting to reconnect...")
                        break  # Exit inner loop to reconnect
                    except msgerror as e:
                        print(f"Error processing message: {e}")
                    except Exception as e:
                        print(f"Error processing message: {e}")

        except Exception as e:
            print(f"Error connecting to WebSocket: {e}")
        print("Reconnecting in 5 seconds...")
        await asyncio.sleep(5)  # Wait before reconnecting

async def get_orderbook(request):
    global bids, asks

    # Sort bids descending by price
    sorted_bids = sorted(
        [[price, bids[price]] for price in bids],
        key=lambda x: Decimal(x[0]),
        reverse=True
    )

    # Sort asks ascending by price
    sorted_asks = sorted(
        [[price, asks[price]] for price in asks],
        key=lambda x: Decimal(x[0])
    )

    snapshot = {
        "market": request.app['market'],  # Include market from app context
        "bids": sorted_bids,
        "asks": sorted_asks
    }

    return web.json_response(snapshot)

async def http_server(market: str):
    clob_pair_id = get_clob_pair_id(market)
    port = 10000 + clob_pair_id

    app = web.Application()
    app['market'] = market  # Store market in app context
    app.add_routes([web.get('/orderbook', get_orderbook)])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    print(f"Starting HTTP server on port {port} for market {market}")
    await site.start()

    # Keep running indefinitely
    await asyncio.Event().wait()

async def heartbeat():
    # Print "I am still alive" every 60 seconds
    while True:
        try:
#            print("I am still alive")
            now_utc = datetime.now(timezone.utc) # Get current UTC time
            zulu_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # Format with milliseconds (3 decimal places) and Zulu suffix
            print(f"{zulu_time} DEBUG: memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            print("Heartbeat task cancelled")
            break
        except Exception as e:
            print(f"heartbeat error: {e}")
            break

async def start_server(market: str):
    # Start the heartbeat task
    heartbeat_task = asyncio.create_task(heartbeat())

    ws_task = asyncio.create_task(websocket_task(market))
    http_task = asyncio.create_task(http_server(market))
    await asyncio.gather(ws_task, http_task)

if __name__ == "__main__":
    args = parse_args()
    process = psutil.Process(os.getpid())
    asyncio.run(start_server(args.market))
