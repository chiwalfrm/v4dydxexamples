import asyncio
import json
import websockets
from typing import Dict
from aiohttp import web
from datetime import datetime, timezone
import os
import psutil

WSINDEXERURL = 'wss://indexer.dydx.trade/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4testnet.dydx.exchange/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4staging.dydx.exchange/v4/ws'

# Global dictionary to store market data
markets: Dict[str, Dict] = {}  # market: {parameters}

async def websocket_task():
    url = WSINDEXERURL

    while True:  # Reconnection loop
        first_message = True
        now_utc = datetime.now(timezone.utc) # Get current UTC time
        zulu_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # Format with milliseconds (3 decimal places) and Zulu suffix
        print(f"{zulu_time} Running updated dYdX WebSocket client with v4_markets")
        try:
            async with websockets.connect(url, ping_interval=None, ping_timeout=20) as ws:
                subscribe_msg = {
                    "type": "subscribe",
                    "channel": "v4_markets"
                }
                await ws.send(json.dumps(subscribe_msg))

                global markets
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

                        if data["type"] == "subscribed":
                            # Initialize markets with all parameters from contents.markets
                            markets.update(data.get("contents", {}).get("markets", {}))

                        elif data["type"] == "channel_data":
                            contents = data.get("contents", {})
                            if "oraclePrices" in contents:
                                # Update oraclePrice, effectiveAt, effectiveAtHeight, marketId for a market
                                for market, params in contents["oraclePrices"].items():
                                    if market in markets:
                                        markets[market].update({
                                            "oraclePrice": params["oraclePrice"],
                                            "effectiveAt": params["effectiveAt"],
                                            "effectiveAtHeight": params["effectiveAtHeight"],
                                            "marketId": params["marketId"]
                                        })
                            elif "trading" in contents:
                                # Update trading-related parameters for specified markets
                                for market, params in contents["trading"].items():
                                    if market in markets:
                                        markets[market].update(params)

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

async def get_markets(request):
    global markets
    market = request.query.get("market")
    if market:
        return web.json_response(markets.get(market, {}))
    return web.json_response({"markets": markets})

async def http_server():
    port = 10999

    app = web.Application()
    app.add_routes([web.get('/markets', get_markets)])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    print(f"Starting HTTP server on port {port}")
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

async def start_server():
    # Start the heartbeat task
    heartbeat_task = asyncio.create_task(heartbeat())

    ws_task = asyncio.create_task(websocket_task())
    http_task = asyncio.create_task(http_server())
    await asyncio.gather(ws_task, http_task)

if __name__ == "__main__":
    process = psutil.Process(os.getpid())
    asyncio.run(start_server())
