import asyncio
import json
import os
import psutil
import sys
import uvloop
import asyncpg
from datetime import datetime, timezone
from picows import ws_connect, WSFrame, WSTransport, WSListener, WSMsgType

WSINDEXERURL = 'wss://indexer.dydx.trade/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4testnet.dydx.exchange/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4staging.dydx.exchange/v4/ws'

keys3master = ['type', 'connection_id', 'message_id', 'channel', 'id', 'version', 'contents']
keys4master = ['trades']
keys5master = ['id', 'size', 'price', 'side', 'createdAt', 'type', 'createdAtHeight']

def parse_sensor_timestamp(ts: str) -> datetime:
    """Convert '2025-08-24T04:56:26.936Z' into a datetime with tzinfo=UTC"""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

class DydxClientListener(WSListener):
    def __init__(self, pool, market_id, table_name):
        self.pool = pool
        self.market_id = market_id
        self.table_name = table_name

    def on_ws_connected(self, transport: WSTransport):
        # Subscribe to the specified market's trades channel (v4)
        subscribe_message = {
            "type": "subscribe",
            "channel": "v4_trades",
            "id": self.market_id
        }
        transport.send(WSMsgType.TEXT, json.dumps(subscribe_message).encode())
        print(f"Subscribed to {self.market_id} trades (v4)")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
        # Handle incoming WebSocket messages
        if frame.msg_type == WSMsgType.TEXT:
            try:
                message = frame.get_payload_as_ascii_text()
                parsed_message = json.loads(message)
                if 'type' in parsed_message and parsed_message["type"] == "error":
                    print(parsed_message)
                    raise msgerror("msgerror")

                if 'type' in parsed_message and parsed_message['type'] in ['subscribed', 'channel_data']:
                    keys3 = list(parsed_message.keys())
                    keys4 = list(parsed_message['contents'].keys())
                    keys5 = parsed_message['contents']['trades']
                    for key in keys3:
                            if key not in keys3master:
                                now_utc = datetime.now(timezone.utc) # Get current UTC time
                                zulu_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # Format with milliseconds (3 decimal places) and Zulu suffix
                                print(f"{zulu_time} DEBUG: {key} not in {keys3master}")
                    for key in keys4:
                            if key not in keys4master:
                                now_utc = datetime.now(timezone.utc) # Get current UTC time
                                zulu_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # Format with milliseconds (3 decimal places) and Zulu suffix
                                print(f"{zulu_time} DEBUG: {key} not in {keys4master}")
                    for key in keys5:
                            bkeylist= list(key.keys())
                            for bkey in bkeylist:
                                if bkey not in keys5master:
                                     now_utc = datetime.now(timezone.utc) # Get current UTC time
                                     zulu_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # Format with milliseconds (3 decimal places) and Zulu suffix
                                     print(f"{zulu_time} DEBUG: {bkey} not in {keys5master}")
                    id_ = parsed_message['id']
                    contents = parsed_message['contents']
                    asyncio.create_task(self.insert_data(id_, contents, parsed_message['type']))
                else:
                    print(parsed_message)
#                print(json.dumps(parsed_message, indent=2))
            except json.JSONDecodeError:
                print(f"Received non-JSON message: {message}")
            except UnicodeDecodeError:
                print("Received invalid UTF-8 text frame")
            except KeyError as e:
                print(f"Missing key in message: {e}")
            except msgerror as e:
                print(f"Error: Exception {e}")
            except Exception as e:
                print(f"Error: Exception {e}")
        elif frame.msg_type == WSMsgType.CLOSE:
            # Handle CLOSE frame
            close_code = frame.get_close_code()
            close_message = frame.get_close_message()
            close_message_str = close_message.decode('utf-8', errors='ignore') if close_message is not None else "No close message"
            print(f"Received CLOSE frame: code={close_code}, message={close_message_str}")
        else:
            print(f"Received non-text frame: {frame.msg_type}")

    async def insert_data(self, id_, contents, message_type):
        try:
            # Handle 'subscribed' messages (snapshots)
            if message_type == 'subscribed':
                trades = contents.get('trades', [])
                now_utc = datetime.now(timezone.utc) # Get current UTC time
                batch_data = []
                for trade in trades:
                    tradeid = trade['id']
                    tradesize = trade['size']
                    tradeprice = trade['price']
                    tradeside = trade['side']
                    tradecreatedat = trade['createdAt']
                    tradetype = trade['type']
                    if 'createdAtHeight' in trade.keys():
                        tradecreatedatheight = trade['createdAtHeight']
                    else:
                        tradecreatedatheight = 0
                    batch_data.append((tradeid, float(tradesize), float(tradeprice), tradeside, parse_sensor_timestamp(tradecreatedat), tradetype, int(tradecreatedatheight), now_utc))
                if batch_data:
                    async with self.pool.acquire() as connection:
                        async with connection.transaction():
                            await connection.executemany(
                                f"INSERT INTO {self.table_name} (id, size, price, side, createdat, type, createdatheight, datetime) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING",
                                batch_data
                            )
                    print(f"DEBUG: Inserted {len(batch_data)} rows into {self.table_name} (subscribed)")
            # Handle 'channel_data' messages (updates)
            elif message_type == 'channel_data':
                trades = contents.get('trades', [])
                now_utc = datetime.now(timezone.utc) # Get current UTC time
                async with self.pool.acquire() as connection:
                    async with connection.transaction():
                        for trade in trades:
                            tradeid = trade['id']
                            tradesize = trade['size']
                            tradeprice = trade['price']
                            tradeside = trade['side']
                            tradecreatedat = trade['createdAt']
                            tradetype = trade['type']
                            if 'createdAtHeight' in trade.keys():
                                tradecreatedatheight = trade['createdAtHeight']
                            else:
                                tradecreatedatheight = 0
                            # Insert new row
                            await connection.execute(
                                f"INSERT INTO {self.table_name} (id, size, price, side, createdat, type, createdatheight, datetime) "
                                f"VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING",
                                tradeid, float(tradesize), float(tradeprice), tradeside, parse_sensor_timestamp(tradecreatedat), tradetype, int(tradecreatedatheight), now_utc
                            )
#                            print(f"Inserted {tradeid} into {self.table_name}")
        except Exception as e:
            print(f"DEBUG: DB insert error: {e} on contents: {contents}")

    def on_ws_disconnected(self, transport: WSTransport):
        # Handle disconnection
        print(f"DEBUG: on_ws_disconnected called with transport={transport}")
        print("Disconnected: Close details provided via CLOSE frame in on_ws_frame")

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

async def dydx_trades_client(market_id):
    # Create PostgreSQL connection pool (asyncpg, using Unix socket)
    pool = await asyncpg.create_pool(database='orderbook', user='vmware')

    # Start the heartbeat task
    heartbeat_task = asyncio.create_task(heartbeat())

    # Split market_id into market1 and market2
    try:
        # Split on the last hyphen to separate base and quote (e.g., USD)
        market_base, market2 = market_id.rsplit('-', 1)
        # Handle commas in market_base by taking the part before the first comma
        if ',' in market_base:
            market1 = market_base.split(',', 1)[0]
        else:
            market1 = market_base
        table_name = f"v4trades{market1}_{market2}".lower()  # Use v4 prefix for table name
    except ValueError:
        print(f"Invalid market_id format: {market_id}. Expected format: 'BASE-QUOTE' (e.g., 'BTC-USD')")
        await pool.close()
        return
    except Exception as e:
        print(f"Error: Exception {e}")
        await pool.close()
        return

    # Updated dYdX WebSocket endpoint (v4)
    url = WSINDEXERURL
    max_reconnect_attempts = 5
    reconnect_delay = 5  # seconds

    for attempt in range(1, max_reconnect_attempts + 1):
        now_utc = datetime.now(timezone.utc) # Get current UTC time
        zulu_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # Format with milliseconds (3 decimal places) and Zulu suffix
        print(f"{zulu_time} Connection attempt {attempt}/{max_reconnect_attempts}")
        # Check if table exists; create if not, truncate if yes
        try:
            exists = await pool.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND upper(table_name) = upper($1))",
                table_name
            )
            if not exists:
                await pool.execute(
                    f"CREATE TABLE {table_name} ("
                    "id varchar(255) NOT NULL, "
                    "size float NOT NULL, "
                    "price float NOT NULL, "
                    "side varchar(255) NOT NULL, "
                    "createdat TIMESTAMPTZ NOT NULL, "
                    "type varchar(255) NOT NULL, "
                    "createdatheight int NOT NULL, "
                    "datetime TIMESTAMPTZ NOT NULL, "
                    "PRIMARY KEY (id, createdat)"
                    ")"
                )
                await pool.execute(
                    f"CREATE INDEX idx_{table_name} ON {table_name} (datetime DESC)"
                )
                print(f"Created table {table_name} and index idx_{table_name}")
#            else:
#                await pool.execute(f"TRUNCATE TABLE {table_name}")
#                print(f"Truncated table {table_name}")
        except Exception as e:
            print(f"Error managing table {table_name}: {e}")

        try:
            # Create WebSocket client, passing pool, market_id, and table_name to listener
            transport, client = await ws_connect(lambda: DydxClientListener(pool, market_id, table_name), url)
            # Wait for disconnection
            await transport.wait_disconnected()
        except Exception as e:
            print(f"Error during connection: {e}")
        # Reconnection logic
        if attempt < max_reconnect_attempts:
            print(f"Reconnecting in {reconnect_delay} seconds...")
            await asyncio.sleep(reconnect_delay)
        else:
            print("Max reconnection attempts reached. Exiting.")
            break

    # Cancel the heartbeat task
    heartbeat_task.cancel()
    # Close the pool after all attempts
    await pool.close()

# Run the async client
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python dydx_trades_websocket.py <market_id>")
        sys.exit(1)
    market_id = sys.argv[1]
    process = psutil.Process(os.getpid())
    print(f"Running updated dYdX WebSocket client with v4_trades for {market_id}")
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Optional: use uvloop for better performance
    asyncio.run(dydx_trades_client(market_id))
