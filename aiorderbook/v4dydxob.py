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

keys1master = ['type', 'connection_id', 'message_id', 'id', 'channel', 'version', 'contents']
keys2master = ['bids', 'asks']

class DydxClientListener(WSListener):
    def __init__(self, pool, market_id, table_name):
        self.pool = pool
        self.market_id = market_id
        self.table_name = table_name
        self.message_queue = asyncio.Queue()  # Queue for channel_data messages
        self.consumer_task = None  # Task for processing queued messages

    def on_ws_connected(self, transport: WSTransport):
        # Start the consumer task for processing queued channel_data messages
        self.consumer_task = asyncio.create_task(self.process_message_queue())
        # Subscribe to the specified market's order book channel (v4)
        subscribe_message = {
            "type": "subscribe",
            "channel": "v4_orderbook",
            "id": self.market_id
        }
        transport.send(WSMsgType.TEXT, json.dumps(subscribe_message).encode())
        print(f"Subscribed to {self.market_id} order book (v4)")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
        # Handle incoming WebSocket messages
        if frame.msg_type == WSMsgType.TEXT:
            try:
                message = frame.get_payload_as_ascii_text()
                parsed_message = json.loads(message)
                if 'type' in parsed_message and parsed_message['type'] in ['subscribed', 'channel_data']:
                    keys1 = list(parsed_message.keys())
                    keys2 = list(parsed_message['contents'].keys())
                    for key in keys1:
                            if key not in keys1master:
                                now_utc = datetime.now(timezone.utc) # Get current UTC time
                                zulu_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # Format with milliseconds (3 decimal places) and Zulu suffix
                                print(f"{zulu_time} DEBUG: {key} not in {keys1master}")
                    for key in keys2:
                            if key not in keys2master:
                                now_utc = datetime.now(timezone.utc) # Get current UTC time
                                zulu_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # Format with milliseconds (3 decimal places) and Zulu suffix
                                print(f"{zulu_time} DEBUG: {key} not in {keys2master}")
                    message_id = parsed_message['message_id']
                    id_ = parsed_message['id']
                    contents = parsed_message['contents']
                    if parsed_message['type'] == 'subscribed':
                        # Process subscribed messages asynchronously
                        asyncio.create_task(self.insert_data(message_id, id_, contents, parsed_message['type']))
                    else:
                        # Queue channel_data messages for serial processing
                        self.message_queue.put_nowait((message_id, id_, contents))
#                    print(json.dumps(parsed_message, indent=2))
                else:
                     print(parsed_message)
            except json.JSONDecodeError:
                print(f"Received non-JSON message: {message}")
            except UnicodeDecodeError:
                print("Received invalid UTF-8 text frame")
            except KeyError as e:
                print(f"Missing key in message: {e}")
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

    async def process_message_queue(self):
        # Consumer task to process channel_data messages serially
        while True:
            try:
                message_id, id_, contents = await self.message_queue.get()
                await self.insert_data(message_id, id_, contents, 'channel_data')
                self.message_queue.task_done()
            except asyncio.CancelledError:
                print("Message queue consumer cancelled")
                break
            except Exception as e:
                print(f"Error processing queued message: {e}")
                break

    async def insert_data(self, message_id, id_, contents, message_type):
        try:
            # Handle 'subscribed' messages (snapshots)
            if message_type == 'subscribed':
                bids = contents.get('bids', [])
                asks = contents.get('asks', [])
                now_utc = datetime.now(timezone.utc) # Get current UTC time
                batch_data = []
                for bid in bids:
                    batch_data.append(('bid', float(bid['price']), float(bid['size']), int(message_id), now_utc))
                for ask in asks:
                    batch_data.append(('ask', float(ask['price']), float(ask['size']), int(message_id), now_utc))
                if batch_data:
                    async with self.pool.acquire() as connection:
                        async with connection.transaction():
                            await connection.executemany(
                                f"INSERT INTO {self.table_name} (type, price, size, offset1, datetime) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
                                batch_data
                            )
                    print(f"DEBUG: Inserted {len(batch_data)} rows into {self.table_name} (subscribed)")
            # Handle 'channel_data' messages (updates)
            elif message_type == 'channel_data':
                bids = contents.get('bids', [])
                asks = contents.get('asks', [])
                now_utc = datetime.now(timezone.utc) # Get current UTC time
                async with self.pool.acquire() as connection:
                    async with connection.transaction():
                        for order_type, orders in [('bid', bids), ('ask', asks)]:
                            for price, size in orders:
                                price = float(price)
                                size = float(size)
                                # Check if row exists
                                exists = await connection.fetchval(
                                    f"SELECT EXISTS (SELECT 1 FROM {self.table_name} WHERE type = $1 AND price = $2)",
                                    order_type, price
                                )
                                if size == 0:
                                    # Delete if size is 0 and row exists
                                    if exists:
                                        await connection.execute(
                                            f"DELETE FROM {self.table_name} WHERE type = $1 AND price = $2",
                                            order_type, price
                                        )
#                                        print(f"Deleted {order_type} at price {price} from {self.table_name}")
                                else:
                                    if exists:
                                        # Update existing row
                                        await connection.execute(
                                            f"UPDATE {self.table_name} SET size = $1, offset1 = $2, datetime = $3 "
                                            f"WHERE type = $4 AND price = $5",
                                            size, int(message_id), now_utc, order_type, price
                                        )
#                                        print(f"Updated {order_type} at price {price} in {self.table_name}")
                                    else:
                                        # Insert new row
                                        await connection.execute(
                                            f"INSERT INTO {self.table_name} (type, price, size, offset1, datetime) "
                                            f"VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
                                            order_type, price, size, int(message_id), now_utc
                                        )
#                                        print(f"Inserted {order_type} at price {price} into {self.table_name}")
        except Exception as e:
            print(f"DEBUG: DB insert error: {e} on contents: {contents}")

    def on_ws_disconnected(self, transport: WSTransport):
        # Handle disconnection
        print(f"DEBUG: on_ws_disconnected called with transport={transport}")
        print("Disconnected: Close details provided via CLOSE frame in on_ws_frame")
        # Cancel the consumer task on disconnection
        if self.consumer_task is not None:
            self.consumer_task.cancel()

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

async def dydx_orderbook_client(market_id):
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
        table_name = f"v4{market1}_{market2}".lower()  # Use v4 prefix for table name
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
                    "type varchar(3) NOT NULL, " #either 'bid' or 'ask'
                    "price float NOT NULL, "
                    "size float NOT NULL, "
                    "offset1 bigint NOT NULL, "
                    "datetime TIMESTAMPTZ NOT NULL, "
                    "PRIMARY KEY (type, price)"
                    ")"
                )
                print(f"Created table {table_name}")
            else:
                await pool.execute(f"TRUNCATE TABLE {table_name}")
                print(f"Truncated table {table_name}")
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
        print("Usage: python dydx_orderbook_websocket.py <market_id>")
        sys.exit(1)
    market_id = sys.argv[1]
    process = psutil.Process(os.getpid())
    print(f"Running updated dYdX WebSocket client with v4_orderbook for {market_id}")
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Optional: use uvloop for better performance
    asyncio.run(dydx_orderbook_client(market_id))
