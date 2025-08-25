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

keys6master = ['type', 'connection_id', 'message_id', 'channel', 'version', 'contents']
keys7master = ['markets', 'oraclePrices', 'trading']
keys8master = ['clobPairId', 'ticker', 'status', 'oraclePrice', 'priceChange24H', 'volume24H', 'trades24H', 'nextFundingRate', 'initialMarginFraction', 'maintenanceMarginFraction', 'openInterest', 'atomicResolution', 'quantumConversionExponent', 'tickSize', 'stepSize', 'stepBaseQuantums', 'subticksPerTick', 'marketType', 'openInterestLowerCap', 'openInterestUpperCap', 'baseOpenInterest', 'defaultFundingRate1H', 'effectiveAt', 'effectiveAtHeight', 'marketId']

def parse_sensor_timestamp(ts: str) -> datetime:
    """Convert '2025-08-24T04:56:26.936Z' into a datetime with tzinfo=UTC"""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

class DydxClientListener(WSListener):
    def __init__(self, pool, table_name):
        self.pool = pool
        self.table_name = table_name

    def on_ws_connected(self, transport: WSTransport):
        # Subscribe to the markets channel (v4)
        subscribe_message = {
            "type": "subscribe",
            "channel": "v4_markets"
        }
        transport.send(WSMsgType.TEXT, json.dumps(subscribe_message).encode())
        print(f"Subscribed to markets (v4)")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
        # Handle incoming WebSocket messages
        if frame.msg_type == WSMsgType.TEXT:
            try:
                message = frame.get_payload_as_ascii_text()
                parsed_message = json.loads(message)
                if 'type' in parsed_message and parsed_message['type'] in ['subscribed', 'channel_data']:
                    keys6 = list(parsed_message.keys())
                    keys7 = list(parsed_message['contents'].keys())
                    for key in keys6:
                        if key not in keys6master:
                            now_utc = datetime.now(timezone.utc) # Get current UTC time
                            zulu_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # Format with milliseconds (3 decimal places) and Zulu suffix
                            print(f"{zulu_time} DEBUG: {key} not in {keys6master}")
                    for key in keys7:
                        if key not in keys7master:
                            now_utc = datetime.now(timezone.utc) # Get current UTC time
                            zulu_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" # Format with milliseconds (3 decimal places) and Zulu suffix
                            print(f"{zulu_time} DEBUG: {key} not in {keys7master}")
                    contents = parsed_message['contents']
                    asyncio.create_task(self.insert_data(contents, parsed_message['type']))
                else:
                    print(parsed_message)
#                print(json.dumps(parsed_message, indent=2))
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

    async def insert_data(self, contents, message_type):
        try:
            # Handle 'subscribed' messages (snapshots)
            if message_type == 'subscribed':
                markets = contents.get('markets', [])
                now_utc = datetime.now(timezone.utc) # Get current UTC time
                batch_data = []
                for key, value in markets.items():
                    market_id = key
                    clobpairid = value.get('clobPairId') or 0
                    ticker = value['ticker']
                    status = value['status']
                    oracleprice = value.get('oraclePrice') or 0
                    pricechange24h = value.get('priceChange24H') or 0
                    volume24h = value.get('volume24H') or 0
                    trades24h = value.get('trades24H') or 0
                    nextfundingrate = value.get('nextFundingRate') or 0
                    initialmarginfraction = value.get('initialMarginFraction') or 0
                    maintenancemarginfraction = value.get('maintenanceMarginFraction') or 0
                    openinterest = value.get('openInterest') or 0
                    atomicresolution = value.get('atomicResolution') or 0
                    quantumconversionexponent = value.get('quantumConversionExponent') or 0
                    ticksize = value.get('tickSize') or 0
                    stepsize = value.get('stepSize') or 0
                    stepbasequantums = value.get('stepBaseQuantums') or 0
                    subtickspertick = value.get('subticksPerTick') or 0
                    markettype = value['marketType']
                    openinterestlowercap = value.get('openInterestLowerCap') or 0
                    openinterestuppercap = value.get('openInterestUpperCap') or 0
                    baseopeninterest = value.get('baseOpenInterest') or 0
                    defaultfundingrate1h = value.get('defaultFundingRate1H') or 0
                    batch_data.append((market_id, int(clobpairid), ticker, status, float(oracleprice), float(pricechange24h), float(volume24h), int(trades24h), float(nextfundingrate), float(initialmarginfraction), float(maintenancemarginfraction), float(openinterest), int(atomicresolution), int(quantumconversionexponent), float(ticksize), float(stepsize), int(stepbasequantums), int(subtickspertick), markettype, int(openinterestlowercap), int(openinterestuppercap), float(baseopeninterest), float(defaultfundingrate1h), None, None, None, now_utc))
                if batch_data:
                    async with self.pool.acquire() as connection:
                        async with connection.transaction():
                            await connection.executemany(
                                f"INSERT INTO {self.table_name} (market_id, clobpairid, ticker, status, oracleprice, pricechange24h, volume24h, trades24h, nextfundingrate, initialmarginfraction, maintenancemarginfraction, openinterest, atomicresolution, quantumconversionexponent, ticksize, stepsize, stepbasequantums, subtickspertick, markettype, openinterestlowercap, openinterestuppercap, baseopeninterest, defaultfundingrate1h, effectiveat, effectiveatheight, marketid, datetime) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27) ON CONFLICT DO NOTHING",
                                batch_data
                            )
                    print(f"DEBUG: Inserted {len(batch_data)} rows into {self.table_name} (subscribed)")
            # Handle 'channel_data' messages (updates)
            elif message_type == 'channel_data':
                # Handle oraclePrices or trading updates
                update_data = contents.get('oraclePrices', contents.get('trading', {}))
                if update_data:
                    now_utc = datetime.now(timezone.utc) # Get current UTC time
                    async with self.pool.acquire() as connection:
                        async with connection.transaction():
                            updated_count = 0
                            for market_id, data in update_data.items():
                                # Validate fields against keys8master
                                valid_fields = [k for k in data.keys() if k in keys8master]
                                if not valid_fields:
                                    print(f"Skipping {market_id}: No valid fields in update")
                                    continue
                                # Map message fields to column names
                                column_map = {
                                    'clobPairId': 'clobpairid',
                                    'oraclePrice': 'oracleprice',
                                    'priceChange24H': 'pricechange24h',
                                    'volume24H': 'volume24h',
                                    'trades24H': 'trades24h',
                                    'nextFundingRate': 'nextfundingrate',
                                    'initialMarginFraction': 'initialmarginfraction',
                                    'maintenanceMarginFraction': 'maintenancemarginfraction',
                                    'openInterest': 'openinterest',
                                    'atomicResolution': 'atomicresolution',
                                    'quantumConversionExponent': 'quantumconversionexponent',
                                    'tickSize': 'ticksize',
                                    'stepSize': 'stepsize',
                                    'stepBaseQuantums': 'stepbasequantums',
                                    'subticksPerTick': 'subtickspertick',
                                    'marketType': 'markettype',
                                    'openInterestLowerCap': 'openinterestlowercap',
                                    'openInterestUpperCap': 'openinterestuppercap',
                                    'baseOpenInterest': 'baseopeninterest',
                                    'defaultFundingRate1H': 'defaultfundingrate1h',
                                    'effectiveAt': 'effectiveat',
                                    'effectiveAtHeight': 'effectiveatheight',
                                    'marketId': 'marketid'
                                }
                                # Build dynamic UPDATE query
                                set_clauses = []
                                values = []
                                for field in valid_fields:
                                    column = column_map.get(field, field.lower())
                                    value = data[field]
                                    if value is None:
                                        print(f"DEBUG: Excluding field {field} for {market_id}: Value is None")
                                        continue
                                    # Convert types based on column
                                    if column in ['oracleprice', 'pricechange24h', 'volume24h', 'nextfundingrate', 'initialmarginfraction', 'maintenancemarginfraction', 'openinterest', 'ticksize', 'stepsize', 'baseopeninterest', 'defaultfundingrate1h']:
                                        value = float(value)
                                    elif column in ['clobpairid', 'trades24h', 'atomicresolution', 'quantumconversionexponent', 'stepbasequantums', 'subtickspertick', 'openinterestlowercap', 'openinterestuppercap', 'effectiveatheight', 'marketid']:
                                        value = int(value)
                                    elif column == 'effectiveat':
                                        try:
                                            value = parse_sensor_timestamp(value)
                                        except ValueError as e:
                                            print(f"Skipping {market_id}: Invalid effectiveAt format {value}")
                                            continue
                                    set_clauses.append(f"{column} = ${len(values) + 1}")
                                    values.append(value)
                                # Always update datetime
                                set_clauses.append(f"datetime = ${len(values) + 1}")
                                values.append(now_utc)
                                # Add market_id for WHERE clause
                                values.append(market_id)
                                # Execute UPDATE
                                await connection.execute(
                                    f"""
                                    UPDATE {self.table_name}
                                    SET {', '.join(set_clauses)}
                                    WHERE market_id = ${len(values)}
                                    """,
                                    *values
                                )
                                updated_count += 1
#                            print(f"Updated {updated_count} rows in {self.table_name} ({'oraclePrices' if 'oraclePrices' in contents else 'trading'})")
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

async def dydx_markets_client():
    # Create PostgreSQL connection pool (asyncpg, using Unix socket)
    pool = await asyncpg.create_pool(database='orderbook', user='vmware')

    # Start the heartbeat task
    heartbeat_task = asyncio.create_task(heartbeat())

    table_name = "v4markets"

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
                    "market_id varchar(255) NOT NULL, "
                    "clobpairid int NOT NULL, "
                    "ticker varchar(255) NOT NULL, "
                    "status varchar(255) NOT NULL, "
                    "oracleprice float NOT NULL, "
                    "pricechange24h float NOT NULL, "
                    "volume24h float NOT NULL, "
                    "trades24h int NOT NULL, "
                    "nextfundingrate float NOT NULL, "
                    "initialmarginfraction float NOT NULL, "
                    "maintenancemarginfraction float NOT NULL, "
                    "openinterest float NOT NULL, "
                    "atomicresolution int NOT NULL, "
                    "quantumconversionexponent int NOT NULL, "
                    "ticksize float NOT NULL, "
                    "stepsize float NOT NULL, "
                    "stepbasequantums int NOT NULL, "
                    "subtickspertick int NOT NULL, "
                    "markettype varchar(255) NOT NULL, "
                    "openinterestlowercap int NOT NULL, "
                    "openinterestuppercap int NOT NULL, "
                    "baseopeninterest float NOT NULL, "
                    "defaultfundingrate1h float NOT NULL, "
                    "effectiveat TIMESTAMPTZ, "
                    "effectiveatheight int, "
                    "marketid int, "
                    "datetime TIMESTAMPTZ NOT NULL, "
                    "PRIMARY KEY (clobpairid)"
                    ")"
                )
                print(f"Created table {table_name}")
            else:
                await pool.execute(f"TRUNCATE TABLE {table_name}")
                print(f"Truncated table {table_name}")
        except Exception as e:
            print(f"Error managing table {table_name}: {e}")

        try:
            # Create WebSocket client, passing pool and table_name to listener
            transport, client = await ws_connect(lambda: DydxClientListener(pool, table_name), url)
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
    process = psutil.Process(os.getpid())
    print("Running updated dYdX WebSocket client with v4_markets")
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Optional: use uvloop for better performance
    asyncio.run(dydx_markets_client())
