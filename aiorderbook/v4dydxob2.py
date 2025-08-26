import argparse
import requests
import time
import os
import asyncio
import asyncpg
from typing import List, Tuple, Dict
from decimal import Decimal
import datetime

INDEXERURL = 'https://indexer.dydx.trade/v4'
#INDEXERURL = 'https://indexer.v4testnet.dydx.exchange/v4'
#INDEXERURL = 'https://indexer.v4staging.dydx.exchange/v4'

def parse_args():
    parser = argparse.ArgumentParser(description="dYdX v4 Orderbook Client")
    parser.add_argument("--ip", type=str, default="localhost", help="Server IP address (default: localhost)")
    parser.add_argument("--market", type=str, default="BTC-USD", help="Market to fetch orderbook for (e.g., BTC-USD, PEPE-USD) (default: BTC-USD)")
    parser.add_argument("--depth", type=int, default=10, help="Number of orderbook rows to display (default: 10)")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between requests in seconds when looping (default: 1.0)")
    return parser.parse_args()

def get_clob_pair_id(market: str) -> int:
    try:
        response = requests.get(f"{INDEXERURL}/perpetualMarkets", timeout=5)
        response.raise_for_status()
        data = response.json()
        markets = data.get("markets", {})
        if market not in markets:
            raise ValueError(f"Market {market} not found in API response")
        clob_pair_id = int(markets[market]["clobPairId"])
        return clob_pair_id
    except (requests.exceptions.RequestException, ValueError, KeyError) as e:
        print(f"Error fetching clobPairId for {market}: {e}")
        raise

async def get_latest_trade(pool: asyncpg.Pool, market: str) -> dict:
    market_part, base_part = market.split('-')
    table_name = f"v4trades{market_part.lower()}_{base_part.lower()}"
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT createdat, createdatheight, price, side, size "
                f"FROM {table_name} ORDER BY datetime DESC LIMIT 1"
            )
            if row:
                return {
                    "createdat": row["createdat"].strftime("%Y-%m-%dT%H:%M:%S"),
                    "createdatheight": row["createdatheight"],
                    "price": float(row["price"]),
                    "side": row["side"],
                    "size": float(row["size"])
                }
            return {}
    except Exception as e:
        print(f"Error querying latest trade from {table_name}: {e}")
        return {}

async def get_market_data(pool: asyncpg.Pool, market: str) -> dict:
    query = (
        f"SELECT pricechange24h, nextfundingrate, openinterest, trades24h, volume24h, "
        f"effectiveat, effectiveatheight, marketid, oracleprice, datetime "
        f"FROM v4markets WHERE market_id = $1"
    )
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, market)
            if row:
                return {
                    "pricechange24h": row["pricechange24h"],
                    "nextfundingrate": row["nextfundingrate"],
                    "openinterest": row["openinterest"],
                    "trades24h": row["trades24h"],
                    "volume24h": row["volume24h"],
                    "effectiveat": row["effectiveat"].strftime("%Y-%m-%dT%H:%M:%S") if row["effectiveat"] else "",
                    "effectiveatheight": row["effectiveatheight"] if row["effectiveatheight"] is not None else 0,
                    "marketid": row["marketid"] if row["marketid"] is not None else 0,
                    "oracleprice": row["oracleprice"],
                    "datetime": row["datetime"].strftime("%Y-%m-%dT%H:%M:%S")
                }
            return {}
    except Exception as e:
        print(f"Error querying market data for market_id={market}: {e}")
        return {}

def resolve_crossed_order_book(bid_list: List[Tuple[Decimal, Decimal]], ask_list: List[Tuple[Decimal, Decimal]]) -> Tuple[List[Tuple[Decimal, Decimal]], List[Tuple[Decimal, Decimal]]]:
    bid_list = bid_list.copy()
    ask_list = ask_list.copy()
    while bid_list and ask_list and bid_list[0][0] >= ask_list[0][0]:
        bid_price, bid_size = bid_list[0]
        ask_price, ask_size = ask_list[0]
        if bid_size > ask_size:
            bid_list[0] = (bid_price, bid_size - ask_size)
            ask_list.pop(0)
        elif bid_size < ask_size:
            ask_list[0] = (ask_price, ask_size - bid_size)
            bid_list.pop(0)
        else:
            ask_list.pop(0)
            bid_list.pop(0)
    return bid_list, ask_list

def print_order_book(bids: List[Tuple[str, str]], asks: List[Tuple[str, str]], market: str, depth: int, latest_trade: dict, market_data: dict, runtime: str):
    # Convert to Decimal for sorting and cross-resolution
    bid_decimals = [(Decimal(price), Decimal(size)) for price, size in bids]
    ask_decimals = [(Decimal(price), Decimal(size)) for price, size in asks]

    # Resolve crossed orderbook
    resolved_bids, resolved_asks = resolve_crossed_order_book(bid_decimals, ask_decimals)

    # Create price maps to preserve original strings
    bid_price_map: Dict[Decimal, str] = {Decimal(price): price for price, _ in bids}
    ask_price_map: Dict[Decimal, str] = {Decimal(price): price for price, _ in asks}

    # Collect displayed prices and sizes for dynamic widths and precision
    price_strs: List[str] = []
    size_strs: List[str] = []
    for lst, price_map in [(resolved_bids, bid_price_map), (resolved_asks, ask_price_map)]:
        for price_dec, size_dec in lst[:depth]:
            price_str = price_map.get(price_dec, str(price_dec))
            size_str = str(size_dec)
            price_strs.append(price_str)
            size_strs.append(size_str)

    # Find maximum decimal places in displayed prices
    max_decimals = 0
    for price_str in price_strs:
        try:
            fractional_part = price_str.split('.')[-1] if '.' in price_str else ''
            max_decimals = max(max_decimals, len(fractional_part.rstrip('0')))
        except ValueError:
            continue

    # Format prices with space padding for alignment
    formatted_price_strs: List[str] = []
    for price_str in price_strs:
        try:
            if '.' in price_str:
                integer_part, fractional_part = price_str.split('.')
                fractional_part = fractional_part.rstrip('0')
                fractional_part = fractional_part + ' ' * (max_decimals - len(fractional_part))
                formatted_price_str = f"{integer_part}.{fractional_part}"
            else:
                formatted_price_str = price_str + ' ' * (max_decimals + 1)  # +1 for decimal point
            formatted_price_strs.append(formatted_price_str)
        except ValueError:
            formatted_price_strs.append(price_str)

    # Calculate maximum string lengths for alignment, ensuring minimum width of 4
    max_price_len = max(max((len(p) for p in formatted_price_strs), default=0), 4)  # Minimum len("BidP")
    max_size_len = max(max((len(s) for s in size_strs), default=0), 4)  # Minimum len("BidQ")

    col_width = max_price_len + 3 + max_size_len  # For " | "

    print(f"OrderBook for {market}:")
    if latest_trade:
        print(f"Last trade: {latest_trade['createdat']} {latest_trade['createdatheight']} {latest_trade['price']} {latest_trade['side']} {latest_trade['size']}")
    # Dynamic header with 1-space indentation
    price_header = "BidP".rjust(max_price_len)
    qty_header = "BidQ".rjust(max_size_len)
    left_header = f"{price_header} | {qty_header}".ljust(col_width)
    price_header = "AskP".rjust(max_price_len)
    qty_header = "AskQ".rjust(max_size_len)
    right_header = f"{price_header} | {qty_header}"
    print(f" {left_header} | {right_header}")

    # Display rows with 1-space indentation
    for i in range(depth):
        bid_str = ""
        ask_str = ""

        # Format bids
        if i < len(resolved_bids):
            price_dec, size_dec = resolved_bids[i]
            price_str = bid_price_map.get(price_dec, str(price_dec))
            if '.' in price_str:
                integer_part, fractional_part = price_str.split('.')
                fractional_part = fractional_part.rstrip('0')
                fractional_part = fractional_part + ' ' * (max_decimals - len(fractional_part))
                formatted_price = f"{integer_part}.{fractional_part}"
            else:
                formatted_price = f"{price_str}{' ' * (max_decimals + 1)}"
            formatted_price = formatted_price.rjust(max_price_len)
            size_str = str(size_dec).rjust(max_size_len)
            bid_str = f"{formatted_price} | {size_str}"
        else:
            bid_str = " " * col_width

        # Format asks
        if i < len(resolved_asks):
            price_dec, size_dec = resolved_asks[i]
            price_str = ask_price_map.get(price_dec, str(price_dec))
            if '.' in price_str:
                integer_part, fractional_part = price_str.split('.')
                fractional_part = fractional_part.rstrip('0')
                fractional_part = fractional_part + ' ' * (max_decimals - len(fractional_part))
                formatted_price = f"{integer_part}.{fractional_part}"
            else:
                formatted_price = f"{price_str}{' ' * (max_decimals + 1)}"
            formatted_price = formatted_price.rjust(max_price_len)
            size_str = str(size_dec).rjust(max_size_len)
            ask_str = f"{formatted_price} | {size_str}"
        else:
            ask_str = " " * col_width

        print(f" {bid_str} | {ask_str}")

    # Calculate additional metrics
    maxbid = resolved_bids[0][0] if resolved_bids else Decimal('0')
    minask = resolved_asks[0][0] if resolved_asks else Decimal('0')
    diff = minask - maxbid if resolved_bids and resolved_asks else Decimal('0')
    diff_percent = (diff / maxbid * 100) if maxbid != 0 else Decimal('0')

    # Format difference without trailing zeros
    diff_str = f"{'+' if diff >= 0 else ''}{float(diff):.4f}".rstrip('0').rstrip('.')

    # Format percentage with up to 4 digits precision, no trailing zeros
    diff_percent_str = f"{float(diff_percent):.4f}".rstrip('0').rstrip('.') + '%'

    bid_volume = sum(size_dec for _, size_dec in resolved_bids[:depth])
    ask_volume = sum(size_dec for _, size_dec in resolved_asks[:depth])

    # Format metrics with alignment
    maxbid_str = bid_price_map.get(maxbid, str(maxbid))
    if '.' in maxbid_str:
        integer_part, fractional_part = maxbid_str.split('.')
        fractional_part = fractional_part.rstrip('0')
        fractional_part = fractional_part + ' ' * (max_decimals - len(fractional_part))
        formatted_maxbid = f"{integer_part}.{fractional_part}"
    else:
        formatted_maxbid = f"{maxbid_str}{' ' * (max_decimals + 1)}"
    formatted_maxbid = formatted_maxbid.rjust(max_price_len)

    minask_str = ask_price_map.get(minask, str(minask))
    if '.' in minask_str:
        integer_part, fractional_part = minask_str.split('.')
        fractional_part = fractional_part.rstrip('0')
        fractional_part = fractional_part + ' ' * (max_decimals - len(fractional_part))
        formatted_minask = f"{integer_part}.{fractional_part}"
    else:
        formatted_minask = f"{minask_str}{' ' * (max_decimals + 1)}"
    formatted_minask = formatted_minask.rjust(max_price_len)

    bid_volume_str = str(bid_volume).rjust(max_size_len)
    ask_volume_str = str(ask_volume).rjust(max_size_len)

    # Print footer metrics without indentation
    print(f"MaxBid   : {formatted_maxbid}")
    print(f"MinAsk   : {formatted_minask} ({diff_str}, {diff_percent_str})")
    print(f"BidVolume: {bid_volume_str}")
    print(f"AskVolume: {ask_volume_str}")
    if market_data:
        # Calculate max width for market data labels
        max_label_width = max(len(label) for label in [
            'priceChange24H', 'nextFundingRate', 'openInterest', 'trades24H',
            'volume24H', 'effectiveAt', 'effectiveAtHeight', 'marketId', 'oraclePrice', 'Runtime'
        ])
        # Calculate max width for market data values
        max_data_width = max(
            len(str(market_data.get(key, '')))
            for key in ['pricechange24h', 'openinterest', 'trades24h',
                        'volume24h', 'effectiveat', 'effectiveatheight', 'marketid', 'oracleprice']
        ) if market_data else 13
        # Include nextfundingrate with fixed 8 decimal places
        max_data_width = max(max_data_width, len(format(round(market_data.get('nextfundingrate', 0), 8), '.8f')) if market_data else 0)
        datetime_width = len(market_data.get('datetime', '2025-08-25T20:45:57'))  # 19 chars

        print(f"{'priceChange24H'.ljust(max_label_width)} : {str(market_data['pricechange24h']).ljust(max_data_width)} {market_data['datetime'].ljust(datetime_width)}")
        print(f"{'nextFundingRate'.ljust(max_label_width)} : {format(round(market_data['nextfundingrate'], 8), '.8f').ljust(max_data_width)} {market_data['datetime'].ljust(datetime_width)}")
        print(f"{'openInterest'.ljust(max_label_width)} : {str(market_data['openinterest']).ljust(max_data_width)} {market_data['datetime'].ljust(datetime_width)}")
        print(f"{'trades24H'.ljust(max_label_width)} : {str(market_data['trades24h']).ljust(max_data_width)} {market_data['datetime'].ljust(datetime_width)}")
        print(f"{'volume24H'.ljust(max_label_width)} : {str(market_data['volume24h']).ljust(max_data_width)} {market_data['datetime'].ljust(datetime_width)}")
        print(f"{'effectiveAt'.ljust(max_label_width)} : {str(market_data['effectiveat']).ljust(max_data_width)} {market_data['datetime'].ljust(datetime_width)}")
        print(f"{'effectiveAtHeight'.ljust(max_label_width)} : {str(market_data['effectiveatheight']).ljust(max_data_width)} {market_data['datetime'].ljust(datetime_width)}")
        print(f"{'marketId'.ljust(max_label_width)} : {str(market_data['marketid']).ljust(max_data_width)} {market_data['datetime'].ljust(datetime_width)}")
        print(f"{'oraclePrice'.ljust(max_label_width)} : {str(market_data['oracleprice']).ljust(max_data_width)} {market_data['datetime'].ljust(datetime_width)}")
        print(f"{'Runtime'.ljust(max_label_width)} : {runtime}")
    print("---")

async def main():
    args = parse_args()
    # Derive port from clobPairId
    try:
        clob_pair_id = get_clob_pair_id(args.market)
        port = 10000 + clob_pair_id
    except Exception as e:
        print(f"Failed to start client: {e}")
        return

    url = f"http://{args.ip}:{port}/orderbook"
    print(f"Connecting to server at {url}")

    # Set up database connection
    try:
        if args.ip in ("localhost", "127.0.0.1"):
            # Local socket connection
            pool = await asyncpg.create_pool(
                database="orderbook",
                user="vmware",
                password="orderbook"
            )
        else:
            # Network connection
            pool = await asyncpg.create_pool(
                host=args.ip,
                port=5432,
                database="orderbook",
                user="vmware",
                password="orderbook"
            )
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return

    async with pool:
        while True:
            start_time = time.perf_counter()
            try:
                response = requests.get(url, timeout=5)
                response.raise_for_status()
                snapshot = response.json()

                market = snapshot.get("market", "Unknown")
                bids = snapshot.get("bids", [])
                asks = snapshot.get("asks", [])

                latest_trade = await get_latest_trade(pool, market)
                market_data = await get_market_data(pool, market)

                # Calculate runtime for this iteration
                duration = time.perf_counter() - start_time
                runtime = str(datetime.timedelta(seconds=duration))

                print_order_book(bids, asks, market, args.depth, latest_trade, market_data, runtime)

                loop = os.environ.get("OB2LOOP") == "x"
                if not loop:
                    break  # Exit after one fetch if not looping

            except requests.exceptions.RequestException as e:
                print(f"Error fetching orderbook: {e}")
                loop = os.environ.get("OB2LOOP") == "x"
                if not loop:
                    break  # Exit on error if not looping

            time.sleep(args.interval)

if __name__ == "__main__":
    asyncio.run(main())
