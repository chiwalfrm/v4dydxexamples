import asyncio
import random
from functools import partial

from v4_proto.dydxprotocol.clob.order_pb2 import Order

from dydx_v4_client import MAX_CLIENT_ID, OrderFlags
from dydx_v4_client.indexer.rest.constants import OrderType, OrderExecution
from dydx_v4_client.indexer.rest.indexer_client import IndexerClient
from dydx_v4_client.network import make_mainnet, make_testnet, make_secure, make_insecure, mainnet_node, testnet_node
from dydx_v4_client.node.client import NodeClient
from dydx_v4_client.node.market import Market, since_now
from dydx_v4_client.wallet import Wallet

#========== FILL THIS SECTION OUT ==========
DYDX_MNEMONIC = '<FILL THIS OUT>'
DYDX_ADDRESS = '<FILL THIS OUT>'
DYDX_SUBACCOUNT = 0
DYDX_NETWORK = 'testnet'
DYDX_SECURE = False
DYDX_ORDER_MARKET = 'BTC-USD'
DYDX_ORDER_TYPE = OrderType.LIMIT
DYDX_ORDER_SIDE = Order.Side.SIDE_BUY
DYDX_ORDER_SIZE = 0.001
DYDX_ORDER_PRICE = 40000
DYDX_ORDER_EXPIRATION = 60
#===========================================

if DYDX_NETWORK == 'mainnet':
        if DYDX_SECURE == True:
                NETWORK = partial(
                        make_secure,
                        mainnet_node,
                        rest_indexer="https://indexer.dydx.trade",
                        websocket_indexer="wss://indexer.dydx.trade/v4/ws",
                        node_url="dydx-grpc.publicnode.com",
                )
        else:
                NETWORK = partial(
                        make_insecure,
                        mainnet_node,
                        rest_indexer="https://indexer.dydx.trade",
                        websocket_indexer="wss://indexer.dydx.trade/v4/ws",
                        node_url="<FILL THIS OUT>:9090",
                )
elif DYDX_NETWORK == 'testnet':
        if DYDX_SECURE == True:
                NETWORK = partial(
                        make_secure,
                        testnet_node,
                        rest_indexer="https://dydx-testnet.imperator.co",
                        websocket_indexer="wss://indexer.v4testnet.dydx.exchange/v4/ws",
                        node_url="test-dydx-grpc.kingnodes.com",
                )
        else:
                NETWORK = partial(
                        make_insecure,
                        testnet_node,
                        rest_indexer="https://dydx-testnet.imperator.co",
                        websocket_indexer="wss://indexer.v4testnet.dydx.exchange/v4/ws",
                        node_url="<FILL THIS OUT>:9090",
                )

async def main():
        node = await NodeClient.connect(NETWORK().node)
        indexer = IndexerClient(NETWORK().rest_indexer)
        market = Market(
                (await indexer.markets.get_perpetual_markets(DYDX_ORDER_MARKET))["markets"][DYDX_ORDER_MARKET]
        )
        wallet = await Wallet.from_mnemonic(node, DYDX_MNEMONIC, DYDX_ADDRESS)
        order_id = market.order_id(
                address=DYDX_ADDRESS,
                subaccount_number=DYDX_SUBACCOUNT,
                client_id=random.randint(0, MAX_CLIENT_ID),
                order_flags=OrderFlags.LONG_TERM,
        )
        place_order_result = await node.place_order(
                wallet,
                market.order(
                        order_id=order_id,
                        order_type=DYDX_ORDER_TYPE,
                        side=DYDX_ORDER_SIDE,
                        size=DYDX_ORDER_SIZE,
                        price=DYDX_ORDER_PRICE,
                        time_in_force=Order.TIME_IN_FORCE_UNSPECIFIED,
                        reduce_only=False,
                        post_only=False,
                        good_til_block=0,
                        good_til_block_time=since_now(seconds=DYDX_ORDER_EXPIRATION),
                        execution=OrderExecution.DEFAULT,
                        conditional_order_trigger_subticks=0,
                ),
        )
        print('order_id:', order_id)
        print(place_order_result)

asyncio.run(main())
