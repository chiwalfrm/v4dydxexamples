import asyncio
import json
import sys
import time
import uvloop
from picows import ws_connect, WSFrame, WSTransport, WSListener, WSMsgType

WSINDEXERURL = 'wss://indexer.dydx.trade/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4testnet.dydx.exchange/v4/ws'
#WSINDEXERURL = 'wss://indexer.v4staging.dydx.exchange/v4/ws'

class DydxClientListener(WSListener):
    def on_ws_connected(self, transport: WSTransport):
        # Subscribe to BTC-USD order book channel (v4) after connection
        subscribe_message = {
            "type": "subscribe",
            "channel": "v4_subaccounts",
#            "id": "BTC-USD"
            "id": sys.argv[1],
        }
        transport.send(WSMsgType.TEXT, json.dumps(subscribe_message).encode())
        print("Subscribed to BTC-USD order book (v4)")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
        # Handle incoming WebSocket messages
        if frame.msg_type == WSMsgType.TEXT:
            try:
                message = frame.get_payload_as_ascii_text()
                parsed_message = json.loads(message)
                if 'type' in parsed_message and parsed_message["type"] == "error":
                    print(parsed_message)
                    raise msgerror("msgerror")

#                print(json.dumps(parsed_message, indent=2))
                parsed_message['timestamp3'] = time.time()
                print(parsed_message)
            except msgerror as e:
                print(f"Exception {e} on message {message}")
            except Exception as e:
                print(f"Exception {e} on message {message}")
#            except json.JSONDecodeError:
#                print(f"Received non-JSON message: {message}")
#            except UnicodeDecodeError:
#                print("Received invalid UTF-8 text frame")
        elif frame.msg_type == WSMsgType.CLOSE:
            # Handle CLOSE frame
            close_code = frame.get_close_code()
            close_message = frame.get_close_message()
            close_message_str = close_message.decode('utf-8', errors='ignore') if close_message is not None else "No close message"
            print(f"Received CLOSE frame: code={close_code}, message={close_message_str}")
        else:
            print(f"Received non-text frame: {frame.msg_type}")

    def on_ws_disconnected(self, transport: WSTransport):
        # Handle disconnection
        print(f"DEBUG: on_ws_disconnected called with transport={transport}")
        print("Disconnected: Close details provided via CLOSE frame in on_ws_frame")

async def dydx_orderbook_client():
    # Updated dYdX WebSocket endpoint (v4)
    url = WSINDEXERURL
    max_reconnect_attempts = 5
    reconnect_delay = 5  # seconds

    for attempt in range(1, max_reconnect_attempts + 1):
        try:
            print(f"Connection attempt {attempt}/{max_reconnect_attempts}")
            # Create WebSocket client
            transport, client = await ws_connect(DydxClientListener, url)
            # Wait for disconnection
            await transport.wait_disconnected()
        except Exception as e:
            print(f"Error during connection: {e}")
            if attempt < max_reconnect_attempts:
                print(f"Reconnecting in {reconnect_delay} seconds...")
                await asyncio.sleep(reconnect_delay)
            else:
                print("Max reconnection attempts reached. Exiting.")
                break

# Run the async client
if __name__ == "__main__":
    print("Running updated dYdX WebSocket client with v4_orderbook")
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Optional: use uvloop for better performance
    asyncio.run(dydx_orderbook_client())
