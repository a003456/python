import asyncio
import time

import websockets
import struct
import json
import pandas as pd
import struct
from datetime import datetime


import struct
from datetime import datetime

def decode_binary_data(bin_data):


    exchange_map = {
        "cds": 10000000.0,
        "bcd": 10000.0,
        "indices": [0, 1, 2]  # List of indices segment constants that are not tradable
    }


    def _unpack_int(bin_data, start, end, byte_format="I"):
        """Unpack binary data as unsigned integer."""
        return struct.unpack(">" + byte_format, bin_data[start:end])[0]

    def _split_packets(bin_data):
        """Split the data to individual packets of ticks."""
        if len(bin_data) < 2:
            return []

        number_of_packets = _unpack_int(bin_data, 0, 2, byte_format="H")
        packets = []

        j = 2
        for i in range(number_of_packets):
            packet_length = _unpack_int(bin_data, j, j + 2, byte_format="H")
            packets.append(bin_data[j + 2: j + 2 + packet_length])
            j = j + 2 + packet_length

        return packets

    data = []
    packets = _split_packets(bin_data)

    for packet in packets:
        instrument_token = _unpack_int(packet, 0, 4)
        segment = instrument_token & 0xff  # Retrieve segment constant from instrument_token

        # Determine divisor based on segment using exchange_map
        if segment in exchange_map:
            divisor = exchange_map[segment]
        else:
            divisor = 100.0

        # Determine if tradable based on segment
        tradable = segment not in exchange_map.get("indices", [])

        # Process based on packet length
        if len(packet) == 8:
            # LTP packets
            data.append({
                "instrument_token": instrument_token,
                "last_price": _unpack_int(packet, 4, 8) / divisor
            })


        elif len(packet) == 28 or len(packet) == 32:
            # Indices quote and full mode
            mode = "QUOTE" if len(packet) == 28 else "FULL"

            d = {
                "tradable": tradable,
                "mode": mode,
                "instrument_token": instrument_token,
                "last_price": _unpack_int(packet, 4, 8) / divisor,
                "ohlc": {
                    "high": _unpack_int(packet, 8, 12) / divisor,
                    "low": _unpack_int(packet, 12, 16) / divisor,
                    "open": _unpack_int(packet, 16, 20) / divisor,
                    "close": _unpack_int(packet, 20, 24) / divisor
                }
            }

            # Compute the change price using close price and last price
            d["change"] = 0
            if d["ohlc"]["close"] != 0:
                d["change"] = (d["last_price"] - d["ohlc"]["close"]) * 100 / d["ohlc"]["close"]

            # Full mode with timestamp
            if len(packet) == 32:
                try:
                    timestamp = datetime.fromtimestamp(_unpack_int(packet, 28, 32))
                except Exception:
                    timestamp = None

                d["exchange_timestamp"] = timestamp

            data.append(d)


        elif len(packet) == 44 or len(packet) == 184:
            # Quote and full mode
            mode = "QUOTE" if len(packet) == 44 else "FULL"

            d = {
                "tradable": tradable,
                "mode": mode,
                "instrument_token": instrument_token,
                "last_price": _unpack_int(packet, 4, 8) / divisor,
                "last_traded_quantity": _unpack_int(packet, 8, 12),
                "average_traded_price": _unpack_int(packet, 12, 16) / divisor,
                "volume_traded": _unpack_int(packet, 16, 20),
                "total_buy_quantity": _unpack_int(packet, 20, 24),
                "total_sell_quantity": _unpack_int(packet, 24, 28),
                "ohlc": {
                    "open": _unpack_int(packet, 28, 32) / divisor,
                    "high": _unpack_int(packet, 32, 36) / divisor,
                    "low": _unpack_int(packet, 36, 40) / divisor,
                    "close": _unpack_int(packet, 40, 44) / divisor
                }
            }

            # Compute the change price using close price and last price
            d["change"] = 0
            if d["ohlc"]["close"] != 0:
                d["change"] = (d["last_price"] - d["ohlc"]["close"]) * 100 / d["ohlc"]["close"]

            # Parse full mode
            if len(packet) == 184:
                try:
                    last_trade_time = datetime.fromtimestamp(_unpack_int(packet, 44, 48))
                except Exception:
                    last_trade_time = None

                try:
                    timestamp = datetime.fromtimestamp(_unpack_int(packet, 60, 64))
                except Exception:
                    timestamp = None

                d["last_trade_time"] = last_trade_time
                d["oi"] = _unpack_int(packet, 48, 52)
                d["oi_day_high"] = _unpack_int(packet, 52, 56)
                d["oi_day_low"] = _unpack_int(packet, 56, 60)
                d["exchange_timestamp"] = timestamp

                # Market depth entries.
                depth = {
                    "buy": [],
                    "sell": []
                }

                # Compile the market depth lists.
                for i, p in enumerate(range(64, len(packet), 12)):
                    depth["sell" if i >= 5 else "buy"].append({
                        "quantity": _unpack_int(packet, p, p + 4),
                        "price": _unpack_int(packet, p + 4, p + 8) / divisor,
                        "orders": _unpack_int(packet, p + 8, p + 10, byte_format="H")
                    })

                d["depth"] = depth

            data.append(d)

    return data





latest_prices = pd.DataFrame(columns=['symbol', 'price'])


def update_global_df(instrument_token, last_price):
    global global_df
    # Check if the instrument_token already exists in the DataFrame
    if instrument_token in global_df['instrument_token'].values:
        # Update the existing row
        global_df.loc[global_df['instrument_token'] == instrument_token, 'last_price'] = last_price
    else:
        # Add a new row
        global_df = global_df.append({'instrument_token': instrument_token, 'last_price': last_price}, ignore_index=True)

    print(global_df)


async def connect_websocket():
    uri = "wss://ws.zerodha.com/?api_key=kitefront&user_id=BS0777&enctoken=rBlDy9EIGKSmI%2BC3J3H2IdFXnzH36DmclNnJu%2BJB%2F2%2F5kZo2VCF7kKUE95ixEOxR4YHcCsHrgzchpJSw%2FJ9k8V%2B6QLzKSFlA2jQ0s7k3yZfFBXC3BIje8g%3D%3D&uid=1721663314974&user-agent=kite3-web&version=3.0.0"

    async with websockets.connect(uri) as websocket:
        print("WebSocket connection established.")

        # Construct message
        message = json.dumps({"a": "subscribe", "v": [111202567,109790471]})
        await websocket.send(message)
        message = json.dumps({"a": "mode", "v": ["ltp", [111202567,109790471]]})
        await websocket.send(message)

        # print(f"Sent message to server: {message}")

        # Example: Receiving messages indefinitely
        while True:

            response = await websocket.recv()
            try:
                decoded_data = decode_binary_data(response)
                if decoded_data:
                    if decoded_data:
                        for item in decoded_data:
                            instrument_token = item['instrument_token']
                            last_price = item['last_price']
                            update_global_df(instrument_token, last_price)
                            print(f"Updated latest price for instrument_token {instrument_token}: {last_price}")

            except:
                continue

            time.sleep(1)

asyncio.get_event_loop().run_until_complete(connect_websocket())
