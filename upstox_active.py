import threading
from threading import Thread, Event
from pandas import json_normalize
import warnings
import time
import os
import requests
from selenium.webdriver.common.by import By
from seleniumbase import Driver
from urllib.parse import quote
import json
from requests import Session
from datetime import time
import pyotp
from datetime import timedelta
import pandas as pd
import pytz
from time import sleep
from datetime import datetime
import struct
import urllib.parse
import websockets
import asyncio
import copy


os.chdir("D:\\algo_trading\\option_buy_algo")
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)
pd.set_option('display.max_rows', None)
warnings.filterwarnings('ignore')




global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
            checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
            main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
            upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser



nse_timming = (time(9, 20) <= datetime.now().time() <= time(15, 10))

main_margin = 10
websocket_2 = None

orderbook_columns = ["cta", "message", "orderType", "originalMessage", "product", "side", "status",
    "time", "validity", "instrument_key", "isAMO", "instrument_e", "instrument_lotSize",
    "instrument_t", "instrument_s", "orderNumber_exchange", "orderNumber_oms",
    "orderNumber_orderRefId", "orderNumber_parent", "orderNumber_strategyCode",
    "price_avg", "price_limit", "price_trigger", "price_trailingTicks", "price_initialSL",
    "price_reference", "quantity_cancelled", "quantity_disclosed", "quantity_pending",
    "quantity_total", "quantity_traded", "instrument_token", "exchange_token",
    "tradingsymbol", "name", "last_price", "expiry", "strike", "tick_size", "lot_size",
    "instrument_type", "segment", "exchange", "option_type"]

main_orderbook = pd.DataFrame(columns=orderbook_columns)



position_book_columns = ["product", "instrument_key", "fillInfo_cfBuy_amt", "fillInfo_cfBuy_avgPrice", "fillInfo_cfBuy_qty",
    "fillInfo_cfSell_amt", "fillInfo_cfSell_avgPrice", "fillInfo_cfSell_qty", "fillInfo_dayBuy_amt",
    "fillInfo_dayBuy_avgPrice", "fillInfo_dayBuy_qty", "fillInfo_daySell_amt", "fillInfo_daySell_avgPrice",
    "fillInfo_daySell_qty", "instrument_e", "instrument_m", "instrument_s", "instrument_t", "netInfo_buyQty",
    "netInfo_buyValue", "netInfo_netAvgPrice", "netInfo_sellQty", "netInfo_sellValue", "instrument_token",
    "exchange_token", "tradingsymbol", "name", "last_price", "expiry", "strike", "tick_size", "lot_size",
    "instrument_type", "segment", "exchange", "option_type"]


main_position = pd.DataFrame(columns=position_book_columns)



chain_columns = ["instrument_token", "exchange_token", "tradingsymbol", "name", "last_price",
                 "expiry", "strike", "tick_size", "lot_size", "instrument_type", "segment",
                 "exchange", "instrument_key", "option_type", "index_token", "index_ltp",
                 "stkdiff", "chain_count"]

no_of_lots = 1
chain_lenth_no = 10
chain_lenth = ((chain_lenth_no*2)+1)*2
chain = pd.DataFrame(columns=chain_columns)


checkorderbook = pd.DataFrame(columns=["name", "option_type"])
checkpositionbook = pd.DataFrame(columns=["name", "option_type"])




main_volume_data = {}
main_ltp_data = {}

enc_token = str("enc_token")
websocket = None
peing_count = 0





def zerodha_login():

    try:

        global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
            checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
            main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
            upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser

        # print("getting new login details")
        s = Session()
        base_url = "https://kite.zerodha.com/"
        r = s.get(base_url)
        user_id = "BS0777"
        password = "Art@003456"
        login_url = "https://kite.zerodha.com/api/login"
        r = s.post(login_url, data={"user_id": user_id, "password": password})
        j = json.loads(r.text)
        print(j)
        # send_to_telegram(str(j), -1001922818361)
        totp = pyotp.TOTP("3HRGVZG6WMUUAFSHQI6IOLGB43CIFV5Y")
        twofa = totp.now()
        # print(twofa)
        twofa_url = "https://kite.zerodha.com/api/twofa"
        urldata = {"user_id": user_id, "request_id": j['data']["request_id"], "twofa_value": twofa}
        r = s.post(twofa_url, data=urldata)
        j = json.loads(r.text)
        # send_to_telegram(str(j), -1001922818361)
        # print(s.cookies["enctoken"])
        enc_token = s.cookies['enctoken']
        h = {}
        h['authorization'] = "enctoken {}".format(enc_token)
        h['referer'] = 'https://kite.zerodha.com/dashboard'
        h['x-kite-version'] = '2.4.0'
        h['sec-fetch-site'] = 'same-origin'
        h['sec-fetch-mode'] = 'cors'
        h['sec-fetch-dest'] = 'empty'
        s.headers.update(h)  # Update the request session object with headers
        # print(h)
        print("zerodha_loged_in")
        return s,enc_token


    except Exception as error:
        print("zerodha_login " + str(error))
        sleep(3)
        zerodha_login()






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

    try:
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

                d = {"tradable": tradable,"mode": mode,"instrument_token": instrument_token,
                     "last_price": _unpack_int(packet, 4, 8) / divisor,"ohlc": {"high": _unpack_int(packet, 8, 12) / divisor,
                        "low": _unpack_int(packet, 12, 16) / divisor,
                        "open": _unpack_int(packet, 16, 20) / divisor,
                        "close": _unpack_int(packet, 20, 24) / divisor}}

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

                d = {"tradable": tradable,
                    "mode": mode,
                    "instrument_token": instrument_token,
                    "last_price": _unpack_int(packet, 4, 8) / divisor,
                    "last_traded_quantity": _unpack_int(packet, 8, 12),
                    "average_traded_price": _unpack_int(packet, 12, 16) / divisor,
                    "volume_traded": _unpack_int(packet, 16, 20),
                    "total_buy_quantity": _unpack_int(packet, 20, 24),
                    "total_sell_quantity": _unpack_int(packet, 24, 28),
                    "ohlc": {"open": _unpack_int(packet, 28, 32) / divisor,
                             "high": _unpack_int(packet, 32, 36) / divisor,
                             "low": _unpack_int(packet, 36, 40) / divisor,
                             "close": _unpack_int(packet, 40, 44) / divisor}}

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

    except Exception as error:
        print("error while decoding data " + str(error))
        return None




async def subscribe_to_instruments(keys):
    global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
        checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
        main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
        upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser

    try:
        # print(keys)
        peing_count = 0
        message = json.dumps({"a": "mode", "v": ["full", keys]})
        await websocket.send(message)
        # print("message send " + keys )


    except Exception as error:
        print("subscribe_to_instruments " + str(error))





async def add_instrument_keys(new_keys):
    global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
        checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
        main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
        upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser


    try:
        keystoadd = []
        for key in new_keys:
            if key not in main_ltp_data:
                key = int(key)
                keystoadd.append(key)  # Change extend to append

        if len(keystoadd) > 0:
            await subscribe_to_instruments(keystoadd)
            print("keys added: " + str(keystoadd))


    except Exception as error:
        print("add_instrument_keys " + str(error))








async def connect_websocket():
    global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
        checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
        main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
        upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser

    parse_enc_token = urllib.parse.quote(enc_token)
    uri = f"wss://ws.zerodha.com/?api_key=kitefront&user_id=BS0777&enctoken={parse_enc_token}&uid=1721742018518&user-agent=kite3-web&version=3.0.0"

    async with websockets.connect(uri) as websocket:
        print("WebSocket connection established.")
        websocket_2 = websocket
        keys = [109790471,256265, 260105, 257801, 288009]
        await subscribe_to_instruments(keys)

        while not stop_flag.is_set():
            try:
                peing_count += 1
                if peing_count == 200:
                    await subscribe_to_instruments(instrument_keys)
                    sleep(0.5)


                response = await asyncio.wait_for(websocket.recv(), timeout=0.5)
                decoded_data = decode_binary_data(response)
                if decoded_data:
                    # print(decoded_data)
                    for item in decoded_data:
                        instrument_token = item['instrument_token']
                        last_price = item['last_price']
                        main_ltp_data[instrument_token] = last_price

                        volume_traded = item.get('volume_traded')
                        if volume_traded is not None:
                            main_volume_data[instrument_token] = volume_traded

                        # print(main_ltp_data)

            except Exception as error:
                # print("websocket " + str(error))
                # print(main_position)
                continue

            sleep(0.01)









def get_data(instrument_token):
    try:
        timeframe = "minute"
        holdtime = 1 if timeframe == "minute" else int(''.join(filter(str.isdigit, timeframe)))
        holdcandle = 0 if timeframe == "minute" else holdtime * 60

        previousdays = 3
        params = {'user_id': 'BS0777', 'oi': '1',
                  'from': str((datetime.now() - timedelta(days=previousdays)).strftime("%Y-%m-%d")),
                  'to': str(datetime.now().strftime("%Y-%m-%d")), }

        instrument_token = int(instrument_token)
        instrument_token = str(instrument_token)

        ist = pytz.timezone('Asia/Kolkata')
        urlpart1 = "https://kite.zerodha.com/oms/instruments/historical/"
        urlpart2 = "/" + str(timeframe)


        instrument_url = urlpart1 + instrument_token + urlpart2
        r = s.get(instrument_url, params=params)
        data = r.json()
        data = pd.DataFrame(data['data']['candles'])
        data.columns = ["Datetime", "Open", "High", "Low", "Close", "Volume", "oi"]
        data = data.tail(50)
        data["time"] = data["Datetime"].str.split("T").str[1].str[:-6]
        current_time = pd.Timestamp.now(tz=ist).strftime("%H:%M:%S")
        data["time_diff"] = pd.to_datetime(current_time) - pd.to_datetime(data["time"])
        data["time_diff_seconds"] = abs(data["time_diff"].dt.total_seconds())
        data = data[data["time_diff_seconds"] > holdcandle]
        # print(data.tail(3))
        return data

    except Exception as error:
        print("get_data_1 " + str(error))
        # sleep(0.5)
        try:
            instrument_token = int(instrument_token)
            instrument_token = str(instrument_token)

            ist = pytz.timezone('Asia/Kolkata')
            urlpart1 = "https://kite.zerodha.com/oms/instruments/historical/"
            urlpart2 = "/" + str(timeframe)

            instrument_url = urlpart1 + instrument_token + urlpart2
            r = s.get(instrument_url, params=params)
            data = r.json()
            data = pd.DataFrame(data['data']['candles'])
            data.columns = ["Datetime", "Open", "High", "Low", "Close", "Volume", "oi"]
            data = data.tail(50)
            data["time"] = data["Datetime"].str.split("T").str[1].str[:-6]
            current_time = pd.Timestamp.now(tz=ist).strftime("%H:%M:%S")
            data["time_diff"] = pd.to_datetime(current_time) - pd.to_datetime(data["time"])
            data["time_diff_seconds"] = abs(data["time_diff"].dt.total_seconds())
            data = data[data["time_diff_seconds"] > holdcandle]
            # print(data.tail(3))
            return data

        except Exception as error:
            print("get_data_2 " + str(error))
            stop_flag.set()









def upstox_login():
    global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
        checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
        main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
        upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser

    try:
        API_KEY = '67ff0f62-a07c-4a5b-8959-5a93b5ed3921'
        SECRET_KEY = 'ahhmqrt7tj'
        RURL = 'https://127.0.0.1:5000/'
        mobile_no = "8850908394"
        pinCode = "121093"
        tttime = 0.5
        rurlEncode = quote(RURL, safe="")
        baseurl = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={rurlEncode}'
        print(baseurl)
        # webbrowser.open(baseurl)

        browser = Driver()

        # browser = webdriver.Chrome()

        browser.get(baseurl)
        # browser.execute_script("window.open()")

        sleep(tttime)
        browser.find_element(By.ID, "mobileNum").send_keys(mobile_no)
        browser.find_element(By.ID, "getOtp").click()


        sleep(tttime)
        totp = pyotp.TOTP("HCIW5A3PZLR4TEA4H3TVM4SSYD26TH3L")
        twofa = totp.now()
        twofa = str(twofa[0:3]) + "-" + str(twofa[3:6])
        browser.find_element(By.ID, "otpNum").click()
        browser.find_element(By.ID, "otpNum").send_keys(twofa)
        browser.find_element(By.ID, "continueBtn").click()
        sleep(tttime)


        browser.find_element(By.ID, "pinCode").send_keys(pinCode)
        browser.find_element(By.ID, "pinContinueBtn").click()

        sleep(tttime)
        code_index = str(browser.current_url).find("code=") + len("code=")
        upstox_token = str(browser.current_url)[code_index:code_index + 6]
        # print(upstox_token)

        browser.get("https://pro.upstox.com/orders/regular")
        # sleep(3)
        # browser.get("https://pro.upstox.com/orders/regular")


        upstox_cookie = browser.get_cookies()
        # print(upstox_cookie)

        web_access_token = next((cookie['value'] for cookie in upstox_cookie if cookie['name'] == 'access_token'), None)

        # print(upstox_cookie)
        upstox_session = requests.Session()
        for cookie in upstox_cookie:

            upstox_session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])

        upstox_web_headers = {'authority'      : 'service.upstox.com', 'accept': 'application/json',
                              'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,mr;q=0.6',
                              'origin'         : 'https://pro.upstox.com',
                              'referer'        : 'https://pro.upstox.com/', }


        url = 'https://api.upstox.com/v2/login/authorization/token'
        headers = {'accept'      : 'application/json','Api-Version' : '2.0','Content-Type': 'application/x-www-form-urlencoded'}

        data = {'code'         : upstox_token,'client_id'    : API_KEY,'client_secret': SECRET_KEY,'redirect_uri' : RURL,'grant_type'   : 'authorization_code'}

        response = requests.post(url, headers=headers, data=data)
        json_response = response.json()
        # print(json_response)

        access_token = json_response['access_token']
        upstox_headers = {'accept': 'application/json', 'Api-Version': '2.0', 'Authorization': f'Bearer {access_token}'}
        # print(upstox_headers)
        browser.quit()
        print("upstox_loged_in")
        # print(upstox_web_headers)
        # print(upstox_cookie)


        return upstox_headers



    except Exception as error:
        print("upstox_login " + str(error))
        browser.quit()
        sleep(5)
        upstox_login()






def getinstruments():
    try:

        global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
            checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
            main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
            upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser

        print("downloading instruments wait.............")

        url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
        instrumentslist = pd.read_csv(url, dtype={'exchange_token': str, 'tradingsymbol': str}, compression='gzip')
        instrumentslist = instrumentslist.drop(
            columns=['name', 'last_price', 'expiry', 'strike', 'tick_size', 'lot_size', 'instrument_type'])

        # print(instrumentslist.head(5))

        zinstru = pd.read_csv('https://api.kite.trade/instruments',
                              dtype={'instrument_token': int, 'exchange_token': str, 'tradingsymbol': str})
        exchange_mapping = {'NSE': 'NSE_EQ', 'BSE': 'BSE_EQ', 'NFO': 'NSE_FO', 'BFO': 'BSE_FO', 'MCX': 'MCX_FO'}
        zinstru['exchange'] = zinstru['exchange'].map(exchange_mapping)

        # print(zinstru.head(5))

        instrumentslist = pd.merge(zinstru, instrumentslist, on=['exchange_token', "tradingsymbol", "exchange"],how='inner')

        # instrumentslist.to_csv("instrumentslist.csv", mode='w', index=False, header=True, float_format='%.2f')
        #

        OPT_instr = instrumentslist[instrumentslist['exchange'] == 'NSE_FO']

        # OPT_instr = OPT_instr[OPT_instr['name'].isin(["NIFTY", "FINNIFTY", "BANKNIFTY","MIDCPNIFTY"])]
        OPT_instr = OPT_instr[OPT_instr['name'].isin(["NIFTY", "FINNIFTY", "BANKNIFTY"])]

        OPT_instr['expiry'] = pd.to_datetime(OPT_instr['expiry'])
        OPT_instr = OPT_instr[OPT_instr['expiry'] > (datetime.now() - timedelta(days=1))]
        OPT_instr = OPT_instr[OPT_instr['expiry'] == OPT_instr.groupby(['name',"option_type"])['expiry'].transform('min')]

        fut_instr = OPT_instr[OPT_instr['option_type'].isin(["FF"])]
        fut_instr.rename(columns={'instrument_token': 'index_token'}, inplace=True)
        fut_instr = fut_instr[["name","index_token"]]
        fut_instr.loc[fut_instr['name'] == 'NIFTY', 'index_token'] = 256265
        fut_instr.loc[fut_instr['name'] == 'BANKNIFTY', 'index_token'] = 260105
        fut_instr.loc[fut_instr['name'] == 'FINNIFTY', 'index_token'] = 257801
        fut_instr.loc[fut_instr['name'] == 'MIDCPNIFTY', 'index_token'] = 288009
        fut_instr.loc[fut_instr['name'] == 'NIFTYNXT50', 'index_token'] = 270857

        # new_sym = fut_instr["index_token"].unique().tolist()
        # asyncio.run(add_instrument_keys(new_sym))

        OPT_instr = OPT_instr[OPT_instr['option_type'].isin(["CE", "PE"])]
        OPT_instr = OPT_instr[OPT_instr['expiry'] == OPT_instr.groupby(['exchange'])['expiry'].transform('min')]

        # print(OPT_instr)

        print("downloaded instruments.................")


    except Exception as error:
        print("error while downloading instruments " + str(error))
        print("downloading instruments failed................")
        sleep(5)
        getinstruments()
















def getroundnumber(number, nn):
    rounded_value = round(number/nn,0) * nn
    return round(float(rounded_value),2)







def get_upsotx_margin_sec():
    global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
        checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
        main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
        upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser

    FOmargins = (upstox_session.get('https://service.upstox.com/limit/v3/sec', headers=upstox_web_headers, verify=False,timeout=1)).json()['data']['SEC']['cash']['available_to_trade']
    FOmargins = pd.DataFrame(FOmargins)
    main_margin = FOmargins.loc['total', 'total']
    return main_margin





def pending_orders():
    global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
        checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
        main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
        upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser


    while not stop_flag.is_set():
        try:

            sleep(0.5)
            orderbook_url = 'https://service.upstox.com/portfolio/v2/orderbook?includeTsl=true'
            orderbook = (upstox_session.get(orderbook_url, headers=upstox_web_headers, verify=False,timeout=1)).json()

            if str(orderbook["success"]) == "True":

                orderbook = orderbook["data"]["history"]
                if len(orderbook) > 0:
                    orderbook = json_normalize(orderbook, sep='_')
                    orderbook = pd.DataFrame(orderbook)

                    main_orderbook = pd.merge(orderbook, instrumentslist, on=['instrument_key', "instrument_key"], how='inner')


                    orderbook = main_orderbook.copy()

                    req_chain = chain.copy()
                    req_chain = req_chain[["instrument_key", "chain_count"]]
                    orderbook = pd.merge(orderbook, req_chain, on=['instrument_key', "instrument_key"], how='left')

                    # print(orderbook.head(5))
                    orderbook = orderbook[(orderbook["status"] == "TP") | (orderbook["status"] == "O")]
                    orderbook = orderbook[(orderbook["side"] == "S")]
                    orderbook = orderbook[(orderbook["chain_count"] < 0)]


                    checkorderbook = orderbook[["name", "option_type"]]

                    new_sym = orderbook["instrument_token"].unique().tolist()
                    asyncio.run(add_instrument_keys(new_sym))
                    orderbook['count'] = orderbook.groupby(["name", "option_type"]).cumcount()

                    for orderbook_orders in range(len(orderbook)):
                        orderbook_dat = orderbook.iloc[orderbook_orders]

                        if orderbook_dat["instrument_token"] in main_ltp_data:
                            try:
                                orderbook_data = get_data(orderbook_dat["instrument_token"])
                                orderbook_data = orderbook_data.tail(50)
                                sell_trigger_price = orderbook_data.iloc[-1]['Low']
                                sell_trigger_price = getroundnumber(sell_trigger_price - (orderbook_dat["tick_size"]),(orderbook_dat["tick_size"]))
                                sell_price = getroundnumber(sell_trigger_price - (orderbook_dat["tick_size"]),(orderbook_dat["tick_size"]))


                                orderbook_condition1 = orderbook_dat["count"] > 0
                                orderbook_condition2 = getroundnumber(orderbook_dat["price_trigger"],(orderbook_dat["tick_size"])) != getroundnumber(sell_trigger_price,(orderbook_dat["tick_size"]))
                                orderbook_ltp = main_ltp_data.get(orderbook_dat["instrument_token"])
                                orderbook_condition3 = getroundnumber(orderbook_ltp,(orderbook_dat["tick_size"])) < getroundnumber(orderbook_dat["price_trigger"],(orderbook_dat["tick_size"]))
                                nse_timming = (time(9, 20) <= datetime.now().time() <= time(15, 10))
                                orderbook_timing_condition = not nse_timming


                                try:
                                    if orderbook_condition1 or orderbook_condition2 or orderbook_condition3 or orderbook_timing_condition :
                                        orderbook_dat['isAMO'] = orderbook_dat['isAMO'].tolist()
                                        orderbook_json_data = {'data': {'product': orderbook_dat["product"], 'orderNumber': {'oms': orderbook_dat["orderNumber_oms"], 'exchange': '', }, 'isAmo': orderbook_dat["isAMO"], }, }
                                        orderbook_cancel_order = (upstox_session.patch('https://service.upstox.com/interactive/v3/order', headers=upstox_web_headers, json=orderbook_json_data, verify=False,timeout=1)).json()
                                        print(orderbook_cancel_order)
                                        sleep(0.5)

                                except Exception as error:
                                    print("error1" + str(error))
                                    stop_flag.set()

                            except Exception as error:
                                print("error2" + str(error))
                                orderbook_dat['isAMO'] = orderbook_dat['isAMO'].tolist()
                                orderbook_json_data = {'data': {'product': orderbook_dat["product"], 'orderNumber': {'oms': orderbook_dat["orderNumber_oms"], 'exchange': '', }, 'isAmo': orderbook_dat["isAMO"], }, }
                                orderbook_cancel_order = (upstox_session.patch('https://service.upstox.com/interactive/v3/order', headers=upstox_web_headers, json=orderbook_json_data, verify=False,timeout=1)).json()
                                print(orderbook_cancel_order)
                                sleep(0.5)
                                pass

                        else:
                            pass


            else:
                stop_flag.set()


        except Exception as error:
            print("pending_orders " + str(error))
            stop_flag.set()








def create_chain(opt_df):
    try:
        global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
            checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
            main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
            upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser

        ce_opt_df = opt_df[opt_df["option_type"] == "CE"]
        ce_opt_df = ce_opt_df.sort_values(by=['name', 'strike'], ascending=[False, True]).reset_index(drop=True)
        ce_opt_df['chain_count'] = ce_opt_df.groupby(["name", "option_type"]).cumcount()-chain_lenth_no

        pe_opt_df = opt_df[opt_df["option_type"] == "PE"]
        pe_opt_df = pe_opt_df.sort_values(by=['name', 'strike'], ascending=[False, False]).reset_index(drop=True)
        pe_opt_df['chain_count'] = pe_opt_df.groupby(["name", "option_type"]).cumcount()-chain_lenth_no

        chain = pd.concat([pe_opt_df, ce_opt_df])
        return chain

    except Exception as error:
        print("create_chain " + str(error))
        create_chain(opt_df)





def clear_positions():

    try:
        global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
            checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
            main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
            upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser

        print(main_ltp_data)
        nse_timming = (time(9, 20) <= datetime.now().time() <= time(15, 10))

        if nse_timming:
            req_chain = chain.copy()
            req_chain = req_chain[["instrument_key", "chain_count"]]


            position_orderbook = main_orderbook.copy()
            position_orderbook = position_orderbook[(position_orderbook["status"] == "TP") | (position_orderbook["status"] == "O")]
            position_orderbook = pd.merge(position_orderbook, req_chain, on=['instrument_key', "instrument_key"], how='left')
            position_orderbook = position_orderbook[position_orderbook["chain_count"] > 0]

            if len(position_orderbook) > 0:
                for positionbook_orders in range(len(position_orderbook)):
                    position_orderbook_dat = position_orderbook.iloc[positionbook_orders]
                    position_orderbook_dat['isAMO'] = position_orderbook_dat['isAMO'].tolist()
                    orderbook_json_data = {'data': {'product': position_orderbook_dat["product"],
                                                    'orderNumber': {'oms': position_orderbook_dat["orderNumber_oms"], 'exchange': '', },
                                                    'isAmo': position_orderbook_dat["isAMO"], }, }
                    orderbook_cancel_order = (upstox_session.patch('https://service.upstox.com/interactive/v3/order', headers=upstox_web_headers,
                                                                   json=orderbook_json_data, verify=False, timeout=1)).json()
                    print(orderbook_json_data)
                    print(orderbook_cancel_order)
                    sleep(0.5)



            position = main_position.copy()
            new_sym = position["instrument_token"].unique().tolist()
            asyncio.run(add_instrument_keys(new_sym))
            sleep(0.5)


            position["BuyQty"] = pd.to_numeric(position["netInfo_buyQty"])
            position["SellQty"] = pd.to_numeric(position["netInfo_sellQty"])
            position["quantity"] = position["BuyQty"] - position["SellQty"]

            position = position[position["quantity"] != 0]

            position = pd.merge(position, req_chain, on=['instrument_key', "instrument_key"], how='left')
            position["position_lot"] = position["quantity"]/position["lot_size"]


            position = position[~((position["chain_count"] < 0) & (position["quantity"] < 0))]

            buy_positions = position[(position["chain_count"] > 0) & (position["quantity"] > 0) & (position["position_lot"] == no_of_lots)]
            position = position[~((position["chain_count"] > 0) & (position["quantity"] > 0) & (position["position_lot"] == no_of_lots))]

            buy_positions['count'] = buy_positions.groupby(["name", "option_type"]).cumcount()
            invalid_buy_position = buy_positions[buy_positions['count'] > 0]
            position = pd.concat([position, invalid_buy_position], ignore_index=True)

            buy_positions = buy_positions[buy_positions['count'] == 0]
            buy_positions = buy_positions[['name',"option_type","product"]]
            required_chain_leg = chain.copy()

            required_chain_leg = required_chain_leg[required_chain_leg["chain_count"] > 6]
            required_chain_leg = required_chain_leg[required_chain_leg["chain_count"] < 10]
            required_chain_leg['day_volume'] = required_chain_leg['instrument_token'].map(main_volume_data)
            required_chain_leg = required_chain_leg.sort_values(by=['name', 'option_type', 'day_volume', 'chain_count'], ascending=[False, False, False, False]).reset_index(drop=True)
            required_chain_leg['count'] = required_chain_leg.groupby(["name", "option_type"]).cumcount()
            required_chain_leg = required_chain_leg[required_chain_leg["count"] == 0]
            # print(required_chain_leg)

            required_chain_leg = pd.merge(required_chain_leg, buy_positions, on=['name', "option_type"], how='left')
            required_chain_leg = required_chain_leg[required_chain_leg["product"].isna()]
            # print(required_chain_leg)

            if len(required_chain_leg)>0:
                for req_leg_token in range(len(required_chain_leg)):
                    req_leg_dat = required_chain_leg.iloc[req_leg_token]
                    order_ltp = main_ltp_data.get(req_leg_dat["instrument_token"])

                    buy_order = {"quantity": int(abs(no_of_lots * req_leg_dat['lot_size'])), "product": "I",
                                 "validity": "DAY", "price": getroundnumber(order_ltp, req_leg_dat['tick_size']),
                                 "tag": "string", "instrument_token": str(req_leg_dat['instrument_key']),
                                 "order_type": "LIMIT", "transaction_type": "BUY", "disclosed_quantity": 0,
                                 "trigger_price": 0, "is_amo": False}

                    print(buy_order)
                    response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=buy_order, verify=False,timeout=1)
                    print(str(response.json()))
                    sleep(0.5)


            for symbol in range(len(position)):
                position_dat = position.iloc[symbol]

                position_orderbook = main_orderbook.copy()
                position_orderbook = position_orderbook[(position_orderbook["status"] == "TP") | (position_orderbook["status"] == "O")]
                position_orderbook = position_orderbook[position_orderbook["instrument_key"] == str(position_dat["instrument_key"])]


                if len(position_orderbook)>0:
                    for positionbook_orders in range(len(position_orderbook)):
                        position_orderbook_dat = position_orderbook.iloc[positionbook_orders]
                        position_orderbook_dat['isAMO'] = position_orderbook_dat['isAMO'].tolist()
                        orderbook_json_data = {'data': {'product': position_orderbook_dat["product"],
                                                        'orderNumber': {'oms': position_orderbook_dat["orderNumber_oms"],'exchange': '', },
                                                        'isAmo': position_orderbook_dat["isAMO"], }, }
                        orderbook_cancel_order = (upstox_session.patch('https://service.upstox.com/interactive/v3/order', headers=upstox_web_headers,
                                                 json=orderbook_json_data, verify=False,timeout=1)).json()
                        print(orderbook_json_data)
                        print(orderbook_cancel_order)
                        sleep(0.5)



                position_dat_ltp = main_ltp_data.get(position_dat["instrument_token"])
                transaction_type = "BUY" if position_dat["quantity"] < 0 else "SELL"
                order = {"quantity": int(abs(position_dat["quantity"])), "product": str(position_dat["product"]),
                             "validity": "DAY", "price": getroundnumber(position_dat_ltp, (position_dat["tick_size"])),
                             "tag": "string", "instrument_token": str(position_dat["instrument_key"]),
                             "order_type": "LIMIT", "transaction_type": transaction_type, "disclosed_quantity": 0,
                             "trigger_price": 0, "is_amo": False}

                print(order)
                response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=order, verify=False,timeout=1)
                print(str(response.json()))
                sleep(0.5)

            other_orders = main_orderbook.copy()
            other_orders = other_orders[(other_orders["status"] == "TP") | (other_orders["status"] == "O")]

            required_chain_leg = chain.copy()
            other_orders = pd.merge(other_orders, required_chain_leg, on=['name', "option_type"], how='left')
            other_orders = other_orders[other_orders["chain_count"].isna()]
            # print(other_orders)



            if len(other_orders) > 0:
                for positionbook_orders in range(len(other_orders)):
                    position_orderbook_dat = other_orders.iloc[positionbook_orders]
                    position_orderbook_dat['isAMO'] = position_orderbook_dat['isAMO'].tolist()
                    orderbook_json_data = {'data': {'product': position_orderbook_dat["product"],
                                                    'orderNumber': {'oms': position_orderbook_dat["orderNumber_oms"],
                                                                    'exchange': '', },'isAmo': position_orderbook_dat["isAMO"], }, }

                    orderbook_cancel_order = (upstox_session.patch('https://service.upstox.com/interactive/v3/order', headers=upstox_web_headers,
                                             json=orderbook_json_data, verify=False, timeout=1)).json()
                    print(orderbook_json_data)
                    print(orderbook_cancel_order)
                    sleep(0.5)

        else:
            position_orderbook = main_orderbook.copy()
            position_orderbook = position_orderbook[(position_orderbook["status"] == "TP") | (position_orderbook["status"] == "O")]

            if len(position_orderbook) > 0:

                for positionbook_orders in range(len(position_orderbook)):
                    position_orderbook_dat = position_orderbook.iloc[positionbook_orders]
                    position_orderbook_dat['isAMO'] = position_orderbook_dat['isAMO'].tolist()
                    orderbook_json_data = {'data': {'product': position_orderbook_dat["product"],
                                                    'orderNumber': {'oms': position_orderbook_dat["orderNumber_oms"],
                                                                    'exchange': '', },
                                                    'isAmo': position_orderbook_dat["isAMO"], }, }

                    orderbook_cancel_order = (upstox_session.patch('https://service.upstox.com/interactive/v3/order', headers=upstox_web_headers,
                                             json=orderbook_json_data, verify=False, timeout=1)).json()
                    print(orderbook_json_data)
                    print(orderbook_cancel_order)
                    sleep(0.5)
            else:
                pass


            try:

                position = main_position.copy()
                new_sym = position["instrument_token"].unique().tolist()
                asyncio.run(add_instrument_keys(new_sym))
                sleep(0.5)

                position["BuyQty"] = pd.to_numeric(position["netInfo_buyQty"])
                position["SellQty"] = pd.to_numeric(position["netInfo_sellQty"])
                position["quantity"] = position["BuyQty"] - position["SellQty"]

                position = position[position["quantity"] != 0]

                if len(position) > 0:
                    for symbol in range(len(position)):
                        position_dat = position.iloc[symbol]

                        position_orderbook = main_orderbook.copy()
                        position_orderbook = position_orderbook[(position_orderbook["status"] == "TP") | (position_orderbook["status"] == "O")]
                        position_orderbook = position_orderbook[position_orderbook["instrument_key"] == str(position_dat["instrument_key"])]

                        if len(position_orderbook) > 0:
                            for positionbook_orders in range(len(position_orderbook)):
                                position_orderbook_dat = position_orderbook.iloc[positionbook_orders]
                                position_orderbook_dat['isAMO'] = position_orderbook_dat['isAMO'].tolist()
                                orderbook_json_data = {'data': {'product': position_orderbook_dat["product"],
                                                                'orderNumber': {
                                                                    'oms': position_orderbook_dat["orderNumber_oms"],
                                                                    'exchange': '', },
                                                                'isAmo': position_orderbook_dat["isAMO"], }, }
                                orderbook_cancel_order = (upstox_session.patch('https://service.upstox.com/interactive/v3/order',
                                                         headers=upstox_web_headers,
                                                         json=orderbook_json_data, verify=False, timeout=1)).json()
                                print(orderbook_json_data)
                                print(orderbook_cancel_order)
                                sleep(0.5)

                        position_dat_ltp = main_ltp_data.get(position_dat["instrument_token"])
                        transaction_type = "BUY" if position_dat["quantity"] < 0 else "SELL"
                        order = {"quantity": int(abs(position_dat["quantity"])), "product": str(position_dat["product"]),
                                 "validity": "DAY", "price": getroundnumber(position_dat_ltp, (position_dat["tick_size"])),
                                 "tag": "string", "instrument_token": str(position_dat["instrument_key"]),
                                 "order_type": "LIMIT", "transaction_type": transaction_type, "disclosed_quantity": 0,
                                 "trigger_price": 0, "is_amo": False}

                        print(order)
                        response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=order,
                                                 verify=False, timeout=1)
                        print(str(response.json()))
                        sleep(0.5)
                    else:
                        pass
                else:
                    pass

            except Exception as error:
                print("error3" + str(error))
                pass

    except Exception as error:
        print("clear_positions " + str(error))


def create_order():
    global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
        checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
        main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
        upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser

    while not stop_flag.is_set():
        try:

            sleep(0.5)
            if main_ltp_data:
                # print(main_ltp_data)

                fut_instr['index_ltp'] = fut_instr['index_token'].map(main_ltp_data)
                opt_df = OPT_instr.merge(fut_instr, left_on='name', right_on='name')
                opt_df["stkdiff"] = abs(opt_df["strike"] - opt_df["index_ltp"])
                opt_df = opt_df.groupby('name').apply(lambda x: x.nsmallest(chain_lenth, 'stkdiff')).reset_index(drop=True)

                new_sym = opt_df["instrument_token"].unique().tolist()
                asyncio.run(add_instrument_keys(new_sym))
                sleep(0.5)

                opt_df = create_chain(opt_df)

                clear_positions()

                opt_df = opt_df[opt_df["chain_count"] <= -3]
                opt_df = opt_df[opt_df["chain_count"] >= -8]
                opt_df['day_volume'] = opt_df['instrument_token'].map(main_volume_data)
                opt_df = opt_df.sort_values(by=['name','option_type','day_volume', 'chain_count'], ascending=[False,False,False,True]).reset_index(drop=True)
                opt_df['count'] = opt_df.groupby(["name", "option_type"]).cumcount()
                opt_df = opt_df[opt_df["count"] == 0]
                # print(opt_df)

                checkbook = pd.concat([checkorderbook, checkpositionbook], ignore_index=True).drop_duplicates()
                # print(checkbook)
                merge_cols = ["name", "option_type"]
                opt_df = pd.concat([checkbook, opt_df], ignore_index=True).drop_duplicates(merge_cols)
                opt_df.drop_duplicates(["name", "option_type"])
                opt_df.dropna(subset=['instrument_key',"exchange_token"], inplace=True)

                # print(opt_df)

                for opt_df_token in range(len(opt_df)):
                    opt_df_dat = opt_df.iloc[opt_df_token]
                    # print(opt_df_dat)


                    data = get_data(opt_df_dat["instrument_token"])
                    sell_trigger_price = data.iloc[-1]['Low']
                    sell_trigger_price = getroundnumber(sell_trigger_price - (opt_df_dat["tick_size"]),(opt_df_dat["tick_size"]))
                    sell_price = getroundnumber(sell_trigger_price - (opt_df_dat["tick_size"]),(opt_df_dat["tick_size"]))

                    nse_timming = (time(9, 20) <= datetime.now().time() <= time(15, 10))
                    timing_condition = nse_timming

                    key_to_check = (opt_df_dat["instrument_token"])
                    is_key_present = key_to_check in main_ltp_data

                    if timing_condition and is_key_present:
                        # print(opt_df_dat["instrument_token"])

                        ltp = main_ltp_data.get((opt_df_dat["instrument_token"]))
                        # margins = float(get_upsotx_margin_sec()) + 200000
                        # quantity = get_margin_req((opt_df_dat.exchange_token), (opt_df_dat.exchange), int(opt_df_dat.lot_size),
                        #                           (ltp), "S", margins)

                        quantity = int(opt_df_dat["lot_size"]*no_of_lots)
                        if ltp > sell_trigger_price and quantity > 0:
                            sell_order = {"quantity": quantity, "product": "I", "validity": "DAY",
                                         "price": getroundnumber(sell_price,(opt_df_dat["tick_size"])),
                                         "tag": "string", "instrument_token": str(opt_df_dat["instrument_key"]),
                                         "order_type": "SL", "transaction_type": "SELL", "disclosed_quantity": 0,
                                         "trigger_price": getroundnumber(sell_trigger_price,(opt_df_dat.tick_size)), "is_amo": False}

                            print(sell_order)
                            response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers,json=sell_order, verify=False,timeout=1)
                            print(str(response.json()))
                            sleep(0.5)

                        else:
                            print(ltp)
                            print(sell_trigger_price)

                    else:
                        pass

                for opt_df_token in range(len(opt_df)):
                    opt_df_dat = opt_df.iloc[opt_df_token]
                    # print(opt_df_dat)
                    instrument_token = str(opt_df_dat.instrument_key)
                    fav_response = upstox_session.post('https://service.upstox.com/watchlists/v1/user/Favourites',
                                                       headers=upstox_web_headers,
                                                       json={'data': {'items': [instrument_token, ], }, }, verify=False,timeout=1)
                    # print(fav_response.json())
                    sleep(0.5)

            else:
                sleep(1)


        except Exception as error:
            print("create_order " + str(error))
            stop_flag.set()









def pending_position():
    global enc_token, websocket, instrument_keys, peing_count, checkorderbook, \
        checkpositionbook, main_orderbook, main_ltp_data, main_volume_data, chain, instrumentslist, \
        main_position, s, websocket_2, upstox_headers, access_token, upstox_cookie, \
        upstox_session, upstox_web_headers, web_access_token, OPT_instr, fut_instr, main_margin, no_of_lots, browser

    while not stop_flag.is_set():
        try:

            sleep(0.5)
            if main_ltp_data:
                position = (upstox_session.get('https://service.upstox.com/portfolio/v2/positions', headers=upstox_web_headers, verify=False,timeout=1)).json()

                if str(position["success"]) == "True":
                    position = position["data"]["list"]

                    if len(position) > 0:
                        position = json_normalize(position, sep='_')
                        position = pd.DataFrame(position)

                        main_position = pd.merge(position, instrumentslist, on=['instrument_key', "instrument_key"],how='inner')

                        new_sym = main_position["instrument_token"].unique().tolist()
                        asyncio.run(add_instrument_keys(new_sym))
                        sleep(0.5)

                        position = main_position.copy()

                        req_chain = chain.copy()
                        req_chain = req_chain[["instrument_key", "chain_count"]]
                        position = pd.merge(position, req_chain, on=['instrument_key', "instrument_key"], how='left')

                        position["BuyQty"] = pd.to_numeric(position["netInfo_buyQty"])
                        position["SellQty"] = pd.to_numeric(position["netInfo_sellQty"])
                        position["quantity"] = position["BuyQty"] - position["SellQty"]

                        position = position[position["quantity"] < 0]
                        position = position[position["chain_count"] < 0]
                        position['count'] = position.groupby(["name", "option_type"]).cumcount()
                        checkpositionbook = position[["name", "option_type"]]

                        nse_timming = (time(9, 20) <= datetime.now().time() <= time(15, 10))

                        if nse_timming:
                            for symbol in range(len(position)):
                                position_dat = position.iloc[symbol]

                                position_orderbook = main_orderbook.copy()
                                position_orderbook = position_orderbook[(position_orderbook["status"] == "TP") | (position_orderbook["status"] == "O")]
                                position_orderbook = position_orderbook[(position_orderbook["side"] == "B")]
                                position_orderbook = position_orderbook[position_orderbook["instrument_key"] == position_dat["instrument_key"]]
                                position_orderbook['count'] = position_orderbook.groupby(["instrument_key"]).cumcount()


                                position_data = get_data(position_dat["instrument_token"])
                                position_dat_ltp = main_ltp_data.get(position_dat["instrument_token"])

                                position_low = getroundnumber(position_data.iloc[-1]['Low'],(position_dat["tick_size"]))
                                position_high = getroundnumber((position_data.iloc[-1]['High'] + ((position_dat["tick_size"])*2)),(position_dat["tick_size"]))
                                position_target = getroundnumber(max((position_low - ((position_high - position_low)*5)),0.1),(position_dat["tick_size"]))

                                if position_dat["count"] < 1 :
                                    if position_dat_ltp <= position_low <= position_high :
                                        sl_value = position_target
                                    elif position_low <= position_dat_ltp <= position_high :
                                        sl_value = position_high
                                    elif position_low <= position_high <= position_dat_ltp :
                                        sl_value = position_dat_ltp
                                    else:
                                        sl_value = position_dat_ltp
                                else:
                                    sl_value = position_dat_ltp

                                sl_trigger_value = getroundnumber(sl_value - (position_dat["tick_size"]),(position_dat["tick_size"]))

                                if position_orderbook.empty:
                                    try:
                                        if sl_value == position_high:
                                            buy_order = {"quantity"    : int(abs(position_dat["quantity"])), "product": str(position_dat["product"]),
                                                          "validity": "DAY","price": getroundnumber(sl_value,(position_dat["tick_size"])),
                                                         "tag"          : "string", "instrument_token": str(position_dat["instrument_key"]),
                                                         "order_type"   : "SL", "transaction_type": "BUY", "disclosed_quantity": 0,
                                                         "trigger_price": getroundnumber(sl_trigger_value,(position_dat["tick_size"])), "is_amo": False}

                                            print(buy_order)
                                            response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=buy_order)
                                            print(str(response.json()))
                                            sleep(0.5)

                                        elif sl_value == position_dat_ltp:
                                            buy_order = {"quantity"     : int(abs(position_dat["quantity"])), "product": str(position_dat["product"]),
                                                          "validity": "DAY","price": getroundnumber(position_dat_ltp,(position_dat["tick_size"])),
                                                          "tag"          : "string", "instrument_token": str(position_dat["instrument_key"]),
                                                          "order_type"   : "LIMIT", "transaction_type": "BUY", "disclosed_quantity": 0,
                                                          "trigger_price": 0, "is_amo": False}

                                            print(buy_order)
                                            response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=buy_order)
                                            print(str(response.json()))
                                            sleep(0.5)


                                        elif sl_value == position_target:
                                            buy_order = {"quantity": int(abs(position_dat["quantity"])), "product": str(position_dat["product"]),
                                                          "validity": "DAY","price": getroundnumber(position_target,(position_dat["tick_size"])),
                                                          "tag": "string", "instrument_token": str(position_dat["instrument_key"]),
                                                          "order_type": "LIMIT", "transaction_type": "BUY", "disclosed_quantity": 0,
                                                          "trigger_price": 0, "is_amo": False}

                                            print(buy_order)
                                            response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=buy_order)
                                            print(str(response.json()))
                                            sleep(0.5)

                                        else:
                                            buy_order = {"quantity": int(abs(position_dat["quantity"])), "product": str(position_dat["product"]),
                                                         "validity": "DAY", "price": getroundnumber(position_dat_ltp, (position_dat["tick_size"])),
                                                         "tag": "string", "instrument_token": str(position_dat["instrument_key"]),
                                                         "order_type": "LIMIT", "transaction_type": "BUY", "disclosed_quantity": 0,
                                                         "trigger_price": 0, "is_amo": False}

                                            print(buy_order)
                                            response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=buy_order)
                                            print(str(response.json()))
                                            sleep(0.5)

                                    except Exception as error:
                                        print("error4" + str(error))
                                        pass


                                else:
                                    try:
                                        for positionbook_orders in range(len(position_orderbook)):
                                            position_orderbook_dat = position_orderbook.iloc[positionbook_orders]
                                            old_price_limit = getroundnumber(position_orderbook_dat['price_limit'],(position_dat["tick_size"]))

                                            if position_orderbook_dat['count'] > 0 or position_dat["quantity"] != position_orderbook_dat["quantity"] or old_price_limit != sl_value:

                                                position_orderbook_dat['isAMO'] = position_orderbook_dat['isAMO'].tolist()
                                                orderbook_json_data = {'data': {'product': position_orderbook_dat["product"], 'orderNumber': {'oms': position_orderbook_dat["orderNumber_oms"], 'exchange': '', }, 'isAmo': position_orderbook_dat["isAMO"], }, }
                                                orderbook_cancel_order = (upstox_session.patch('https://service.upstox.com/interactive/v3/order', headers=upstox_web_headers, json=orderbook_json_data, verify=False,timeout=1)).json()
                                                print(orderbook_json_data)
                                                print(orderbook_cancel_order)
                                                sleep(0.5)


                                                if sl_value == position_high:
                                                    buy_order = {"quantity": int(abs(position_dat["quantity"])),
                                                                  "product": str(position_dat["product"]),
                                                                  "validity": "DAY",
                                                                  "price": getroundnumber(sl_value,(position_dat["tick_size"])),
                                                                  "tag": "string",
                                                                  "instrument_token": str(position_dat["instrument_key"]),
                                                                  "order_type": "SL", "transaction_type": "BUY",
                                                                  "disclosed_quantity": 0,
                                                                  "trigger_price": getroundnumber(order_sl_trigger_value,(position_dat["tick_size"])),
                                                                  "is_amo": False}

                                                    print(buy_order)
                                                    response = requests.post('https://api.upstox.com/v2/order/place',
                                                                             headers=upstox_headers, json=buy_order)
                                                    print(str(response.json()))
                                                    sleep(0.5)

                                                elif sl_value == position_dat_ltp:
                                                    buy_order = {"quantity"     : int(abs(position_dat["quantity"])), "product": str(position_dat["product"]),
                                                                  "validity": "DAY","price": getroundnumber(position_dat_ltp,(position_dat["tick_size"])),
                                                                  "tag": "string", "instrument_token": str(position_dat["instrument_key"]),
                                                                  "order_type":"LIMIT", "transaction_type": "BUY", "disclosed_quantity": 0,
                                                                  "trigger_price": 0, "is_amo": False}

                                                    print(buy_order)
                                                    response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=buy_order)
                                                    print(str(response.json()))
                                                    sleep(0.5)


                                                elif sl_value == position_target:
                                                    buy_order = {"quantity": int(abs(position_dat["quantity"])),
                                                                  "product": str(position_dat["product"]), "validity": "DAY",
                                                                  "price": getroundnumber(position_target,(position_dat["tick_size"])),
                                                                  "tag": "string",
                                                                  "instrument_token": str(position_dat["instrument_key"]),
                                                                  "order_type": "LIMIT", "transaction_type": "BUY",
                                                                  "disclosed_quantity": 0,
                                                                  "trigger_price": 0, "is_amo": False}

                                                    print(buy_order)
                                                    response = requests.post('https://api.upstox.com/v2/order/place',headers=upstox_headers, json=buy_order)
                                                    print(str(response.json()))
                                                    sleep(0.5)

                                                else:

                                                    buy_order = {"quantity": int(abs(position_dat["quantity"])), "product": str(position_dat["product"]),
                                                                 "validity": "DAY", "price": getroundnumber(position_dat_ltp, (position_dat["tick_size"])),
                                                                 "tag": "string", "instrument_token": str(position_dat["instrument_key"]),
                                                                 "order_type": "LIMIT", "transaction_type": "BUY", "disclosed_quantity": 0,
                                                                 "trigger_price": 0, "is_amo": False}

                                                    print(buy_order)
                                                    response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=buy_order)
                                                    print(str(response.json()))
                                                    sleep(0.5)
                                            else:
                                                pass

                                    except Exception as error:
                                        print("error5 " + str(error))
                                        pass

                            else:
                                pass
                        else:
                            pass
                    else:
                        pass
                else:
                    stop_flag.set()
            else:
                pass


        except Exception as error:
            print("pending_position " + str(error))
            stop_flag.set()





def makelogins():
    try:

        zerodha_login()
        upstox_login()
        getinstruments()


    except Exception as error:
        print("makelogins " + str(error))
        sleep(0.5)
        makelogins()



def run_websocket():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(connect_websocket())
    finally:
        loop.close()
        stop_flag.set()












while True:
    try:

        stop_flag = Event()
        while not stop_flag.is_set():


            t0 = Thread(target=run_websocket)
            t1 = Thread(target=create_order)
            t2 = Thread(target=pending_orders)
            t3 = Thread(target=pending_position)


            try:

                main_volume_data = {}
                main_ltp_data = {}
                makelogins()
                t0.start()
                sleep(5)
                t1.start()
                t2.start()
                t3.start()

                t0.join()
                t1.join()
                t2.join()
                t3.join()


            except Exception as error:
                print("main_1 " + str(error))

    except Exception as error:
        print("main_2 " + str(error))




