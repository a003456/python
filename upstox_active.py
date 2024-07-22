import threading
import asyncio
import pytz
import ssl
import upstox_client
import websockets
from google.protobuf.json_format import MessageToDict
from threading import Thread
import MarketDataFeed_pb2 as pb
from time import sleep
from seleniumbase import Driver
from pandas import json_normalize
from selenium.webdriver.common.by import By
import re
import warnings
import time
import json
import os
from datetime import time
from datetime import timedelta
from urllib.parse import quote
import pyotp
import pandas as pd
import requests
from datetime import datetime




global market_data_df,access_token,OPT_instr

checkorderbook = pd.DataFrame(columns=["name", "option_type"])
checkpositionbook = pd.DataFrame(columns=["name", "option_type"])
main_orderbook = pd.DataFrame()
market_data_df = pd.DataFrame()
websocket_conn = None
instrument_keys = ["NSE_INDEX|Nifty Bank", "NSE_INDEX|Nifty 50", "NSE_INDEX|Nifty Fin Service"]




os.chdir("D:\\algo_trading\\option_buy_algo")
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)
pd.set_option('display.max_rows', None)
warnings.filterwarnings('ignore')



def extract_before_number(s):
    match = re.match(r"^[^\d]*", s)
    if match:
        return match.group(0)
    return ""



def getinstruments():
    try:
        global instrumentslist, OPT_instr
        print("downloading instruments wait.............")

        url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
        instrumentslist = pd.read_csv(url, dtype={'exchange_token': str}, compression='gzip')
        instrumentslist['name'] = instrumentslist.apply(
            lambda row: extract_before_number(row['tradingsymbol']) if pd.isna(row['name']) else row['name'], axis=1)

        OPT_instr = instrumentslist[instrumentslist['exchange'] == 'NSE_FO']
        OPT_instr = OPT_instr[OPT_instr['name'].isin(["NIFTY", "FINNIFTY", "BANKNIFTY"])]
        OPT_instr = OPT_instr[OPT_instr['option_type'].isin(["CE", "PE"])]


        OPT_instr['expiry'] = pd.to_datetime(OPT_instr['expiry'])
        OPT_instr = OPT_instr[OPT_instr['expiry'] > (datetime.now() - timedelta(days=1))]
        OPT_instr = OPT_instr[OPT_instr['expiry'] == OPT_instr.groupby('name')['expiry'].transform('min')]
        OPT_instr = OPT_instr[OPT_instr['expiry'] == OPT_instr.groupby('exchange')['expiry'].transform('min')]


        print("downloaded instruments.................")

    except:
        print("downloading instruments failed................")
        time.sleep(5)
        getinstruments()



getinstruments()



def upstox_login():
    global upstox_headers,access_token,upstox_cookie,upstox_session,upstox_web_headers,web_access_token
    try:
        API_KEY = '67ff0f62-a07c-4a5b-8959-5a93b5ed3921'
        SECRET_KEY = 'ahhmqrt7tj'
        RURL = 'https://127.0.0.1:5000/'
        mobile_no = "8850908394"
        pinCode = "121093"

        rurlEncode = quote(RURL, safe="")
        baseurl = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={rurlEncode}'
        print(baseurl)
        # webbrowser.open(baseurl)

        browser = Driver()
        sleep(0.2)

        # browser = webdriver.Chrome()

        browser.get(baseurl)
        # browser.execute_script("window.open()")

        sleep(0.2)
        browser.find_element(By.ID, "mobileNum").send_keys(mobile_no)
        browser.find_element(By.ID, "getOtp").click()


        sleep(0.2)
        totp = pyotp.TOTP("HCIW5A3PZLR4TEA4H3TVM4SSYD26TH3L")
        twofa = totp.now()
        twofa = str(twofa[0:3]) + "-" + str(twofa[3:6])
        browser.find_element(By.ID, "otpNum").click()
        browser.find_element(By.ID, "otpNum").send_keys(twofa)
        browser.find_element(By.ID, "continueBtn").click()
        sleep(0.2)


        browser.find_element(By.ID, "pinCode").send_keys(pinCode)
        browser.find_element(By.ID, "pinContinueBtn").click()

        sleep(0.5)
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


    except Exception as error01:
        print("error01" + str(error01))
        browser.quit()
        sleep(5)
        upstox_login()


upstox_login()




def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    api_instance = upstox_client.WebsocketApi(
        upstox_client.ApiClient(configuration))
    api_response = api_instance.get_market_data_feed_authorize(api_version)
    return api_response





def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response




async def subscribe_to_instruments(instrument_keys):
    global websocket_conn
    data = {
        "guid": "someguid",
        "method": "sub",
        "data": {
            "mode": "full",
            "instrumentKeys": instrument_keys
        }
    }

    # Convert data to binary and send over WebSocket
    binary_data = json.dumps(data).encode('utf-8')
    await websocket_conn.send(binary_data)


async def fetch_market_data():
    global market_data_df,access_token,websocket_conn,web_access_token,instrument_keys
    """Fetch market data using WebSocket and print it."""

    try:

        # Create default SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Configure OAuth2 access token for authorization
        configuration = upstox_client.Configuration()

        api_version = '2.0'
        # print(access_token)
        configuration.access_token = access_token

        # Get market data feed authorization
        response = get_market_data_feed_authorize(
            api_version, configuration)
        # print(response)

        # Connect to the WebSocket with SSL context
        async with websockets.connect(response.data.authorized_redirect_uri, ssl=ssl_context) as websocket:


            websocket_conn = websocket
            print('Connection established')

            await asyncio.sleep(0.2)  # Wait for 1 second

            await subscribe_to_instruments(instrument_keys)


            try:
                while True:
                    message = await websocket.recv()
                    decoded_data = decode_protobuf(message)

                    # Convert the decoded data to a dictionary
                    market_data_dict = MessageToDict(decoded_data)

                    ltp_data = []
                    for key, value in market_data_dict['feeds'].items():
                        ff_data = value.get('ff', {})
                        index_ff = ff_data.get('indexFF', {})
                        market_ff = ff_data.get('marketFF', {})

                        if 'ltpc' in index_ff:
                            ltp = index_ff['ltpc']['ltp']
                        elif 'ltpc' in market_ff:
                            ltp = market_ff['ltpc']['ltp']
                        else:
                            ltp = None

                        if ltp is not None:
                            ltp_data.append({'name': key, 'ltp': ltp})

                    replacements = {
                        'NSE_INDEX|Nifty Bank': 'BANKNIFTY',
                        'NSE_INDEX|Nifty 50': 'NIFTY',
                        'NSE_INDEX|Nifty Fin Service': 'FINNIFTY'
                    }

                    for item in ltp_data:
                        item['name'] = replacements.get(item['name'], item['name'])
                    market_data_df = pd.DataFrame(ltp_data)
                    # print(market_data_df)

            except asyncio.CancelledError:
                # Perform cleanup or termination tasks here
                print("WebSocket connection is being gracefully shutdown.")
                await websocket.close()

    except Exception as EE:
        print(EE)
        websocket_thread = Thread(target=run_websocket)
        websocket_thread.start()


def run_websocket():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(fetch_market_data())


websocket_thread = Thread(target=run_websocket)
websocket_thread.start()
sleep(2)





async def add_instrument_keys(new_keys):
    global instrument_keys
    try:
        keys_to_add = [key for key in new_keys if key not in instrument_keys]

        if keys_to_add:
            instrument_keys.extend(keys_to_add)
            instrument_keys = list(dict.fromkeys(instrument_keys))
            await asyncio.create_task(subscribe_to_instruments(instrument_keys))
            print("keys added" + str(keys_to_add))


    except Exception as e:
        print(f"Error adding keys: {e}")



#
# while True:
#     if not market_data_df.empty:
#         print(market_data_df)
#         new_keys = ['NSE_FO|50957', 'NSE_FO|50952']
#         asyncio.run(add_instrument_keys(new_keys))
#         sleep(2)
#     else:
#         time(2)


def getroundnumber(number, nn):
    # Calculate the rounded value
    rounded_value = round(number / nn) * nn

    # Convert the rounded value to an integer and return it
    return int(rounded_value)


def get_data(instrument_key):
    instrument_key = quote(instrument_key, safe="")
    url = f'https://service.upstox.com/charts/v2/open/intraday/IN/{instrument_key}/1minute'
    # url = f'https://service.upstox.com/charts/v2/open/historical/IN/{instrument_key}/1minute/2024-07-19'
    response = (upstox_session.get(url, headers=upstox_web_headers, verify=False)).json()
    if response["status"] == "OK":
        df = pd.DataFrame(response["data"]["candles"])
        df.columns = ["Datetime", "Open", "High", "Low", "Close", "Volume", "oi"]
        df['Datetime'] = pd.to_datetime(df['Datetime'], unit='ms')
        df['Datetime'] = df['Datetime'].dt.tz_localize(pytz.utc)
        ist = pytz.timezone('Asia/Kolkata')
        df['Datetime'] = df['Datetime'].dt.tz_convert(ist)
        df = df.sort_values(by='Datetime')
        df = df.tail(50).reset_index(drop=True)
        # print(df)
        return df
    else:
        return pd.DataFrame()



def get_margin_req(token,exchange,quantity,price,side,margins):
    global upstox_web_headers,upstox_session

    url = f'https://service.upstox.com/brokerage-margin-calculator/calculator/v3/charges-margin?token={token}&exchange={exchange}&quantity={quantity}&price={price}&side={side}&plan=basicn1-d0&product=I&requestType=MARGIN'

    print(url)
    response = (upstox_session.get(url, headers=upstox_web_headers, verify=False)).json()
    print(response)
    if response["success"] == str(True):
        print(response)
        # print(df)
        return response
    else:
        return pd.DataFrame()



def get_upsotx_margin_sec():
    margins = (upstox_session.get('https://service.upstox.com/limit/v3/sec', headers=upstox_web_headers, verify=False)).json()['data']['SEC']['cash']['available_to_trade']
    margins = pd.DataFrame(margins)
    margins = margins.loc['total', 'total']
    return margins





def remove_duplicates_preserve_order(seq):
    seen = set()
    return [x for x in seq if not (x in seen or seen.add(x))]



def pending_orders():
    global checkorderbook
    try:
        while True:
            sleep(0.5)
            orderbook_url = 'https://service.upstox.com/portfolio/v2/orderbook?includeTsl=true'
            orderbook = (upstox_session.get(orderbook_url, headers=upstox_web_headers, verify=False)).json()

            if str(orderbook["success"]) == "True":
                orderbook = orderbook["data"]["history"]
                if len(orderbook) > 0:
                    orderbook = json_normalize(orderbook, sep='_')
                    orderbook = pd.DataFrame(orderbook)

                    main_orderbook = pd.merge(orderbook, instrumentslist, on=['instrument_key', "instrument_key"], how='inner')
                    # print(main_orderbook.head(5))
                    orderbook = main_orderbook[(main_orderbook["status"] == "TP") | (main_orderbook["status"] == "O")]
                    orderbook = orderbook[(orderbook["side"] == "S")]
                    checkorderbook = orderbook[["name", "option_type"]]
                    new_sym = remove_duplicates_preserve_order(orderbook["instrument_key"].tolist())
                    asyncio.run(add_instrument_keys(new_sym))

                    orderbook['count'] = orderbook.groupby(["name", "option_type"]).cumcount()
                    for orderbook_orders in range(len(orderbook)):
                        orderbook_dat = orderbook.iloc[orderbook_orders]
                        try:

                            orderbook_data = get_data(str(orderbook_dat["instrument_key"]))
                            orderbook_data = orderbook_data.tail(50)

                            sell_trigger_price = orderbook_data.iloc[-1]['Low']
                            sell_trigger_price = getroundnumber(sell_trigger_price - (orderbook_dat["tick_size"]),(orderbook_dat["tick_size"]))
                            sell_price = getroundnumber(sell_trigger_price - (orderbook_dat["tick_size"]),(orderbook_dat["tick_size"]))
                            nse_timming = (time(9, 20) <= datetime.now().time() <= time(22, 10))



                            orderbook_condition1 = orderbook_dat["count"] > 0
                            orderbook_condition2 = getroundnumber(orderbook_dat["price_trigger"],(orderbook_dat["tick_size"])) != getroundnumber(sell_trigger_price,(orderbook_dat["tick_size"]))
                            orderbook_ltp = market_data_df.loc[market_data_df['name'] == str(orderbook_dat.instrument_key), 'ltp'].values[0]
                            orderbook_condition3 = getroundnumber(orderbook_ltp,(orderbook_dat["tick_size"])) < getroundnumber(orderbook_dat["price_trigger"],(orderbook_dat["tick_size"]))
                            orderbook_timing_condition =  not nse_timming


                            try:
                                if orderbook_condition1 or orderbook_condition2 or orderbook_condition3 or orderbook_timing_condition :
                                    orderbook_dat['isAMO'] = orderbook_dat['isAMO'].tolist()
                                    orderbook_json_data = {'data': {'product': orderbook_dat["product"], 'orderNumber': {'oms': orderbook_dat["orderNumber_oms"], 'exchange': '', }, 'isAmo': orderbook_dat["isAMO"], }, }
                                    orderbook_cancel_order = (upstox_session.patch('https://service.upstox.com/interactive/v3/order', headers=upstox_web_headers, json=orderbook_json_data, verify=False)).json()
                                    print(orderbook_cancel_order)

                            except Exception as error4:
                                print("error4" + str(error4))
                                pass

                        except Exception as error5:
                            print("error5" + str(error5))
                            orderbook_dat['isAMO'] = orderbook_dat['isAMO'].tolist()
                            orderbook_json_data = {'data': {'product': orderbook_dat["product"], 'orderNumber': {'oms': orderbook_dat["orderNumber_oms"], 'exchange': '', }, 'isAmo': orderbook_dat["isAMO"], }, }
                            orderbook_cancel_order = (upstox_session.patch('https://service.upstox.com/interactive/v3/order', headers=upstox_web_headers, json=orderbook_json_data, verify=False)).json()
                            print(orderbook_cancel_order)
                            pass

            elif str(main_orderbook["success"]) == "False":
                pass
            else:
                pass


    except Exception as error6:
        print("error6 " + str(error6))
        pass






def create_order():
    global checkpositionbook, checkorderbook
    try:
        while True:
            sleep(0.5)
            if not market_data_df.empty:

                opt_df = OPT_instr.merge(market_data_df, left_on='name', right_on='name')
                opt_df["stkdiff"] = abs(opt_df["strike"] - opt_df["ltp"])
                opt_df = opt_df.groupby('name').apply(lambda x: x.nsmallest(10, 'stkdiff')).reset_index(drop=True)

                # new_sym = remove_duplicates_preserve_order(opt_df["instrument_key"].tolist())
                # asyncio.run(add_instrument_keys(new_sym))
                # sleep(2)

                time_ranges = {(time(2, 35), time(10, 30)): 2, (time(10, 31), time(11, 30)): 2,
                               (time(11, 31), time(12, 30)): 2, (time(12, 31), time(13, 30)): 2,
                               (time(13, 31), time(15, 30)): 2, (time(15, 31), time(23, 30)): 2, }

                stkselect = next((value for ranges, value in time_ranges.items() if ranges[0] <= (datetime.now().time()) <= ranges[1]), 2)
                ce_opt_df = opt_df[opt_df["option_type"] == "CE"]
                ce_opt_df = ce_opt_df.groupby(['name']).apply(lambda x: x.nsmallest(stkselect, 'strike')).reset_index(drop=True)
                ce_opt_df = ce_opt_df.groupby(['name']).apply(lambda x: x.nlargest(1, 'strike')).reset_index(drop=True)

                pe_opt_df = opt_df[opt_df["option_type"] == "PE"]
                pe_opt_df = pe_opt_df.groupby(['name']).apply(lambda x: x.nlargest(stkselect, 'strike')).reset_index(drop=True)
                pe_opt_df = pe_opt_df.groupby(['name']).apply(lambda x: x.nsmallest(1, 'strike')).reset_index(drop=True)
                opt_df = pd.concat([pe_opt_df, ce_opt_df])



                new_sym = remove_duplicates_preserve_order(opt_df["instrument_key"].tolist())
                asyncio.run(add_instrument_keys(new_sym))

                checkbook = pd.concat([checkorderbook, checkpositionbook], ignore_index=True).drop_duplicates()
                merge_cols = ["name", "option_type"]
                opt_df = pd.concat([checkbook, opt_df], ignore_index=True).drop_duplicates(merge_cols)
                opt_df.drop_duplicates(["name", "option_type"])
                opt_df.dropna(subset=['instrument_key',"exchange_token"], inplace=True)


                for opt_df_token in range(len(opt_df)):
                    opt_df_dat = opt_df.iloc[opt_df_token]
                    # print(opt_df_dat)


                    data = get_data(str(opt_df_dat.instrument_key))
                    sell_trigger_price = data.iloc[-1]['Low']
                    sell_trigger_price = getroundnumber(sell_trigger_price - (opt_df_dat.tick_size),(opt_df_dat.tick_size))
                    sell_price = getroundnumber(sell_trigger_price - (opt_df_dat.tick_size),(opt_df_dat.tick_size))
                    nse_timming = (time(9, 20) <= datetime.now().time() <= time(22, 10))



                    timing_condition = nse_timming
                    new_sym = opt_df["instrument_key"].tolist()
                    is_in_list = str(opt_df_dat.instrument_key) in new_sym

                    if timing_condition and is_in_list:
                        print(str(opt_df_dat.instrument_key))
                        ltp = market_data_df.loc[market_data_df['name'] == str(opt_df_dat.instrument_key), 'ltp'].values[0]


                        margins = float(get_upsotx_margin_sec()) + 200000
                        # quantity = get_margin_req((opt_df_dat.exchange_token), (opt_df_dat.exchange), int(opt_df_dat.lot_size),
                        #                           (ltp), "S", margins)

                        quantity = int(1)
                        if ltp > sell_trigger_price and quantity > 0:
                            sell_order = {"quantity": int(1), "product": "I", "validity": "DAY",
                                         "price": getroundnumber(sell_price,(opt_df_dat.tick_size)),
                                         "tag": "string", "instrument_token": str(opt_df_dat.instrument_key),
                                         "order_type": "SL", "transaction_type": "SELL", "disclosed_quantity": 0,
                                         "trigger_price": getroundnumber(sell_trigger_price,(opt_df_dat.tick_size)), "is_amo": True}

                            print(sell_order)
                            response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers,json=sell_order)
                            print(str(response.json()))

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

            else:
                sleep(1)

    except Exception as EE:
        print(EE)








def pending_position():
    global checkpositionbook
    try:
        while True:
            print(market_data_df)
            sleep(0.5)
            if not market_data_df.empty:

                main_position = (upstox_session.get('https://service.upstox.com/portfolio/v2/positions', headers=upstox_web_headers, verify=False)).json()
                # print(main_position)
                if str(main_position["success"]) == "True":
                    position = main_position["data"]["listV3"]

                    if len(position) > 0:
                        position = json_normalize(position, sep='_')
                        position = pd.DataFrame(position)
                        # print(position)

                        new_sym = remove_duplicates_preserve_order(position["instrument_key"].tolist())
                        asyncio.run(add_instrument_keys(new_sym))

                        position["BuyQty"] = pd.to_numeric(position["netInfoV3_buyQty"]) #change column name
                        position["SellQty"] = pd.to_numeric(position["netInfoV3_sellQty"]) #change column name
                        position["quantity"] = position["BuyQty"] - position["SellQty"]


                        buyposition = position[position["quantity"] > 0]
                        checkpositionbook = position[["name", "option_type"]]

                        if len(buyposition)>0:
                            for symbol in range(len(position)):
                                position_dat = position.iloc[symbol]
                                new_sym = market_data_df["name"].tolist()
                                is_in_list = str(position_dat.instrument_key) in new_sym

                                if is_in_list:
                                    sell_ltp = market_data_df.loc[market_data_df['name'] == str(position_dat.instrument_key), 'ltp'].values[0]
                                    position_orderbook = main_orderbook[(main_orderbook["status"] == "TP") | (main_orderbook["status"] == "O")]
                                    position_orderbook = position_orderbook[(position_orderbook["side"] == "S")]
                                    position_orderbook = position_orderbook[(position_orderbook["instrument_key"] == str(position_dat["instrument_key"]))]


                                    if len(position_orderbook)>0:
                                        for positionbook_orders in range(len(position_orderbook)):
                                            positionbook_dat = position_orderbook.iloc[positionbook_orders]
                                            positionbook_dat['isAMO'] = positionbook_dat['isAMO'].tolist()
                                            orderbook_json_data = {'data': {'product': positionbook_dat["product"],
                                                                            'orderNumber': {
                                                                                'oms': positionbook_dat["orderNumber_oms"],
                                                                                'exchange': '', },
                                                                            'isAmo': positionbook_dat["isAMO"], }, }
                                            orderbook_cancel_order = (
                                                upstox_session.patch('https://service.upstox.com/interactive/v3/order',
                                                                     headers=upstox_web_headers, json=orderbook_json_data,
                                                                     verify=False)).json()
                                            print(orderbook_cancel_order)

                                    sale_order = {"quantity": int(position_dat["quantity"]),
                                                  "product": str(position_dat["product"]),
                                                  "validity": "DAY",
                                                  "price": getroundnumber(sell_ltp, (position_dat["tick_size"])),
                                                  "tag": "string", "instrument_token": str(position_dat["instrument_key"]),
                                                  "order_type": "LIMIT", "transaction_type": "BUY", "disclosed_quantity": 0,
                                                  "trigger_price": 0, "is_amo": False}

                                    # print(sale_order)
                                    response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers,
                                                             json=sale_order)
                                    print(str(response.json()))

                                else:
                                    pass


                        else:
                            position = position[position["quantity"] < 0]
                            position = pd.merge(position, instrumentslist, on=['instrument_key', "instrument_key"],how='inner')

                            checkpositionbook = position[["name", "instrument_type"]]
                            for symbol in range(len(position)):
                                position_dat = position.iloc[symbol]
                                position_orderbook = main_orderbook[(main_orderbook["status"] == "TP") | (main_orderbook["status"] == "O")]
                                position_orderbook = position_orderbook[(position_orderbook["side"] == "B")]
                                position_orderbook['count'] = position_orderbook.groupby(["instrument_key"]).cumcount()
                                sl_order = position_orderbook[(position_orderbook["instrument_key"] == str(position_dat["instrument_key"]))]
                                position_data = get_data(str(position_dat["instrument_key"]))
                                position_data = position_data.tail(50)
                                buy_ltp = market_data_df.loc[market_data_df['name'] == str(position_dat.instrument_key), 'ltp'].values[0]

                                position_low = position_data.iloc[-1]['Low']
                                position_high = position_data.iloc[-1]['High']
                                position_target = max(position_low - (position_high - position_low)*5,0.1)

                                if buy_ltp <= position_low <= position_high:
                                    sl_value = position_target
                                elif position_low <= buy_ltp <= position_high:
                                    sl_value = position_high
                                elif position_low <= position_high <= buy_ltp:
                                    sl_value = buy_ltp
                                else:
                                    sl_value = position_high



                                sl_trigger_value = getroundnumber(sl_value + (position_dat["tick_size"]),(position_dat["tick_size"]))
                                sl_value = getroundnumber(sl_trigger_value+(position_dat["tick_size"]),(position_dat["tick_size"]))

                                if (len(sl_order)) == 0:
                                    try:
                                        if sl_value == position_high:
                                            buy_order = {"quantity"    : int(position_dat["quantity"]), "product": str(position_dat["product"]),
                                                          "validity": "DAY","price": getroundnumber(sl_value,(position_dat["tick_size"])),
                                                         "tag"          : "string", "instrument_token": str(position_dat["instrument_key"]),
                                                         "order_type"   : "SL", "transaction_type": "BUY", "disclosed_quantity": 0,
                                                         "trigger_price": getroundnumber(sl_trigger_value,(position_dat["tick_size"])), "is_amo": False}

                                            # print(sale_order)
                                            response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=buy_order)
                                            print(str(response.json()))

                                        elif sl_value == buy_ltp:
                                            buy_order = {"quantity"     : int(position_dat["quantity"]), "product": str(position_dat["product"]),
                                                          "validity": "DAY","price": getroundnumber(buy_ltp,(position_dat["tick_size"])),
                                                          "tag"          : "string", "instrument_token": str(position_dat["instrument_key"]),
                                                          "order_type"   : "LIMIT", "transaction_type": "BUY", "disclosed_quantity": 0,
                                                          "trigger_price": 0, "is_amo": False}

                                            # print(sale_order)
                                            response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=buy_order)
                                            print(str(response.json()))
                                            sleep(0.02)


                                        elif sl_value == position_target:
                                            buy_order = {"quantity": int(position_dat["quantity"]), "product": str(position_dat["product"]),
                                                          "validity": "DAY","price": getroundnumber(position_target,(position_dat["tick_size"])),
                                                          "tag": "string", "instrument_token": str(position_dat["instrument_key"]),
                                                          "order_type": "LIMIT", "transaction_type": "BUY", "disclosed_quantity": 0,
                                                          "trigger_price": 0, "is_amo": False}

                                            # print(sale_order)
                                            response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=buy_order)
                                            print(str(response.json()))
                                            sleep(0.02)

                                        else:
                                            pass

                                    except Exception as error15:
                                        print("error15" + str(error15))
                                        pass


                                elif (len(sl_order)) > 0:
                                    try:
                                        for positionbook_orders in range(len(sl_order)):
                                            positionbook_dat = sl_order.iloc[positionbook_orders]

                                            if positionbook_dat['count'] >0:
                                                positionbook_dat['isAMO'] = positionbook_dat['isAMO'].tolist()
                                                orderbook_json_data = {'data': {'product': positionbook_dat["product"],
                                                                                'orderNumber': {
                                                                                    'oms': positionbook_dat["orderNumber_oms"],
                                                                                    'exchange': '', },
                                                                                'isAmo': positionbook_dat["isAMO"], }, }
                                                orderbook_cancel_order = (
                                                    upstox_session.patch('https://service.upstox.com/interactive/v3/order',
                                                                         headers=upstox_web_headers, json=orderbook_json_data,
                                                                         verify=False)).json()
                                                print(orderbook_cancel_order)

                                            else:
                                                old_price_limit = getroundnumber(positionbook_dat['price_limit'],(position_dat["tick_size"]))

                                                if old_price_limit != sl_value or position_dat["quantity"] != positionbook_dat["quantity"] :

                                                    positionbook_dat['isAMO'] = positionbook_dat['isAMO'].tolist()
                                                    orderbook_json_data = {'data': {'product': positionbook_dat["product"], 'orderNumber': {'oms': positionbook_dat["orderNumber_oms"], 'exchange': '', }, 'isAmo': positionbook_dat["isAMO"], }, }
                                                    orderbook_cancel_order = (upstox_session.patch('https://service.upstox.com/interactive/v3/order', headers=upstox_web_headers, json=orderbook_json_data, verify=False)).json()
                                                    print(orderbook_cancel_order)

                                                    if sl_value == position_high:
                                                        buy_order = {"quantity": int(position_dat["quantity"]),
                                                                      "product": str(position_dat["product"]),
                                                                      "validity": "DAY",
                                                                      "price": getroundnumber(sl_value,(position_dat["tick_size"])),
                                                                      "tag": "string",
                                                                      "instrument_token": str(position_dat["instrument_key"]),
                                                                      "order_type": "SL", "transaction_type": "BUY",
                                                                      "disclosed_quantity": 0,
                                                                      "trigger_price": getroundnumber(sl_trigger_value,(position_dat["tick_size"])),
                                                                      "is_amo": False}

                                                        # print(sale_order)
                                                        response = requests.post('https://api.upstox.com/v2/order/place',
                                                                                 headers=upstox_headers, json=buy_order)
                                                        print(str(response.json()))

                                                    elif sl_value == buy_ltp:
                                                        buy_order = {"quantity"     : int(position_dat["quantity"]), "product": str(position_dat["product"]),
                                                                      "validity": "DAY","price"        : getroundnumber(buy_ltp,(position_dat["tick_size"])),
                                                                      "tag"          : "string", "instrument_token": str(position_dat["instrument_key"]),
                                                                      "order_type"   : "LIMIT", "transaction_type": "BUY", "disclosed_quantity": 0,
                                                                      "trigger_price": 0, "is_amo": False}

                                                        # print(sale_order)
                                                        response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=buy_order)
                                                        print(str(response.json()))
                                                        sleep(0.02)


                                                    elif sl_value == position_target:
                                                        buy_order = {"quantity": int(position_dat["quantity"]),
                                                                      "product": str(position_dat["product"]), "validity": "DAY",
                                                                      "price": getroundnumber(position_target,(position_dat["tick_size"])),
                                                                      "tag": "string",
                                                                      "instrument_token": str(position_dat["instrument_key"]),
                                                                      "order_type": "LIMIT", "transaction_type": "BUY",
                                                                      "disclosed_quantity": 0,
                                                                      "trigger_price": 0, "is_amo": False}

                                                        # print(sale_order)
                                                        response = requests.post('https://api.upstox.com/v2/order/place',
                                                                                 headers=upstox_headers, json=buy_order)
                                                        print(str(response.json()))
                                                        sleep(0.02)

                                                    else:
                                                        pass

                                                else:
                                                    pass

                                    except Exception as error15:
                                        print("error15 " + str(error15))
                                        pass

                                else:
                                    pass

                                try:
                                    nse_timming = (time(9, 20) <= datetime.now().time() <= time(15, 10))
                                    exit_condition4 = not nse_timming
                                    exit_conditiion = exit_condition4

                                    if exit_conditiion:
                                        for orders in range(len(sl_order)):
                                            sl_order_dat = sl_order.iloc[orders]
                                            sl_order_dat['isAMO'] = sl_order_dat['isAMO'].tolist()
                                            orderbook_json_data = {'data': {'product': sl_order_dat["product"], 'orderNumber': {'oms': sl_order_dat["orderNumber_oms"], 'exchange': '', }, 'isAmo': sl_order_dat["isAMO"], }, }
                                            orderbook_cancel_order = (upstox_session.patch('https://service.upstox.com/interactive/v3/order', headers=upstox_web_headers, json=orderbook_json_data, verify=False)).json()
                                            print(orderbook_cancel_order)

                                        buy_order = {"quantity"     : int(position_dat["quantity"]), "product": str(position_dat["product"]), "validity": "DAY",
                                                      "price"        : getroundnumber(buy_ltp,(position_dat["tick_size"])),
                                                      "tag"          : "string", "instrument_token": str(position_dat["instrument_key"]),
                                                      "order_type"   : "LIMIT", "transaction_type": "BUY", "disclosed_quantity": 0,
                                                      "trigger_price": 0, "is_amo": False}

                                        # print(sale_order)
                                        response = requests.post('https://api.upstox.com/v2/order/place', headers=upstox_headers, json=buy_order)
                                        print(str(response.json()))
                                        sleep(0.02)


                                except Exception as error8:
                                    print("error8" + str(error8))
                                    pass
                            else:
                                pass
                    else :
                        pass

                elif str(main_position["success"]) == "False":
                    print(main_position)
                    pass
            else:
                sleep(1)
    except Exception as error14:
        print("error14" + str(error14))
        pass





t1 = Thread(target=pending_orders)
t1.start()


t3 = Thread(target=pending_position)
t3.start()



t2 = Thread(target=create_order)
t2.start()



