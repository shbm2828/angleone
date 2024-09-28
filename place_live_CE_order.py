import pandas as pd
import schedule
import time
from threading import Thread
from SmartApi import SmartConnect
from pyotp import TOTP
import os
import urllib
import json
from datetime import date

today = str(date.today())

#counter_1min = 105

TOTP("").now()
key_path = r"D:\key"
os.chdir(key_path)
key_secret = open("shbm_key.txt", "r").read().split()
obj = SmartConnect(api_key=key_secret[0])
data = obj.generateSession(key_secret[2],key_secret[3], TOTP(key_secret[4]).now())

instrument_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
response = urllib.request.urlopen(instrument_url)
instrument_list = json.loads(response.read())

def token_lookup(ticker, instrument_list, exchange="NSE"):
    for instrument in instrument_list:
        if instrument["name"] == ticker and instrument["exch_seg"] == exchange and instrument["symbol"].split("-")[-1] == "EQ":
            return instrument["token"]
        

def symbol_lookup(token, instrument_list, exchange="NFO"):
    for instrument in instrument_list:
        if instrument["token"] == token and instrument["exch_seg"] == exchange:
            return(instrument["symbol"])
                
        
def get_open_orders():
    response = obj.orderBook()
    orderdf = pd.DataFrame(response['data'])
    open_order = orderdf[orderdf["symboltoken"] ==key_secret[5]]
    df = open_order[open_order["orderstatus"] =="trigger pending"]
    if df.empty:
        return 0
    else:
        placed_SL = df["stoploss"].values[0] 
        order_id = df["orderid"].values[0]
        return [placed_SL, order_id]

#get_open_orders() 

def place_market_order(token, buy_sell, quantity, instrument_list):
    params = {
        "variety": "NORMAL",
        "tradingsymbol": symbol_lookup(token, instrument_list),
        "symboltoken": token,
        "transactiontype": buy_sell,
        "exchange": "NFO",
        "ordertype": "MARKET",
        "producttype": "INTRADAY",
        "duration": "DAY",
        "quantity": quantity,       
        }
    order = obj.placeOrder(params)
    print(order)
         

    
def place_sl_limit_order(token,buy_sell, quantity, price, instrument_list):
    
    params = {
        "variety": "STOPLOSS",
        "tradingsymbol": symbol_lookup(token, instrument_list),
        "symboltoken": token,
        "transactiontype": buy_sell,
        "exchange": "NFO",
        "ordertype": "STOPLOSS_LIMIT",
        "producttype": "INTRADAY",
        "duration": "DAY",
        "quantity": quantity,
        "triggerprice": price,
        "price": price
        }
    order = obj.placeOrder(params)
    print(order)

def modify_order(token,low,order_id,instrument_list):
    params = {
                "variety":"STOPLOSS",
                "tradingsymbol":symbol_lookup(token, instrument_list),
                "symboltoken":token,
                "orderid":'order_id',
                "exchange":"NFO",
                "ordertype":"STOPLOSS_MARKET",
                "producttype":"INTRADAY",
                "duration":"DAY",
                "triggerprice": low,
                "price":low 
                }
    response = obj.modifyOrder(params)
    return response

def ce_place_order():
    ce_df_1_temp = pd.read_csv('D:\\key\\april\\candle_data\\BN_CE_1min_candle_'+today+'.csv')
    ce_df_1_temp.columns=['date', 'ce_1_open', 'ce_1_high', 'ce_1_low', 'ce_1_close']
    ce_df_1 = ce_df_1_temp.iloc[-2]
    ce_df_30 = pd.read_csv('D:\\key\\april\\candle_data\\BN_CE_30min_candle_'+today+'.csv', names=['date', 'ce_30_open', 'ce_30_high', 'ce_30_low', 'ce_30_close'])
    high = ce_df_30.iloc[-2]['ce_30_high']
    low = ce_df_30.iloc[-2]['ce_30_low']
    if float(ce_df_1['ce_1_close']) > high and (float(ce_df_1['ce_1_open'])-float(ce_df_1['ce_1_close'])) >= (float(ce_df_1['ce_1_high'])-float(ce_df_1['ce_1_low']))*0.8:
        if get_open_orders() == 0:
            print("BUY CE", ce_df_1['date'])
            print(high, low)
            #place_option_sl_market_order(key_secret[6],"BUY",15, low, instrument_list)
            place_market_order(key_secret[5], "BUY", 15, instrument_list)
            place_sl_limit_order(key_secret[5],"SELL", 15, low, instrument_list)            
            time.sleep(60)
        else:
            print("condition matched but there is already a open order")
            time.sleep(60)
    else:
        print("condition not matched for CE", ce_df_1['date'])
        print("30min high is:", high, "30min low is:", low)
        time.sleep(60)


'''schedule.every(1).minute.do(ce_place_order)
while True:
    schedule.run_pending()
    time.sleep(1)'''

while True:
    ce_place_order()
