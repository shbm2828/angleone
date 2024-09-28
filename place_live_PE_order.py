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

#global pe_toke


TOTP("").now()
key_path = r"D:\key"
os.chdir(key_path)
key_secret = open("shbm_key.txt", "r").read().split()
#pe_token = key_secret[5]
#print(type(token))
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
    open_order = orderdf[orderdf["symboltoken"] == key_secret[6]]
    df = open_order[open_order["orderstatus"] =="trigger pending"]
    if df.empty:
        return 0
    else:
        placed_SL = df["stoploss"].values[0] 
        order_id = df["orderid"].values[0]
        return [placed_SL, order_id]

def get_pending_order():
    response = obj.orderBook()
    orderdf = pd.DataFrame(response['data'])
    orderdf2 = orderdf[orderdf["symboltoken"] == key_secret[5]]
    open_order = orderdf2[orderdf["orderstatus"] == "open"]
    if open_order.empty:
        print("there is no open order related to CE ")
    else:
        sl_price = open_order["price"].iloc[0]
        return sl_price
    



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


def pe_place_order():
    pe_df_1_temp = pd.read_csv('D:\\key\\oct\\candle_data\\BN_PE_1min_candle_'+today+'.csv')
    pe_df_1_temp.columns=['date', 'pe_1_open', 'pe_1_high', 'pe_1_low', 'pe_1_close']
    pe_df_1 = pe_df_1_temp.iloc[-2]
    pe_df_30 = pd.read_csv('D:\\key\\oct\\candle_data\\BN_PE_30min_candle_'+today+'.csv', names=['date', 'pe_30_open', 'pe_30_high', 'pe_30_low', 'pe_30_close'])
    high = pe_df_30.iloc[-2]['pe_30_high']
    low = pe_df_30.iloc[-2]['pe_30_low']
                      
    if float(pe_df_1['pe_1_close']) > high and (float(pe_df_1['pe_1_open'])-float(pe_df_1['pe_1_close'])) >= (float(pe_df_1['pe_1_high'])-float(pe_df_1['pe_1_low']))*0.8:
        if get_open_orders() == 0:
            print("BUY PE", pe_df_1['date'])
            print(high, low)
            #place_option_sl_market_order(key_secret[6],"BUY",15, low, instrument_list)
            place_market_order(key_secret[6], "BUY", 15, instrument_list)
            place_sl_limit_order(key_secret[6],"SELL", 15, low, instrument_list)           
            time.sleep(60)
        else:
            print("condition matched but there is already a open order")
            time.sleep(60)
    else:
        print("condition not matched for PE", pe_df_1['date'])
        print("30min high is:", high, "30min low is:", low)
        #get_pending_order() = low:
        
            
        
        time.sleep(60)
 


#schedule.every(1).second.do(pe_df_1)

while True:
    pe_place_order()



