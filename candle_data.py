import pandas as pd
from threading import Thread
import time
import schedule
from datetime import date

today = str(date.today())

class CE(Thread):
    def one_min_candle_data():
        df = pd.read_csv('D:\\key\\sept\\live_data\\BN_CE_DATA_'+today+'.csv', header=None)
        df.columns=['date', 'ltp']
        df['date'] = pd.to_datetime(df.date)  
        one_min_data = df.set_index('date').resample('1min').ohlc().reset_index()
        one_min_data.to_csv(r'D:\\key\\sept\\candle_data\\BN_CE_1min_candle_'+today+'.csv', header=None, index=False)
        #time.sleep(60)
    

    def half_hr_candle_data():
        df = pd.read_csv('D:\\key\\sept\\live_data\\BN_CE_DATA_'+today+'.csv', header=None)
        df.columns=['date', 'ltp']
        df['date'] = pd.to_datetime(df.date)  
        half_hr_data = df.set_index('date').resample('30min', origin='start').ohlc().reset_index()
        half_hr_data.to_csv(r'D:\\key\\sept\\candle_data\\BN_CE_30min_candle_'+today+'.csv', header=None, index=False)
        #time.sleep(1800)

    schedule.every(1).minute.do(one_min_candle_data)
    schedule.every(30).minutes.do(half_hr_candle_data)
 
class PE(Thread):
    def one_min_candle_data():
        df = pd.read_csv('D:\\key\\sept\\live_data\\BN_PE_DATA_'+today+'.csv', header=None)
        df.columns=['date', 'ltp']
        df['date'] = pd.to_datetime(df.date)  
        one_min_data = df.set_index('date').resample('1min').ohlc().reset_index()
        one_min_data.to_csv(r'D:\\key\\sept\\candle_data\\BN_PE_1min_candle_'+today+'.csv', header=None, index=False)

    def half_hr_candle_data():
        df = pd.read_csv('D:\\key\\sept\\live_data\\BN_PE_DATA_'+today+'.csv', header=None)
        df.columns=['date', 'ltp']
        df['date'] = pd.to_datetime(df.date)  
        half_hr_data = df.set_index('date').resample('30min', origin='start').ohlc().reset_index()
        half_hr_data.to_csv(r'D:\\key\\sept\\candle_data\\BN_PE_30min_candle_'+today+'.csv', header=None, index=False)
  
 
    schedule.every(1).minute.do(one_min_candle_data)
    schedule.every(30).minutes.do(half_hr_candle_data)
    while True:
        schedule.run_pending()
        time.sleep(1)
    
t1 = CE()
t2 = PE()

t1.start()
t2.start()

t1.join()
t2.join()

