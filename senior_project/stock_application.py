import pandas as pd
import talib._ta_lib as ta
from datetime import datetime
from os import path as path
import alpaca_trade_api as tradeapi
import requests
import concurrent.futures
import logging
from alpaca_trade_api import StreamConn

base_url = 'https://paper-api.alpaca.markets' # 'https://paper-api.alpaca.markets' - used for paper account
api_key_id = 'PKK2HPWQFY9I25KIDVP9' # AKJUVZ2YL4C4J9XRVD2P -- paper trading(PKK2HPWQFY9I25KIDVP9)
api_secret = 'IKSKvUlQp5iv9fGMkVlJ27pFiKqE0symg2KseZpQ' # Fms0oy3tdNp7K9E8oF13nFScWFuiECzXiShA2PTF -- paper trading(IKSKvUlQp5iv9fGMkVlJ27pFiKqE0symg2KseZpQ)

api = tradeapi.REST(
    base_url=base_url,
    key_id=api_key_id,
    secret_key=api_secret,
    api_version='v2')
account = api.get_account()

if account.status == 'ACTIVE':
    session = requests.session()

    stock = 'AMD'
    if path.exists(f'E:\\senior_project\\ema_csv\\{stock}_EMAS.csv') == True:
    	df = pd.read_csv(f'E:\\senior_project\\ema_csv\\{stock}_EMAS.csv')
    else:
    	df = pd.read_csv(f'E:\\senior_project\\barset_data\\{stock}_get_barset_data.csv', skiprows=1, usecols=range(3,15)) # skip the first row to make the dataframe easier to mess with when using barset data, don't need skip for historic_agg
    	df['ema5'] = ta.EMA(df['close'], timeperiod=5)
    	df['ema15'] = ta.EMA(df['close'], timeperiod=15)
    	df['ema40'] = ta.EMA(df['close'], timeperiod=40)
    	df.to_csv(f'E:\\senior_project\\ema_csv\\{stock}_EMAS.csv')
    df = df.drop(df.columns[1], axis=1)
    df = df.drop(df.columns[0], axis=1)
    print(df)

    buy_signals = []
    sell_signals = []


    check_condition = False

    def calc_ema(dataframe):
        dataframe['ema5'] = ta.EMA(df['close'],timeperiod=5)
        dataframe['ema15'] = ta.EMA(df['close'],timeperiod=15)
        dataframe['ema40'] = ta.EMA(df['close'],timeperiod=40)
        return dataframe

    def buy_sell_calc(check_condition):
        for i in range(1, len(df['close'])):
    	    if (df['ema5'][i] > df['ema15'][i] and df['ema5'][i] > df['ema40'][i] and df['ema15'][i] > df['ema40'][i]):
    		    if(df['ema5'][i-1] > df['ema15'][i-1] and df['ema5'][i-1] > df['ema40'][i-1] and df['ema15'][i-1] > df['ema40'][i-1]): # making sure the it is tending to move upwards
    			    if (((df['ema5'][i]-df['ema15'][i])/df['ema15'][i]*100) > .5 and ((df['ema5'][i]-df['ema40'][i])/df['ema40'][i]*100) > 2):
    				    if check_condition == True:
    					    sell_signals.append([df.index[i], df['high'][i]])
    					    check_condition = False

    	    if (df['ema5'][i] < df['ema15'][i] and df['ema5'][i] < df['ema40'][i] and df['ema15'][i] < df['ema40'][i]):
    		    if (df['ema5'][i-1] < df['ema15'][i-1] and df['ema5'][i-1] < df['ema40'][i-1] and df['ema15'][i-1] < df['ema40'][i-1]):
    			    if (df['ema5'][i-2] < df['ema15'][i-2] and df['ema5'][i-2] < df['ema40'][i-2] and df['ema15'][i-2] < df['ema40'][i-2]):
    				    if check_condition == False:
    					    buy_signals.append([df.index[i], df['low'][i]])
    					    check_condition = True
        return check_condition

    ticker_list = ['AMD']
    conn = StreamConn(base_url=base_url,key_id=api_key_id,secret_key=api_secret) #had comma at the end dont know if it was needed

    @conn.on(r'^account_updates$')
    async def on_account_updates(conn, channel, account):
        print('account', account)

    @conn.on(r'^status$')
    async def on_status(conn, channel, data):
        print('polygon status update', data)

    @conn.on(r'^AM$')
    async def on_minute_bars(conn, channel, bars):
        global df
        if bars.symbol in ticker_list:
            new_bar = bars._raw
            #print(type(new_bar))
            print('bars', new_bar) #already a dictionary
            del new_bar['symbol']#,'totalvolume','dailyopen','vwap','average','start','end']
            del new_bar['totalvolume']
            del new_bar['dailyopen']
            del new_bar['vwap']
            del new_bar['average']
            del new_bar['start']
            del new_bar['end']
            df = df.append(new_bar, ignore_index=True)
            calc_ema(df)
            print(df)

    @conn.on(r'^A$')
    async def on_second_bars(conn, channel, bar):
        print('bars', bar)

    conn.run(['account_updates', 'AM.*'])