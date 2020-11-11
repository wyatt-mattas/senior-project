from datetime import datetime, date
import time
import alpaca_trade_api as tradeapi
import pandas as pd
import requests
from alpaca_trade_api import StreamConn
import asyncio
import ast

'''c=0
while True:
    today = datetime.now()
    print(today)
    c+=1
    if c >= 5:
        break
    time.sleep(5)'''

base_url = 'https://paper-api.alpaca.markets'
api_key_id = open('C:\\Account IDs\\AlpacaAPIIDNewOrig.txt', 'r').read()
api_secret = open('C:\\Account IDs\\AlpacaAPINewSecretOrig.txt', 'r').read()

api = tradeapi.REST(base_url=base_url,key_id=api_key_id,secret_key=api_secret,api_version='v2')
account = api.get_account()

symbols='NCZ'

min_share_price = 0.50
max_share_price = 13.0
# Minimum previous-day dollar volume for a stock we might consider
min_last_dv = 500000

def get_tickers():
    print('Getting current ticker data...')
    tickers = api.polygon.all_tickers()
    print('Success.')
    assets = api.list_assets()
    symbols = [asset.symbol for asset in assets if asset.tradable and asset.shortable]
    ticker_list = [ticker for ticker in tickers if (
        ticker.ticker in symbols and
        ticker.lastTrade['p'] >= min_share_price and
        ticker.lastTrade['p'] <= max_share_price and
        ticker.prevDay['v'] * ticker.lastTrade['p'] > min_last_dv and
        ticker.todaysChangePerc >= 3.5
    )]
    ticker_list = [ticker.ticker for ticker in ticker_list]
    ticker_list = sorted(ticker_list, key=str.lower)
    return ticker_list

tickers = get_tickers()
print(tickers)
print('ok')
# ticker_list = [ticker.ticker for ticker in tickers]
# ticker_list = sorted(ticker_list, key=str.lower)
# print(ticker_list)

ticker_list = ['AMD']

'''while True:
    for ticker in ticker_list:
        ss = api.polygon.snapshot(ticker)
        lastprice = ss.ticker['lastTrade']['p']
        print(f'{ticker} + {lastprice}')
    time.sleep(60)'''

conn = StreamConn(base_url=base_url,key_id=api_key_id,secret_key=api_secret,)

@conn.on(r'^account_updates$')
async def on_account_updates(conn, channel, account):
    print('account', account)

@conn.on(r'^status$')
async def on_status(conn, channel, data):
    print('polygon status update', data)

@conn.on(r'^AM$')
async def on_minute_bars(conn, channel, bars):
    if bars.symbol in ticker_list:
        new_bar = bars._raw
        #print(type(new_bar))
        #final_bar = ast.literal_eval(new_bar)
        #print(type(new_bar))
        print('bars', new_bar) #already a dictionary

@conn.on(r'^A$')
async def on_second_bars(conn, channel, bar):
    print('bars', bar)

#conn.run(['account_updates', 'AM.*'])
#conn.loop.stop()
conn.run(['account_updates', 'AM.*'])
