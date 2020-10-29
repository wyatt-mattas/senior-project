#import pandas as pd
import csv
import talib._ta_lib as ta
from datetime import timedelta
import datetime
#from os import path as path
import alpaca_trade_api as tradeapi
import requests
from alpaca_trade_api import StreamConn
#import threading
import time
#import concurrent.futures
#import logging
#import sys
import asyncio
from calendar import monthrange
# import nest_asyncio
# nest_asyncio.apply()

#logger = logging.getLogger(__name__)
base_url = 'https://paper-api.alpaca.markets' # 'https://paper-api.alpaca.markets' - used for paper account
api_key_id = 'PKLIQDAA55PPU4YZEQEZ' #paper trading(PKK2HPWQFY9I25KIDVP9)
api_secret = 'SnYX9janNUqheD4PrzrUDTH1Jghj3UUcVwy248BA' #paper trading(IKSKvUlQp5iv9fGMkVlJ27pFiKqE0symg2KseZpQ)
#check_condition = False

class Main:
    def __init__(self, api):

        #self.universe = ['AMD','AAPL'] # TODO read universe from csv file and update once a month
        self._api = api
        self.min_share_price = 1.00
        self.max_share_price = 13.00
        # Minimum previous-day dollar volume for a stock we might consider
        self.min_last_dv = 500000
        self.risk = 0.001

    def get_tickers(self): # TODO need to make this the universe -- make this save to csv -- or could use api.add_watchlist()
        print('Getting current ticker data...')
        tickers = api.polygon.all_tickers()
        print('Success.')
        assets = api.list_assets()
        symbols = [asset.symbol for asset in assets if asset.tradable]
        ticker_list = [ticker for ticker in tickers if (
            ticker.ticker in symbols and
            ticker.lastTrade['p'] >= self.min_share_price and
            ticker.lastTrade['p'] <= self.max_share_price and
            ticker.prevDay['v'] * ticker.lastTrade['p'] > self.min_last_dv and
            ticker.todaysChangePerc >= 3.5
        )]
        ticker_list = [ticker.ticker for ticker in ticker_list]
        ticker_list = sorted(ticker_list, key=str.lower)
        return ticker_list

    def prev_weekday(self, adate):
        # get previous open day
        adate -= timedelta(days=1)
        while adate.weekday() > 4: # Mon-Fri are 0-4
            adate -= timedelta(days=1)
        return adate

    # grab open date closest to the end of the month
    def get_new_date(self):
        year = datetime.datetime.now().year
        month = datetime.datetime.now().month
        month_range = monthrange(year=year,month=month)
        month_last_day = month_range[1]
        date = f'{year}-{month}-{month_last_day}'
        calendar = self._api.get_calendar(start=date, end=date)[0]
        closest_to_end_of_month = calendar.date.strftime('%Y-%m-%d')
        return closest_to_end_of_month

    async def grab_data(self):
        start_new = self.get_new_date()
        today = datetime.datetime.today()
        today = today.strftime('%Y-%m-%d')

        if(today == start_new):
            ticker_list = self.get_tickers()
            with open('ticker_list.csv', 'w') as myfile:
                wr = csv.writer(myfile)
                wr.writerow(ticker_list)
            positions = self._api.list_positions()
            for position in positions:
                if(position.side == 'long'):
                    if(position.symbol not in ticker_list):
                        print(position.side) # TODO make this to actually sell off positions
                        print(position.symbol)
                        qty = abs(int(float(position.qty)))
                        self._api.submit_order(position.symbol, qty, 'sell', 'market', 'day')

        with open('ticker_list.csv', newline='') as f:
            reader = csv.reader(f)
            ticker_list = list(reader)
            ticker_list = ticker_list[0]
            print(ticker_list)

        # get data from pervious market day for tickers in list
        prev_date = self.prev_weekday(datetime.datetime.today())
        prev_date = prev_date.strftime('%Y-%m-%d')
        calendar = self._api.get_calendar(start=prev_date, end=prev_date)[0]
        end_time = calendar.close
        end_date = f'{prev_date}T{end_time}-04:00'
        df_list = {}
        for ticker in ticker_list:
            df_list[f'{ticker}'] = self._api.get_barset(symbols=ticker, timeframe='1Min', start=prev_date,end=end_date, limit=50).df
            #print(df_list[f'{ticker}'])
            df_list[f'{ticker}'].columns = df_list[f'{ticker}'].columns.droplevel()
            #print(df_list[f'{ticker}'])
            df_list[f'{ticker}'] = self.calc_ema(df_list[f'{ticker}'])
            #print(df_list[f'{ticker}'])
        return df_list, ticker_list

    # calculate the moving average for data
    def calc_ema(self,dataframe):
        dataframe['ema5'] = ta.EMA(dataframe['close'],timeperiod=5)
        dataframe['ema15'] = ta.EMA(dataframe['close'],timeperiod=15)
        dataframe['ema40'] = ta.EMA(dataframe['close'],timeperiod=40)
        return dataframe

    def buy_sell_calc(self, dataframe, ticker):
        ticker = ticker.symbol
        if (dataframe['ema5'][dataframe.index[-1]] > dataframe['ema15'][dataframe.index[-1]] and dataframe['ema5'][dataframe.index[-1]] > dataframe['ema40'][dataframe.index[-1]] and dataframe['ema15'][dataframe.index[-1]] > dataframe['ema40'][dataframe.index[-1]]):
            if(dataframe['ema5'][dataframe.index[-2]] > dataframe['ema15'][dataframe.index[-2]] and dataframe['ema5'][dataframe.index[-2]] > dataframe['ema40'][dataframe.index[-2]] and dataframe['ema15'][dataframe.index[-2]] > dataframe['ema40'][dataframe.index[-2]]): # making sure the it is tending to move upwards
                if (((dataframe['ema5'][dataframe.index[-1]]-dataframe['ema15'][dataframe.index[-1]])/dataframe['ema15'][dataframe.index[-1]]*100) > .5 and ((dataframe['ema5'][dataframe.index[-1]]-dataframe['ema40'][dataframe.index[-1]])/dataframe['ema40'][dataframe.index[-1]]*100) > 2):
                    qty, orderside = self.get_positions_sell(ticker)
                    if orderside == 'long':
                        self.submitOrder(qty,ticker,'sell') #TODO change this to certain amount based on what is in account
                        print('sell')

        if (dataframe['ema5'][dataframe.index[-1]] < dataframe['ema15'][dataframe.index[-1]] and dataframe['ema5'][dataframe.index[-1]] < dataframe['ema40'][dataframe.index[-1]] and dataframe['ema15'][dataframe.index[-1]] < dataframe['ema40'][dataframe.index[-1]]):
            if (dataframe['ema5'][dataframe.index[-2]] < dataframe['ema15'][dataframe.index[-2]] and dataframe['ema5'][dataframe.index[-2]] < dataframe['ema40'][dataframe.index[-2]] and dataframe['ema15'][dataframe.index[-2]] < dataframe['ema40'][dataframe.index[-2]]):
                if (dataframe['ema5'][dataframe.index[-3]] < dataframe['ema15'][dataframe.index[-3]] and dataframe['ema5'][dataframe.index[-3]] < dataframe['ema40'][dataframe.index[-3]] and dataframe['ema15'][dataframe.index[-3]] < dataframe['ema40'][dataframe.index[-3]]):
                    can_buy = self.get_positions_buy(ticker)
                    if can_buy == True:
                        quantity = self.calc_num_of_stocks(ticker)
                        self.submitOrder(quantity,ticker,'buy')
                        print('buy')

    def submitOrder(self, qty, stock, side):
        if(qty > 0):
            try:
                self._api.submit_order(stock, qty, side, 'market', 'day')
                print('Market order of | ' + str(qty) + ' ' + stock + ' ' + side + ' | completed.')
            except:
                print('Order of | ' + str(qty) + ' ' + stock + ' ' + side + ' | did not go through.')
        else:
            print('Quantity is 0, order of | ' + str(qty) + ' ' + stock + ' ' + side + ' | not completed.')

    def calc_num_of_stocks(self, stock):
        portfolio_value = float(api.get_account().portfolio_value)
        rough_number = portfolio_value / stock.low
        quantity = round(rough_number)
        return quantity

    def get_positions_sell(self, ticker):
        positions = self._api.list_positions()
        if (positions != []):
            for position in positions:
                if (position.symbol == ticker):
                    if(position.side == 'long'):
                        orderSide = 'long'
                        qty = abs(int(float(position.qty)))
                        return qty, orderSide

    def get_positions_buy(self, ticker):
        positions = self._api.list_positions()
        if (positions != []):
            for position in positions:
                if (position.symbol != ticker):
                    can_buy = True
        return can_buy


api = tradeapi.REST(base_url=base_url,key_id=api_key_id,secret_key=api_secret,api_version='v2')
conn = StreamConn(base_url=base_url,key_id=api_key_id,secret_key=api_secret)

ema = Main(api)
# new_ticker_list = ema.get_tickers()
# print(new_ticker_list)
# print(len(new_ticker_list))
# with open('ticker_list.csv', 'w') as myfile:
#      wr = csv.writer(myfile)
#      wr.writerow(new_ticker_list)

# df_ticker_list, ticker_list= ema.grab_data()
# print(ticker_list)


# portfolio_value = float(api.get_account().portfolio_value)
# print(portfolio_value)
# existing_orders = api.list_orders(limit=500)
# print(existing_orders)
# activities = api.get_activities()
# activities = activities[0]._raw
# print(activities['symbol'],activities['side'])
# print('ok')
#channels = ['AM.' + symbol for symbol in symbols]

async def awaitMarketOpen():
    isOpen = api.get_clock().is_open
    while(not isOpen):
        clock = api.get_clock()
        openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
        currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
        timeToOpen = int((openingTime - currTime) / 60)
        print(str(timeToOpen) + ' minutes til market open.')
        time.sleep(60)
        isOpen = api.get_clock().is_open
        if(timeToOpen < 5):
            global df_ticker_list
            global ticker_list
            df_ticker_list, ticker_list = await ema.grab_data()


async def run():
    # First, cancel any existing orders so they don't impact our buying power.

    # Wait for market to open.
    print('Waiting for market to open...')
    await awaitMarketOpen()
    print('Market opened.')
    # global df_ticker_list
    # global ticker_list
    # df_ticker_list, ticker_list = ema.grab_data()
    #print(type(df))


@conn.on(r'^account_updates$')
async def on_account_updates(conn, channel, account):
    print('account', account)

@conn.on(r'^AM.*$')
async def on_minute_bars(conn, channel, bars):
    if bars.symbol in ticker_list:
        #global check_condition
        global df_ticker_list
        new_bar = bars._raw
        #print(type(new_bar))
        #print(new_bar) #already a dictionary
        del new_bar['symbol']# shorten this up only append what is needed instead of deleting it
        del new_bar['totalvolume']
        del new_bar['vwap']
        del new_bar['average']
        del new_bar['start']
        del new_bar['end']
        del new_bar['timestamp']
        print(new_bar)
        df_ticker_list[bars.symbol] = df_ticker_list[bars.symbol].append(new_bar, ignore_index=True)
        df_ticker_list[bars.symbol] = ema.calc_ema(df_ticker_list[bars.symbol])
        ema.buy_sell_calc(df_ticker_list[bars.symbol], bars)
        print(df_ticker_list[bars.symbol])

async def subscribe():
    #channels = ['AM.' + symbol for symbol in ticker_list]
    await get_clock()
    while(await get_clock() == True) :
        #if(get_clock == True):
        await conn.subscribe(['AM.*'])
        await get_clock()
        print(await get_clock())
        await asyncio.sleep(10)
        if(await get_clock() == False):
            await conn.unsubscribe(['AM.*'])
            print('Finished')
            #subscribe_loop.stop()


async def unsubscribe():
    await conn.unsubscribe(['AM.AMD'])

async def get_clock():
    isOpen = api.get_clock().is_open
    return isOpen

if __name__ == '__main__':

    loop = asyncio.new_event_loop()
    #loop.run_until_complete(run())
    loop.create_task(run())
    loop.create_task(subscribe())
    # subscribe_loop = asyncio.new_event_loop()
    # subscribe_loop.create_task(subscribe())
    # subscribe_loop.run_forever()
    loop.run_forever()

