import pandas as pd
import talib._ta_lib as ta
from datetime import datetime, timedelta
from os import path as path
import alpaca_trade_api as tradeapi
import requests
from alpaca_trade_api import StreamConn
import threading
import time
import concurrent.futures
import logging

base_url = 'https://paper-api.alpaca.markets' # 'https://paper-api.alpaca.markets' - used for paper account
api_key_id = 'PKK2HPWQFY9I25KIDVP9' # AKJUVZ2YL4C4J9XRVD2P -- paper trading(PKK2HPWQFY9I25KIDVP9)
api_secret = 'IKSKvUlQp5iv9fGMkVlJ27pFiKqE0symg2KseZpQ' # Fms0oy3tdNp7K9E8oF13nFScWFuiECzXiShA2PTF -- paper trading(IKSKvUlQp5iv9fGMkVlJ27pFiKqE0symg2KseZpQ)

class Main:
    def __init__(self):

        self.api = tradeapi.REST(
            base_url=base_url,
            key_id=api_key_id,
            secret_key=api_secret,
            api_version='v2')

        self.logger = logging.getLogger(__name__)

        self.check_condition = False

        self.universe = 'AMD'

        self.conn = StreamConn(base_url=base_url,key_id=api_key_id,secret_key=api_secret) #had comma at the end dont know if it was needed

    def prev_weekday(self, adate):
        # get previous open day
        adate -= timedelta(days=1)
        while adate.weekday() > 4: # Mon-Fri are 0-4
            adate -= timedelta(days=1)
        return adate

    def grab_data(self):
        #get data from pervious market day
        prev_date = self.prev_weekday(datetime.today())
        prev_date = prev_date.strftime('%Y-%m-%d')
        calendar = self.api.get_calendar(start=prev_date, end=prev_date)[0]
        end_time = calendar.close
        #TODO add try statment to make sure there was a market open
        end_date = f'{prev_date}T{end_time}-04:00'
        df = self.api.get_barset(symbols=self.universe, timeframe='1Min', start=prev_date,end=end_date, limit=50).df
        df = self.calc_ema(df)
        return df


    def run(self):
        # First, cancel any existing orders so they don't impact our buying power.
        orders = self.api.list_orders(status="open")
        for order in orders:
            self.api.cancel_order(order.id)

        # Wait for market to open.
        print("Waiting for market to open...")
        tAMO = threading.Thread(target=self.awaitMarketOpen)
        tAMO.start()
        tAMO.join()
        print("Market opened.")

        # Rebalance the portfolio every minute, making necessary trades.
        while True:
            # Figure out when the market will close so we can prepare to sell beforehand.
            clock = self.api.get_clock()
            closingTime = clock.next_close.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            self.timeToClose = closingTime - currTime

            if(self.timeToClose < (60 * 10)):
                # Close all positions when 10 minutes til market close.
                print("Market closing soon.  Closing positions.") #TODO want to sell all at end of day
                positions = self.api.list_positions()
                for position in positions:
                    if(position.side == 'long'):
                        orderSide = 'sell'
                    else:
                        orderSide = 'buy'
                    qty = abs(int(float(position.qty)))
                    tSubmitOrder = threading.Thread(target=self.submitOrder(qty, position.symbol, orderSide))
                    tSubmitOrder.start()
                    tSubmitOrder.join()

        # Run script again after market close for next trading day.
                print("Sleeping until market close (15 minutes).")
                time.sleep(60 * 15)
            else:
                #start stream conn on another thread
                ws_thread = threading.Thread(target=self.ws_start)
                ws_thread.start()
                ws_thread.join()
                time.sleep(10)

    def rebalance(self): # Keep for now probably do not need
        # Clear existing orders
        orders = self.api.list_orders(status="open")
        for order in orders:
            self.api.cancel_order(order.id)

    # Wait for market to open.
    def awaitMarketOpen(self):
        isOpen = self.api.get_clock().is_open
        while(not isOpen):
            self.df = self.grab_data()
            clock = self.api.get_clock()
            openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            timeToOpen = int((openingTime - currTime) / 60)
            print(str(timeToOpen) + " minutes til market open.")
            time.sleep(60)
            isOpen = self.api.get_clock().is_open

    def calc_ema(self,dataframe):
        dataframe['ema5'] = ta.EMA(dataframe['AMD']['close'],timeperiod=5)
        dataframe['ema15'] = ta.EMA(dataframe['AMD']['close'],timeperiod=15)
        dataframe['ema40'] = ta.EMA(dataframe['AMD']['close'],timeperiod=40)
        return dataframe

    def buy_sell_calc(self, check_condition, dataframe):
        if (dataframe['ema5'][dataframe.index[-1]] > dataframe['ema15'][dataframe.index[-1]] and dataframe['ema5'][dataframe.index[-1]] > dataframe['ema40'][dataframe.index[-1]] and dataframe['ema15'][dataframe.index[-1]] > dataframe['ema40'][dataframe.index[-1]]):
            if(dataframe['ema5'][dataframe.index[-2]] > dataframe['ema15'][dataframe.index[-2]] and dataframe['ema5'][dataframe.index[-2]] > dataframe['ema40'][dataframe.index[-2]] and dataframe['ema15'][dataframe.index[-2]] > dataframe['ema40'][dataframe.index[-2]]): # making sure the it is tending to move upwards
                if (((dataframe['ema5'][dataframe.index[-1]]-dataframe['ema15'][dataframe.index[-1]])/dataframe['ema15'][dataframe.index[-1]]*100) > .5 and ((dataframe['ema5'][dataframe.index[-1]]-dataframe['ema40'][dataframe.index[-1]])/dataframe['ema40'][dataframe.index[-1]]*100) > 2):
                    if check_condition == True:
                        self.submitOrder(10,self.universe,'sell')
                        self.check_condition = False
                        print('sell')

        if (dataframe['ema5'][dataframe.index[-1]] < dataframe['ema15'][dataframe.index[-1]] and dataframe['ema5'][dataframe.index[-1]] < dataframe['ema40'][dataframe.index[-1]] and dataframe['ema15'][dataframe.index[-1]] < dataframe['ema40'][dataframe.index[-1]]):
            if (dataframe['ema5'][dataframe.index[-2]] < dataframe['ema15'][dataframe.index[-2]] and dataframe['ema5'][dataframe.index[-2]] < dataframe['ema40'][dataframe.index[-2]] and dataframe['ema15'][dataframe.index[-2]] < dataframe['ema40'][dataframe.index[-2]]):
                if (dataframe['ema5'][dataframe.index[-3]] < dataframe['ema15'][dataframe.index[-3]] and dataframe['ema5'][dataframe.index[-3]] < dataframe['ema40'][dataframe.index[-3]] and dataframe['ema15'][dataframe.index[-3]] < dataframe['ema40'][dataframe.index[-3]]):
                    if check_condition == False:
                        self.submitOrder(10,self.universe,'buy')
                        self.check_condition = True
                        print('buy')
        return check_condition

    def submitOrder(self, qty, stock, side):
        if(qty > 0):
            try:
                self.api.submit_order(stock, qty, side, "market", "day")
                print("Market order of | " + str(qty) + " " + stock + " " + side + " | completed.")
            except:
                print("Order of | " + str(qty) + " " + stock + " " + side + " | did not go through.")
        else:
            print("Quantity is 0, order of | " + str(qty) + " " + stock + " " + side + " | not completed.")

    def trade(self):
        ticker_list = [self.universe]

        @self.conn.on(r'^account_updates$')
        async def on_account_updates(conn, channel, account):
            print('account', account)

        @self.conn.on(r'^status$')
        async def on_status(conn, channel, data):
            print('polygon status update', data)

        @self.conn.on(r'^AM$')
        async def on_minute_bars(conn, channel, bars):
            if bars.symbol in ticker_list:
                new_bar = bars._raw
                #print(type(new_bar))
                print('bars', new_bar) #already a dictionary
                del new_bar['symbol']# shorten this up
                del new_bar['totalvolume']
                del new_bar['dailyopen']
                del new_bar['vwap']
                del new_bar['average']
                del new_bar['start']
                del new_bar['end']
                self.df = self.df.append(new_bar, ignore_index=True)
                self.check_condition = self.buy_sell_calc(self.check_condition, self.df)
                print('check condition: ', self.check_condition)
                print(self.df)

        #conn.run(['account_updates', 'AM.*']) # figure out if this is needed

    def ws_start(self):
	    self.conn.run(['account_updates', 'AM.*']) #TODO might need to change to run till complete instead of just run

if __name__ == '__main__':
    trader = Main()
    trader.run()