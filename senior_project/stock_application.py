import pandas as pd
import talib._ta_lib as ta
from datetime import datetime
from os import path as path
import alpaca_trade_api as tradeapi
import requests
from alpaca_trade_api import StreamConn
import threading
import time

base_url = 'https://paper-api.alpaca.markets' # 'https://paper-api.alpaca.markets' - used for paper account
api_key_id = 'PKK2HPWQFY9I25KIDVP9' # AKJUVZ2YL4C4J9XRVD2P -- paper trading(PKK2HPWQFY9I25KIDVP9)
api_secret = 'IKSKvUlQp5iv9fGMkVlJ27pFiKqE0symg2KseZpQ' # Fms0oy3tdNp7K9E8oF13nFScWFuiECzXiShA2PTF -- paper trading(IKSKvUlQp5iv9fGMkVlJ27pFiKqE0symg2KseZpQ)

# api = tradeapi.REST(
#     base_url=base_url,
#     key_id=api_key_id,
#     secret_key=api_secret,
#     api_version='v2')
# account = api.get_account()

#if account.status == 'ACTIVE':
class Main:
    def __init__(self):
        #session = requests.session()

        self.api = tradeapi.REST(
            base_url=base_url,
            key_id=api_key_id,
            secret_key=api_secret,
            api_version='v2')
        #account = api.get_account()

        clock = self.api.get_clock()
        print('The market is {}'.format('open.' if clock.is_open else 'closed.'))

        self.check_condition = False

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


    # buy_signals = []
    # sell_signals = []

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

            if(self.timeToClose < (60 * 15)):
                # Close all positions when 15 minutes til market close.
                print("Market closing soon.  Closing positions.")
                positions = self.api.list_positions()
                for position in positions:
                    if(position.side == 'long'):
                        orderSide = 'sell'
                    else:
                        orderSide = 'buy'
                    qty = abs(int(float(position.qty)))
                    respSO = []
                    tSubmitOrder = threading.Thread(target=self.submitOrder(qty, position.symbol, orderSide, respSO))
                    tSubmitOrder.start()
                    tSubmitOrder.join()

        # Run script again after market close for next trading day.
                print("Sleeping until market close (15 minutes).")
                time.sleep(60 * 15)
            else:
        # Rebalance the portfolio.
                tRebalance = threading.Thread(target=self.rebalance) # might not need this for now
                tRebalance.start()
                tRebalance.join()
                time.sleep(60)

    def rebalance(self):
        # Clear existing orders
        orders = self.api.list_orders(status="open")
        for order in orders:
            self.api.cancel_order(order.id)

    # Wait for market to open.
    def awaitMarketOpen(self):
        isOpen = self.api.get_clock().is_open
        while(not isOpen):
            clock = self.api.get_clock()
            openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            timeToOpen = int((openingTime - currTime) / 60)
            print(str(timeToOpen) + " minutes til market open.")
            time.sleep(60)
            isOpen = self.api.get_clock().is_open

    def calc_ema(self, dataframe):
        dataframe['ema5'] = ta.EMA(df['close'],timeperiod=5)
        dataframe['ema15'] = ta.EMA(df['close'],timeperiod=15)
        dataframe['ema40'] = ta.EMA(df['close'],timeperiod=40)
        return dataframe

    def buy_sell_calc(self, check_condition, dataframe):
        #for i in range(1, len(dataframe['close'])):
        if (dataframe['ema5'][df.index[-1]] > dataframe['ema15'][df.index[-1]] and dataframe['ema5'][df.index[-1]] > dataframe['ema40'][df.index[-1]] and dataframe['ema15'][df.index[-1]] > dataframe['ema40'][df.index[-1]]):
            if(dataframe['ema5'][df.index[-2]] > dataframe['ema15'][df.index[-2]] and dataframe['ema5'][df.index[-2]] > dataframe['ema40'][df.index[-2]] and dataframe['ema15'][df.index[-2]] > dataframe['ema40'][df.index[-2]]): # making sure the it is tending to move upwards
                if (((dataframe['ema5'][df.index[-1]]-dataframe['ema15'][df.index[-1]])/dataframe['ema15'][df.index[-1]]*100) > .5 and ((dataframe['ema5'][df.index[-1]]-dataframe['ema40'][df.index[-1]])/dataframe['ema40'][df.index[-1]]*100) > 2):
                    if check_condition == True:
    					#sell_signals.append([df.index[i], df['high'][i]])
                        check_condition = False
                        print('sell')

        if (dataframe['ema5'][df.index[-1]] < dataframe['ema15'][df.index[-1]] and dataframe['ema5'][df.index[-1]] < dataframe['ema40'][df.index[-1]] and dataframe['ema15'][df.index[-1]] < dataframe['ema40'][df.index[-1]]):
            if (dataframe['ema5'][df.index[-2]] < dataframe['ema15'][df.index[-2]] and dataframe['ema5'][df.index[-2]] < dataframe['ema40'][df.index[-2]] and dataframe['ema15'][df.index[-2]] < dataframe['ema40'][df.index[-2]]):
                if (dataframe['ema5'][df.index[-3]] < dataframe['ema15'][df.index[-3]] and dataframe['ema5'][df.index[-3]] < dataframe['ema40'][df.index[-3]] and dataframe['ema15'][df.index[-3]] < dataframe['ema40'][df.index[-3]]):
                    if check_condition == False:
    					#buy_signals.append([df.index[i], df['low'][i]])
                        check_condition = True
                        print('buy')
        return check_condition

    def submitOrder(self, qty, stock, side, resp):
        if(qty > 0):
            try:
                self.api.submit_order(stock, qty, side, "market", "day")
                print("Market order of | " + str(qty) + " " + stock + " " + side + " | completed.")
                resp.append(True)
            except:
                print("Order of | " + str(qty) + " " + stock + " " + side + " | did not go through.")
                resp.append(False)
        else:
            print("Quantity is 0, order of | " + str(qty) + " " + stock + " " + side + " | not completed.")
            resp.append(True)

    def trade(self):
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
            global check_condition
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
                df = df.append(new_bar, ignore_index=True)
                self.calc_ema(df)
                check_condition = self.buy_sell_calc(check_condition, df)
                print('check condition: ', check_condition)
                print(df)

        @conn.on(r'^A$')
        async def on_second_bars(conn, channel, bar):
            print('bars', bar)

        conn.run(['account_updates', 'AM.*'])
