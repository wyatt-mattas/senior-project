import csv
import talib._ta_lib as ta
from datetime import timedelta, date
import datetime
import alpaca_trade_api as tradeapi
import requests
from alpaca_trade_api import StreamConn
import time
import asyncio
from calendar import monthrange
import concurrent.futures
from twilio.rest import Client
import sys
import traceback
# VERSION LIQUID
# ALPACA API KEYS
base_url = 'https://paper-api.alpaca.markets'
api_key_id = open('C:\\Account IDs\\AlpacaAPIIDOrig.txt', 'r').read()
api_secret = open('C:\\Account IDs\\AlpacaAPISecretOrig.txt', 'r').read()
# KEYS FOR TWILIO
account_sid = open('C:\\Account IDs\\SID.txt', 'r').read()
auth_token = open('C:\\Account IDs\\token.txt', 'r').read()

#TODO need to work on shorting the market
#TODO work on after market buy and sell
#TODO only sell amount of stocks to get RecT up to 200,000 -- sort stocks by performance
#TODO limit only 200 requests for treadpool or limit amout of stocks to look at
class Calculations:
    def __init__(self, api):
        # initalize some variables
        self._api = api
        self.min_share_price = 1.00
        self.max_share_price = 20.00
        self.min_last_dv = 500000
        self.risk = 0.015

    # get a list of tickers that meet a certain criteria
    async def get_tickers(self):
        print('Getting current ticker data...')
        tickers = self._api.polygon.all_tickers() # TODO make sure list is not empty
        print('Success.')
        assets = self._api.list_assets()
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

    async def prev_weekday(self, adate):
        # get previous open day
        adate -= timedelta(days=1)
        while adate.weekday() > 4: # Mon-Fri are 0-4
            adate -= timedelta(days=1)
        return adate

    # grab data for each stock that is in the list
    async def grab_data(self):
        #if date.today().weekday() == 0:
        ticker_list = await self.get_tickers()
        if ticker_list != []:
            with open('ticker_list_liquid.csv', 'w') as myfile:
                wr = csv.writer(myfile)
                wr.writerow(ticker_list) # overwrite csv file with list of new tickers

        # read the ticker list from text file
        with open('ticker_list_liquid.csv', newline='') as f:
            reader = csv.reader(f)
            ticker_list = list(reader)
            ticker_list = ticker_list[0]
            print(ticker_list)

        # get data from pervious market day for tickers in list
        prev_date = await self.prev_weekday(datetime.datetime.today())
        prev_date = prev_date.strftime('%Y-%m-%d')
        calendar = self._api.get_calendar(start=prev_date, end=prev_date)[0]
        end_time = calendar.close
        end_date = f'{prev_date}T{end_time}-04:00'
        # create a list of dataframes with names that are the tickers so they are easily called and manipulated
        df_list = {}
        count = 0
        for ticker in ticker_list: # TODO can mabe use workers for this as well
            df_list[f'{ticker}'] = self._api.get_barset(symbols=ticker, timeframe='1Min', start=prev_date,end=end_date, limit=50).df
            if df_list[f'{ticker}'].empty == True: # check if list is empty
                del df_list[f'{ticker}']
            else:
                df_list[f'{ticker}'].columns = df_list[f'{ticker}'].columns.droplevel()
                df_list[f'{ticker}'] = self.calc_ema(df_list[f'{ticker}'])
            count += 1
            if count%200 == 0:
                time.sleep(61)
        return df_list, ticker_list

    # calculate the moving average for the dataframe
    def calc_ema(self,dataframe):
        dataframe['ema5'] = ta.EMA(dataframe['close'],timeperiod=5)
        dataframe['ema10'] = ta.EMA(dataframe['close'],timeperiod=10)
        dataframe['ema20'] = ta.EMA(dataframe['close'],timeperiod=20)
        return dataframe

    # check to see if we should buy or to sell
    def buy_sell_calc(self, dataframe, ticker): 
        # make sure we get the actual symbol
        ticker_symbol = ticker['symbol']
        # check if we can sell
        #TODO can probably add check if ticker_symbol postion is None so that it doesn't have to calculate all of this
        if (dataframe['ema5'][dataframe.index[-1]] > dataframe['ema10'][dataframe.index[-1]] and dataframe['ema5'][dataframe.index[-1]] > dataframe['ema20'][dataframe.index[-1]] and dataframe['ema10'][dataframe.index[-1]] > dataframe['ema20'][dataframe.index[-1]]):
            if (dataframe['ema5'][dataframe.index[-2]] > dataframe['ema10'][dataframe.index[-2]] and dataframe['ema5'][dataframe.index[-2]] > dataframe['ema20'][dataframe.index[-2]] and dataframe['ema10'][dataframe.index[-2]] > dataframe['ema20'][dataframe.index[-2]]): # making sure the it is tending to move upwards
                if (((dataframe['ema5'][dataframe.index[-1]]-dataframe['ema10'][dataframe.index[-1]])/dataframe['ema10'][dataframe.index[-1]]*100) > .5 and ((dataframe['ema5'][dataframe.index[-1]]-dataframe['ema20'][dataframe.index[-1]])/dataframe['ema20'][dataframe.index[-1]]*100) > 2):
                    if self.get_positions_sell(ticker_symbol) is not None:
                        qty, orderside = self.get_positions_sell(ticker_symbol)
                        if orderside == 'long':
                            #limit_price = round(dataframe['close'][dataframe.index[-1]],2) #TODO either take out limit for sell or make it from low column not close
                            self.submitOrder_sell(qty,ticker_symbol,'sell')
        # check if we can buy
        if (dataframe['ema5'][dataframe.index[-1]] < dataframe['ema10'][dataframe.index[-1]] and dataframe['ema5'][dataframe.index[-1]] < dataframe['ema20'][dataframe.index[-1]] and dataframe['ema10'][dataframe.index[-1]] < dataframe['ema20'][dataframe.index[-1]]):
            if (dataframe['ema5'][dataframe.index[-2]] < dataframe['ema10'][dataframe.index[-2]] and dataframe['ema5'][dataframe.index[-2]] < dataframe['ema20'][dataframe.index[-2]] and dataframe['ema10'][dataframe.index[-2]] < dataframe['ema20'][dataframe.index[-2]]):
                if (dataframe['ema5'][dataframe.index[-3]] < dataframe['ema10'][dataframe.index[-3]] and dataframe['ema5'][dataframe.index[-3]] < dataframe['ema20'][dataframe.index[-3]] and dataframe['ema10'][dataframe.index[-3]] < dataframe['ema20'][dataframe.index[-3]]):
                    can_buy = self.get_positions_buy(ticker_symbol)
                    if can_buy == True:
                        quantity = self.calc_num_of_stocks(dataframe)
                        #limit = round(dataframe['close'][dataframe.index[-1]],2)
                        stop = str(round(dataframe['close'][dataframe.index[-1]]*.95,2))
                        stop_loss = {"stop_price": stop, "limit_price": stop}
                        self.submitOrder_buy(quantity,ticker_symbol,'buy',stop_loss)

    # function for submitting buy order
    def submitOrder_buy(self, qty, stock, side, stop):
        if qty > 0:
            try:
                self._api.submit_order(stock, qty, side, 'market', 'day', stop_loss=stop) # TODO might need to get of limit price
                print('Market order of | ' + str(qty) + ' ' + stock + ' ' + side + ' | completed.')
            except Exception as e:
                print('Order of | ' + str(qty) + ' ' + stock + ' ' + side + ' | did not go through.')
                print(f'Error: {str(e)}')
        else:
            print('Quantity is 0, order of | ' + str(qty) + ' ' + stock + ' ' + side + ' | not completed.')
    # function for submitting sell order
    def submitOrder_sell(self, qty, stock, side):
        if qty > 0:
            try:
                self._api.submit_order(stock, qty, side, 'market', 'day')
                print('Market order of | ' + str(qty) + ' ' + stock + ' ' + side + ' | completed.')
            except Exception as e:
                print('Order of | ' + str(qty) + ' ' + stock + ' ' + side + ' | did not go through.')
                print(f'Error: {str(e)}')
        else:
            print('Quantity is 0, order of | ' + str(qty) + ' ' + stock + ' ' + side + ' | not completed.')

    # calculate the number of stocks that we want to buy
    def calc_num_of_stocks(self, dataframe):
        portfolio_value = float(self._api.get_account().buying_power) # TODO might make this equity or actual cash, might be screwing up buying power after market closes with negative cash and open positions
        rough_number = (portfolio_value * self.risk) / dataframe['low'][dataframe.index[-1]]
        quantity = round(rough_number) # make sure it is an even number
        if quantity < 1:
            quantity = 1 # want to buy at least one stock
        return quantity

    # get stock to sell based on if there is already a position
    def get_positions_sell(self, ticker):
        positions = self._api.list_positions()
        if positions != []:
            for position in positions:
                if position.symbol == ticker:
                    if position.side == 'long':
                        orderSide = 'long'
                        qty = abs(int(float(position.qty)))
                        return qty, orderSide

    # see if there is a position already there, if so we do not want to buy
    def get_positions_buy(self, ticker):
        positions = self._api.list_positions()
        can_buy = False
        if positions == []:
            can_buy = True
            return can_buy
        elif positions != []:
            for position in positions:
                if position.symbol != ticker:
                    can_buy = True
                elif position.symbol == ticker:
                    can_buy = False
                    break
            return can_buy

# create our connection to the api and streamconn for our up to date data
api = tradeapi.REST(base_url=base_url,key_id=api_key_id,secret_key=api_secret,api_version='v2')
conn = StreamConn(base_url=base_url,key_id=api_key_id,secret_key=api_secret)

client = Client(account_sid, auth_token) # Keys for twillio api
# global variables
calc = Calculations(api)
data = []
df_ticker_list = []
ticker_list = []
channels = []

# wait for the market to open
async def awaitMarketOpen():
    isOpen = api.get_clock().is_open
    timeList = []
    c = 0
    global df_ticker_list
    global ticker_list
    global channels
    while not isOpen: #TODO fix this
        try:
            clock = api.get_clock()
            openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            timeToOpen = int((openingTime - currTime) / 60)
            if timeToOpen not in timeList:
                print(str(timeToOpen) + ' minutes til market open.')
                timeList.clear()
            timeList.append(timeToOpen)
            # when there is a 5 minutes till open we want to grab our data
            if timeToOpen == 5:
                c += 1
                if c == 1:
                    df_ticker_list, ticker_list = await calc.grab_data()
                    channels = ['AM.' + symbol for symbol in ticker_list]
                    time.sleep(1)
            time.sleep(3)
            isOpen = api.get_clock().is_open
        except Exception as e:
            print('Await Market Open Error')
            print(f'Error: {str(e)}')
            client.messages.create(from_='+13343732933',to='+16207578055',body=f'Await Market Open Error\nError: {str(e)}')
            sys.exit()

    try:
        if df_ticker_list == [] and ticker_list == [] and channels == []:
            df_ticker_list, ticker_list = await calc.grab_data()
            channels = ['AM.' + symbol for symbol in ticker_list]
    except Exception as e:
        print('Grab data Error')
        print(f'Error: {str(e)}')
        print(traceback.format_exc())
        client.messages.create(from_='+13343732933',to='+16207578055',body=f'Grab data Error\nError: {str(e)}')
        sys.exit()

# Wait for market to open
async def run_await():
    print('Waiting for market to open...')
    await awaitMarketOpen()
    print('Market opened.')
    equity = float(api.get_account().equity)
    client.messages.create(from_='+13343732933',to='+16207578055',body=f'Market Open!\nCurrent Equity: ${equity}')

# get the minute data of the stocks that we want
@conn.on(r'^AM.*$')
async def on_minute_bars(conn, channel, bars):
    new_bar = bars._raw
    data.append(new_bar)
    # Append minute data to list otherwise packets will time out and cause an error

# Calculate what do do with every stock and update every stock in list
def calc_everything(data_list):
    y = {'open': data_list['open'],'high':data_list['high'],'low':data_list['low'],'close':data_list['close'],'volume':data_list['volume']}
    symbol = data_list['symbol']
    df_ticker_list[symbol] = df_ticker_list[symbol].append(y, ignore_index=True)
    df_ticker_list[symbol] = calc.calc_ema(df_ticker_list[symbol])
    calc.buy_sell_calc(df_ticker_list[symbol], data_list)

# using ThreadPoolExecutor run calc_everything using workers to increase speed of program
def calc_faster(data_list):
    with concurrent.futures.ThreadPoolExecutor(
                max_workers=10) as executor:
            {executor.submit(calc_everything,i): i for i in data_list}
    data_list.clear() # clear data list so there can be new data in an empty list

# main loop of getting minute data
async def subscribe():
    c = 0
    while True: # making the loop run forever
        await run_await()
        isOpen = api.get_clock().is_open
        while isOpen == True: # making sure the market is still open
            await conn.subscribe(channels) # get data for the stock that we want
            if data != []:
                calc_faster(data) # TODO work on sleeping with so many stocks or only have so many
            clock = api.get_clock()
            closingTime = clock.next_close.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            timeToClose = int((closingTime - currTime) / 60)
            if timeToClose == 4: # if 5 minutes to close, close all positions at market value
                c += 1
                if c == 1: # only want to do this once, will help out with timeout problems
                    positions = api.list_positions()
                    if positions != []:
                        try:
                            api.close_all_positions()
                            await asyncio.sleep(360)
                        except Exception as e:
                            print('Close all Positions Error')
                            print(f'Error: {str(e)}')
            isOpen = api.get_clock().is_open
            if isOpen == False:
                await conn.unsubscribe(channels)
                print('Market Closed')
                equity = float(api.get_account().equity)
                last_equity = float(api.get_account().last_equity)
                price_change = round(equity - last_equity, 2)
                client.messages.create(from_='+13343732933',to='+16207578055',body=f'Current Equity: ${equity}\nPrice Change: ${str(price_change)}')
            await asyncio.sleep(2) # sleep otherwise is_open will timeout

if __name__ == '__main__':
    # main loop and should continue forever unless an error is thrown
    loop = asyncio.new_event_loop()
    loop.create_task(subscribe())
    loop.run_forever()