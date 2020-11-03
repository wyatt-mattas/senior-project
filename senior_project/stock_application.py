import csv
import talib._ta_lib as ta
from datetime import timedelta
import datetime
import alpaca_trade_api as tradeapi
import requests
from alpaca_trade_api import StreamConn
import time
import asyncio
from calendar import monthrange
import concurrent.futures

base_url = 'https://paper-api.alpaca.markets' # 'https://paper-api.alpaca.markets' - used for paper account
api_key_id = 'PK9XU6G5CC77W5WL1AIS'
api_secret = 'PIH84fvxhYh37k4Bxp1ieEOcyiCZnxkLYufW2FzV'

#TODO need to work on shorting the market instead of just selling

class Main:
    def __init__(self, api):
        # initalize some variables
        self._api = api
        self.min_share_price = 1.00
        self.max_share_price = 13.00
        self.min_last_dv = 500000
        self.risk = 0.01

    # get a list of tickers that meet a certain criteria
    def get_tickers(self):
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
        f = open("end_of_month.txt", "w")
        f.write(closest_to_end_of_month) # overwrite file to new date
        f.close()

    #grab data for each stock that is in the list
    async def grab_data(self):
        end_of_month = open('C:\\Users\\matta\\Documents\\test\\senior-project\\senior_project\\end_of_month.txt', 'r').read()
        today = datetime.datetime.today()
        today = today.strftime('%Y-%m-%d')

        if(today == end_of_month): # grab new list of tickers for the month, might want to change to every two weeks
            self.get_new_date()
            ticker_list = self.get_tickers()
            with open('ticker_list.csv', 'w') as myfile:
                wr = csv.writer(myfile)
                wr.writerow(ticker_list) # overwrite csv file with list of new tickers
            positions = self._api.list_positions()
            if positions != [] :
                for position in positions:
                    if(position.side == 'long'):
                        if(position.symbol not in ticker_list): # want to sell off tickers that have not close yet 
                            qty = abs(int(float(position.qty)))
                            self._api.submit_order(position.symbol, qty, 'sell', 'market', 'day') # TODO might want to change this
        # read the ticker list from text file
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
        # create a list of dataframes with names that are the tickers so they are easily called and manipulated
        df_list = {}
        for ticker in ticker_list: # TODO can mabe use workers for this as well
            df_list[f'{ticker}'] = self._api.get_barset(symbols=ticker, timeframe='1Min', start=prev_date,end=end_date, limit=50).df
            df_list[f'{ticker}'].columns = df_list[f'{ticker}'].columns.droplevel()
            df_list[f'{ticker}'] = self.calc_ema(df_list[f'{ticker}'])
        return df_list, ticker_list

    # calculate the moving average for the dataframe
    def calc_ema(self,dataframe):
        dataframe['ema5'] = ta.EMA(dataframe['close'],timeperiod=5)
        dataframe['ema15'] = ta.EMA(dataframe['close'],timeperiod=15)
        dataframe['ema40'] = ta.EMA(dataframe['close'],timeperiod=40)
        return dataframe

    # check to see if we should buy or to sell
    def buy_sell_calc(self, dataframe, ticker):
        # make sure we get the actual symbol
        ticker_symbol = ticker['symbol']
        if (dataframe['ema5'][dataframe.index[-1]] > dataframe['ema15'][dataframe.index[-1]] and dataframe['ema5'][dataframe.index[-1]] > dataframe['ema40'][dataframe.index[-1]] and dataframe['ema15'][dataframe.index[-1]] > dataframe['ema40'][dataframe.index[-1]]):
            if(dataframe['ema5'][dataframe.index[-2]] > dataframe['ema15'][dataframe.index[-2]] and dataframe['ema5'][dataframe.index[-2]] > dataframe['ema40'][dataframe.index[-2]] and dataframe['ema15'][dataframe.index[-2]] > dataframe['ema40'][dataframe.index[-2]]): # making sure the it is tending to move upwards
                if (((dataframe['ema5'][dataframe.index[-1]]-dataframe['ema15'][dataframe.index[-1]])/dataframe['ema15'][dataframe.index[-1]]*100) > .5 and ((dataframe['ema5'][dataframe.index[-1]]-dataframe['ema40'][dataframe.index[-1]])/dataframe['ema40'][dataframe.index[-1]]*100) > 2):
                    if self.get_positions_sell(ticker_symbol) is not None:
                        qty, orderside = self.get_positions_sell(ticker_symbol)
                        if orderside == 'long':
                            self.submitOrder(qty,ticker_symbol,'sell')

        if (dataframe['ema5'][dataframe.index[-1]] < dataframe['ema15'][dataframe.index[-1]] and dataframe['ema5'][dataframe.index[-1]] < dataframe['ema40'][dataframe.index[-1]] and dataframe['ema15'][dataframe.index[-1]] < dataframe['ema40'][dataframe.index[-1]]):
            if (dataframe['ema5'][dataframe.index[-2]] < dataframe['ema15'][dataframe.index[-2]] and dataframe['ema5'][dataframe.index[-2]] < dataframe['ema40'][dataframe.index[-2]] and dataframe['ema15'][dataframe.index[-2]] < dataframe['ema40'][dataframe.index[-2]]):
                if (dataframe['ema5'][dataframe.index[-3]] < dataframe['ema15'][dataframe.index[-3]] and dataframe['ema5'][dataframe.index[-3]] < dataframe['ema40'][dataframe.index[-3]] and dataframe['ema15'][dataframe.index[-3]] < dataframe['ema40'][dataframe.index[-3]]):
                    can_buy = self.get_positions_buy(ticker_symbol)
                    if can_buy == True:
                        quantity = self.calc_num_of_stocks(ticker)
                        self.submitOrder(quantity,ticker_symbol,'buy')

    # function for submitting the order
    def submitOrder(self, qty, stock, side):
        if(qty > 0):
            try:
                self._api.submit_order(stock, qty, side, 'market', 'day')
                print('Market order of | ' + str(qty) + ' ' + stock + ' ' + side + ' | completed.')
            except:
                print('Order of | ' + str(qty) + ' ' + stock + ' ' + side + ' | did not go through.')
        else:
            print('Quantity is 0, order of | ' + str(qty) + ' ' + stock + ' ' + side + ' | not completed.')

    # calculate the number of stocks that we want to buy
    def calc_num_of_stocks(self, stock):
        portfolio_value = float(api.get_account().buying_power) # TODO might make this equity
        rough_number = (portfolio_value * self.risk) / stock['low']
        quantity = round(rough_number) # make sure it is an even number
        if (quantity < 1):
            quantity = 1
        return quantity

    # get stock to sell based on if there is already a position
    def get_positions_sell(self, ticker):
        positions = self._api.list_positions()
        if (positions != []):
            for position in positions:
                if (position.symbol == ticker):
                    if(position.side == 'long'):
                        orderSide = 'long'
                        qty = abs(int(float(position.qty)))
                        return qty, orderSide

    # see if there is a position already there, if so we do not want to buy
    def get_positions_buy(self, ticker):
        positions = self._api.list_positions()
        can_buy = False
        if (positions == []):
            can_buy = True
            return can_buy
        elif (positions != []):
            for position in positions:
                if (position.symbol != ticker):
                    can_buy = True
                elif(position.symbol == ticker):
                    can_buy = False
                    break
            return can_buy


# create our connection to the api and streamconn for our up to date data
api = tradeapi.REST(base_url=base_url,key_id=api_key_id,secret_key=api_secret,api_version='v2')
conn = StreamConn(base_url=base_url,key_id=api_key_id,secret_key=api_secret)

# global variables
ema = Main(api)
data = []
df_ticker_list = []
ticker_list = []
channels = []

# wait for the market to open
async def awaitMarketOpen():
    isOpen = api.get_clock().is_open
    while(not isOpen):
        clock = api.get_clock()
        openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
        currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
        timeToOpen = int((openingTime - currTime) / 60)
        print(str(timeToOpen) + ' minutes til market open.')
        # when there is a 5 minutes till open we want to grab our data
        #if(timeToOpen == 0):
            # global df_ticker_list
            # global ticker_list
            # df_ticker_list, ticker_list = await ema.grab_data()
            # global channels
            # channels = ['AM.' + symbol for symbol in ticker_list]
        time.sleep(60)
        isOpen = api.get_clock().is_open
    global df_ticker_list
    global ticker_list
    df_ticker_list, ticker_list = await ema.grab_data()
    global channels
    channels = ['AM.' + symbol for symbol in ticker_list]

# Wait for market to open
async def run_await():
    print('Waiting for market to open...')
    await awaitMarketOpen()
    print('Market opened.')

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
    df_ticker_list[symbol] = ema.calc_ema(df_ticker_list[symbol])
    ema.buy_sell_calc(df_ticker_list[symbol], data_list)

# using ThreadPoolExecutor run calc_everything using workers to increase speed of program
def calc_faster(data_list):
    with concurrent.futures.ThreadPoolExecutor(
                max_workers=150) as executor:
            {executor.submit(calc_everything,i): i for i in data_list}
    data_list.clear() # clear data list so there can be new data in an empty list

# main loop of getting minute data
async def subscribe():
    while True: # making the loop run forever
        await run_await()
        await get_clock()
        global channels
        while(await get_clock() == True): # making sure the market is still open
            await conn.subscribe(channels) # get data for the stock that we want
            if (data != []):
                calc_faster(data)
            await get_clock()
            #print(await get_clock())
            await asyncio.sleep(10) # sleep otherwise is_open will timeout
            if(await get_clock() == False):
                await conn.unsubscribe(channels)
                print('Market Closed')

async def get_clock():
    isOpen = api.get_clock().is_open
    return isOpen

if __name__ == '__main__':
    # main loop and should continue forever unless an error is thrown
    loop = asyncio.new_event_loop()
    loop.create_task(subscribe())
    loop.run_forever()