import alpaca_trade_api as tradeapi
import requests
import concurrent.futures
import logging
#import matplotlib.pyplot as plt # TODO make candlestick chart for minutes to see how it looks
#from mpl_finance import candlestick_ohlc
import plotly.graph_objects as go
from plotly.offline import plot
import pandas as pd
from datetime import datetime
from datetime import date
#import matplotlib.dates as mpl_dates
#import alphavantage
import talib._ta_lib as ta

iso_day = datetime.now()

#plt.style.use('ggplot')
logger = logging.getLogger(__name__)

base_url = 'https://paper-api.alpaca.markets' # 'https://paper-api.alpaca.markets' - used for paper account
api_key_id = 'PKLIQDAA55PPU4YZEQEZ'
api_secret = 'SnYX9janNUqheD4PrzrUDTH1Jghj3UUcVwy248BA'

api = tradeapi.REST(
    base_url=base_url,
    key_id=api_key_id,
    secret_key=api_secret,
    api_version='v2')
account = api.get_account()
print(account.status) # make sure that account is active with if statement
if account.status == 'ACTIVE':
    session = requests.session()
    def run(self):
        # First, cancel any existing orders so they don't impact our buying power.
        orders = self._api.list_orders(status='open')
        for order in orders:
            self._api.cancel_order(order.id)

        # Wait for market to open.
        print('Waiting for market to open...')
        loop = asyncio.new_event_loop()
        loop.create_task(self.awaitMarketOpen)
        loop.run_until_complete()
        # tAMO = threading.Thread(target=self.awaitMarketOpen)
        # tAMO.start()
        # tAMO.join()
        print('Market opened.')

        #self.ws_thread.start()

        # Rebalance the portfolio every minute, making necessary trades.
        while True:
            # Figure out when the market will close so we can prepare to sell beforehand.
            #print('test')
            clock = self._api.get_clock()
            closingTime = clock.next_close.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            self.timeToClose = closingTime - currTime

            if(self.timeToClose < (60 * 5)):
                # Close all positions when 10 minutes til market close.
                #self.ws_thread.join()
                print('Market closing soon.  Closing positions.') #TODO get postions at end of day and start of next market see what I am starting with
                positions = self._api.list_positions()
                for position in positions:
                    if(position.side == 'long'):
                        orderSide = 'sell'
                    else:
                        orderSide = 'sell'
                    qty = abs(int(float(position.qty)))
                    tSubmitOrder = threading.Thread(target=self.submitOrder(qty, position.symbol, orderSide))
                    tSubmitOrder.start()
                    tSubmitOrder.join()

                # Run script again after market close for next trading day.
                print('Sleeping until market close (5 minutes).')
                time.sleep(60 * 5)
            else:
                time.sleep(5)
                continue
    #tickers = api.polygon.all_tickers()
    #assets = api.list_assets()
    min_share_price = 0.50
    max_share_price = 13.0
    # Minimum previous-day dollar volume for a stock we might consider
    min_last_dv = 500000
    # Stop limit to default to
    #default_stop = .95
    # How much of our portfolio to allocate to any one position
    #risk = 0.001

    #print(type(api.get_barset(symbols='AAPL', timeframe='1Min', start='2018-01-03T10:00:00-04:00',end=pd.Timestamp(iso_day,tz='US/Eastern').isoformat()).df))

    #barset = api.get_barset(symbols='AAPL', timeframe='1Min', start='2018-01-03T10:00:00-04:00',end=pd.Timestamp(iso_day,tz='US/Eastern').isoformat()).df
    #print(barset.columns[0][0])

    def get_tickers(): # TODO need to make this the universe -- make this save to csv -- or could use api.add_watchlist()
        print('Getting current ticker data...')
        tickers = api.polygon.all_tickers()
        print('Success.')
        assets = api.list_assets()
        symbols = [asset.symbol for asset in assets if asset.tradable]
        return [ticker for ticker in tickers if (
            ticker.ticker in symbols and
            ticker.lastTrade['p'] >= min_share_price and
            ticker.lastTrade['p'] <= max_share_price and
            ticker.prevDay['v'] * ticker.lastTrade['p'] > min_last_dv and
            ticker.todaysChangePerc >= 3.5
        )]

    def historic_ticker(symbols):
        return api.get_barset(symbols=symbols, timeframe='1Min', start='2018-01-03T10:00:00-04:00',end=pd.Timestamp(iso_day,tz='US/Eastern').isoformat()).df # TODO change to day data instead

    def ticker_hist_data(symbols):
        #c = 0
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=150) as executor:
            results = {}
            future_to_symbol = {executor.submit(historic_ticker,symbol): symbol for symbol in symbols}
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    results[symbol] = future.result()
                    #c += 1
                    #print('{}/{}'.format(c, len(symbols)))
                except Exception as exc:
                    logger.warning('{} generated an exception: {}'.format(symbol, exc))
            return results

    def calc_ema(list_data, ticker_data):
        df_list = list()
        for i in ticker_data:
            df = list_data[i]
            df['ema5'] = ta.EMA(df[i]['close'], timeperiod=5)
            df['ema15'] = ta.EMA(df[i]['close'], timeperiod=15)
            df['ema40'] = ta.EMA(df[i]['close'], timeperiod=40)
            df_list.append(df)
        return df_list

    tickers = get_tickers()
    ticker_list = [ticker.ticker for ticker in tickers]
    ticker_list = sorted(ticker_list, key=str.lower)
    print(len(ticker_list))
    #ticker_list = ['AMD']
    print(ticker_list)
    print('ok')
    #print(len(ticker_list))

    data = ticker_hist_data(ticker_list)
    #new_data.to_csv('ticker_data.csv')

    all_data = calc_ema(data, ticker_list)
    for i in range(len(ticker_list)):
        last_ticker = ticker_list[i]
        barset = api.get_barset(last_ticker, '1Min', start='2018-01-03T10:00:00-04:00',end=pd.Timestamp(iso_day,tz='US/Eastern').isoformat()).df
        print(barset.columns[0][0])
        barset.to_csv(f'E:\\senior_project\\barset_data\\{ticker_list[i]}_get_barset_data.csv')


    # ---------------------------------------------

    new_data = all_data[-1]

    print(type(new_data))

    ticker_df_name = new_data.columns[0][0]
    print(ticker_df_name)

    candle = go.Candlestick(
		open = new_data[ticker_df_name]['open'],
		close = new_data[ticker_df_name]['close'],
		high = new_data[ticker_df_name]['high'],
		low = new_data[ticker_df_name]['low'],
		name = "Candlesticks")
	# plot MAs
    fema = go.Scatter(y = new_data['ema5'], name = "Fast EMA", line = dict(color = ('rgba(102, 207, 255, 50)')))
    mema = go.Scatter(y = new_data['ema15'], name = "Medium EMA", line = dict(color = ('rgba(120, 255, 205, 50)')))
    sema = go.Scatter(y = new_data['ema40'], name = "Slow EMA", line = dict(color = ('rgba(50, 255, 100, 50)')))

    data = [candle, fema, mema, sema]

    fig = go.Figure(data = data)

    plot(fig)

else:
    print(account.status + 'need to try again later')
