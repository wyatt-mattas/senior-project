import backtrader as bt
import pandas as pd
#from numba import jit, cuda

class LongOnly(bt.Sizer):
    params = (('stake', 1),)

    def _getsizing(self, comminfo, cash, data, isbuy):
      if isbuy:
          return self.p.stake

      # Sell situation
      position = self.broker.getposition(data)
      if not position.size:
          return 0  # do not sell if nothing is open

      return self.p.stake

class PrintClose(bt.Strategy):

	def __init__(self):
		#Keep a reference to the "close" line in the data[0] dataseries
		self.dataclose = self.datas[0].close

	def log(self, txt, dt=None):
		dt = dt or self.datas[0].datetime.date(0)
		print(f'{dt.isoformat()} {txt}') #Print date and close

	def next(self):
		self.log('Close: ', self.dataclose[0])
#@jit(target ="cuda")
class MAcrossover(bt.Strategy):
	# Moving average parameters
    params = (('pfast',5),('pslow',12),('pslower',17))

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        #print(f'{dt.isoformat()} {txt}') # Comment this line when running optimization

    def __init__(self):
        self.dataclose = self.datas[0].close

		# Order variable will contain ongoing order details/status
        self.order = None

		# Instantiate moving averages
        self.slower_ema = bt.indicators.EMA(self.datas[0], period=self.params.pslower)
        self.slow_ema = bt.indicators.EMA(self.datas[0], period=self.params.pslow)
        self.fast_ema = bt.indicators.EMA(self.datas[0], period=self.params.pfast)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # An active Buy/Sell order has been submitted/accepted - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, {order.executed.price:.2f}')
            elif order.issell():
                self.log(f'SELL EXECUTED, {order.executed.price:.2f}')
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Reset orders
        self.order = None

    def next(self):

        # Check for open orders
        if self.order:
            return

        # Check if we are in the market
        if not self.position:
            # We are not in the market, look for a signal to OPEN trades

            #If the 20 SMA is above the 50 SMA
            # if self.fast_ema[0] > self.slow_ema[0] and self.fast_ema[0] > self.slower_ema[0]:
            #     if self.fast_ema[-1] < self.slow_ema[-1] and self.fast_ema[-1] < self.slower_ema[-1]:
            try:
                if self.fast_ema[-1] < self.slow_ema[-1] and self.fast_ema[-1] < self.slower_ema[-1] and self.slow_ema[-1] < self.slower_ema[-1]:
                    if self.fast_ema[-2] < self.slow_ema[-2] and self.fast_ema[-2] < self.slower_ema[-2] and self.slow_ema[-2] < self.slower_ema[-2]:
                        if self.fast_ema[-3] < self.slow_ema[-3] and self.fast_ema[-3] < self.slower_ema[-3] and self.slow_ema[-3] < self.slower_ema[-3]:
                            self.log(f'BUY CREATE {self.dataclose[0]:2f}')
                            # Keep track of the created order to avoid a 2nd order
                            self.order = self.buy()
                #Otherwise if the 20 SMA is below the 50 SMA
                elif self.fast_ema[-1] > self.slow_ema[-1] and self.fast_ema[-1] > self.slower_ema[-1] and self.slow_ema[-1] > self.slower_ema[-1]:
                    if self.fast_ema[-2] > self.slow_ema[-2] and self.fast_ema[-2] > self.slower_ema[-2] and self.slow_ema[-2] > self.slower_ema[-2]: # making sure the it is tending to move upwards
                        if (((self.fast_ema[-1]-self.slow_ema[-1])/self.slow_ema[-1]*100) > .5 and ((self.fast_ema[-1]-self.slow_ema[-1])/self.slower_ema[-1]*100) > 2):
                            self.log(f'SELL CREATE {self.dataclose[0]:2f}')
                            # Keep track of the created order to avoid a 2nd order
                            self.order = self.sell()
            except Exception as e:
                print(f'{str(e)}: Fast EMA{self.fast_ema[-1]}')
        else:
            # We are already in the market, look for a signal to CLOSE trades
            if len(self) >= (self.bar_executed + 5):
                self.log(f'CLOSE CREATE {self.dataclose[0]:2f}')
                self.order = self.close()

class RSIStrat(bt.Strategy):

    params = (('pfast',3),('pslow',11),('rsi',3),('rsiLow',25),('rsiHigh',73))

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        #print(f'{dt.isoformat()} {txt}') # Comment this line when running optimization

    def __init__(self):
        self.dataclose = self.datas[0].close
        #self.bought = False
        self.in_position = False
		# Order variable will contain ongoing order details/status
        self.order = None
        self.fast_ema = bt.indicators.EMA(self.dataclose, period=self.params.pfast)
        self.slow_ema = bt.indicators.EMA(self.dataclose, period=self.params.pslow)
        self.rsi = bt.talib.RSI(self.dataclose, timeperiod=self.params.rsi)

    def notify_order(self, order):
        #bought = self.bought

        if order.status in [order.Submitted, order.Accepted]:
            # An active Buy/Sell order has been submitted/accepted - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                #self.log(f'BUY EXECUTED, {order.executed.price:.2f}')
                self.log('BUY EXECUTED, %.2f' % self.dataclose[0])
            elif order.issell():
                #self.log(f'SELL EXECUTED, {order.executed.price:.2f}')
                self.log('SELL EXECUTED, %.2f' % self.dataclose[0])
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Reset orders
        self.order = None
    '''
    work on reducing negative sells with checking the price change percentage for 5ish minutes
    '''
    def next(self):
        prices = [self.fast_ema[0],self.slow_ema[0]]

        price_series = pd.Series(prices)
        price_change = price_series.pct_change()
        ema_price_change = price_change[1]
        # c = 0
        # for i in price_change:
        #     if i <= .003 and i >= -.003:
        #         c += 1

        if self.order:
            return
        #if the symbol is overbought
        if self.rsi[0] >= self.params.rsiHigh and self.fast_ema[0] > self.slow_ema[0]:
            #if in position is true, sell
            if self.in_position:
                #print("Sell")
                self.order = self.sell()
                self.in_position = False
            else:
                pass
        #if the symbol is oversold
        elif self.rsi[0] <= self.params.rsiLow and self.fast_ema[0] < self.slow_ema[0]:
            if self.in_position or self.rsi[0] <= 10 or ema_price_change < .00001:
                pass
            #buy if not in position
            else:
                #print("Buy")
                self.order = self.buy()
                self.in_position = True

    # def next(self):

    #     # Check for open orders
    #     if self.order:
    #         return

    #     # Check if we are in the market
    #     if not self.position:
    #         # We are not in the market, look for a signal to OPEN trades

    #         if self.rsi <= 30 and self.fast_ema[0] > self.slow_ema[0]:
    #             #if self.bought == False:
    #             self.order = self.buy()
    #                 #self.bought == True
    #                 #return self.bought
    #         elif self.rsi >= 70 and self.fast_ema[0] < self.slow_ema[0]:
    #             #if self.bought == False:
    #             self.order = self.sell()
    #                 #self.bought == False
    #                 #return self.bought
    #     else:
    #         # We are already in the market, look for a signal to CLOSE trades
    #         if len(self) >= (self.bar_executed + 4):
    #             self.log(f'CLOSE CREATE {self.dataclose[0]:2f}')
    #             self.order = self.close()

class RSIStratBreakout(bt.Strategy):
    #TODO breakout strat
    '''
    should have to be several minutes below a certain rsi value with ema verification to buy
    should have to be several minutes above a certain rsi value with ema verification to sell
    need to wait for it to cross into the middle zone to execute trades
    need to work on middle zone values, rsi value(probably bigger)
    and make sure the ema values are still good for verifying rsi indicators
    '''
    params = (('pfast',3),('pslow',11),('rsi',6),('rsiLow',30),('rsiHigh',70))

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        #print(f'{dt.isoformat()} {txt}') # Comment this line when running optimization

    def __init__(self):
        self.dataclose = self.datas[0].close
        #self.bought = False
        self.in_position = False
		# Order variable will contain ongoing order details/status
        self.order = None
        self.fast_ema = bt.indicators.EMA(self.dataclose, period=self.params.pfast)
        self.slow_ema = bt.indicators.EMA(self.dataclose, period=self.params.pslow)
        self.rsi = bt.talib.RSI(self.dataclose, timeperiod=self.params.rsi)

    def notify_order(self, order):
        #bought = self.bought

        if order.status in [order.Submitted, order.Accepted]:
            # An active Buy/Sell order has been submitted/accepted - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                #self.log(f'BUY EXECUTED, {order.executed.price:.2f}')
                self.log('BUY EXECUTED, %.2f' % self.dataclose[0])
            elif order.issell():
                #self.log(f'SELL EXECUTED, {order.executed.price:.2f}')
                self.log('SELL EXECUTED, %.2f' % self.dataclose[0])
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Reset orders
        self.order = None
    #Version 1
    def next(self):
        if self.order:
            return
        #if the symbol is overbought
        if self.rsi[0] <= self.params.rsiHigh and self.rsi[-1] >= self.params.rsiHigh and self.rsi[-2] >= self.params.rsiHigh and self.rsi[-3] >= self.params.rsiHigh and self.rsi[-4] >= self.params.rsiHigh and self.fast_ema[0] > self.slow_ema[0]:
            #if in position is true, sell
            if self.in_position:
                #print("Sell")
                self.order = self.sell()
                self.in_position = False
            else:
                pass
        #if the symbol is oversold
        elif self.rsi[0] >= self.params.rsiLow and self.rsi[-1] <= self.params.rsiLow and self.rsi[-2] <= self.params.rsiLow and self.rsi[-3] <= self.params.rsiLow and self.rsi[-4] <= self.params.rsiLow and self.fast_ema[0] < self.slow_ema[0]:
            if self.in_position:
                pass
            #buy if not in position
            else:
                #print("Buy")
                self.order = self.buy()
                self.in_position = True