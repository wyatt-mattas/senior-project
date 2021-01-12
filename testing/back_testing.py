import backtrader as bt
import csv
import datetime
import backtrader.feeds as btfeeds
from strategies import MAcrossover, RSIStrat, RSIStratBreakout
#import logging
from backtrader_plotting import Bokeh
# from strategies import LongOnly
# from numba import jit, cuda
#logger = logging.getLogger(__name__)
cerebro = bt.Cerebro()

cerebro.broker.set_cash(10000.0)

data = btfeeds.GenericCSVData(
    dataname='C:\\Users\\matta\\Documents\\Stock_Application\\senior-project\\testing\\btcusdt_data_shortest.csv',

    fromdate=datetime.datetime(2019, 12, 1),
    todate=datetime.datetime(2019, 12, 31),

    nullvalue=0.0,

    timeframe=bt.TimeFrame.Minutes,
    compression=1,
    dtformat=('%Y-%m-%d %H:%M:%S'),
    datetime=0,
    time=-1,
    high=2,
    low=3,
    open=1,
    close=4,
    volume=-1,
    openinterest=-1
    )

cerebro.adddata(data)

cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe_ratio', timeframe=bt.TimeFrame.Minutes)
cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn_ratio')
#cerebro.optstrategy(MAcrossover, pfast=range(3,10), pslow=range(11,19), pslower=range(20,29))  # Add the trading strategy
#cerebro.addstrategy(MAcrossover, pfast=9, pslow=12, pslower=20)
cerebro.addstrategy(RSIStrat, pfast=3, pslow=11, rsi=3, rsiLow=25, rsiHigh=73)
#cerebro.addstrategy(RSIStratBreakout)
#cerebro.optstrategy(RSIStrat, rsi=range(2,15))
# Default position size
cerebro.addsizer(bt.sizers.SizerFix, stake=1)

if __name__ == '__main__':
    # run()
    # cerebro.broker.set_cash(100000)

    try:
        print('Running...')
        optimized_runs = cerebro.run()
        strat = optimized_runs[0]
        print('Sharpe Ratio:', strat.analyzers.sharpe_ratio.get_analysis())
        print('SQN Ratio:', strat.analyzers.sqn_ratio.get_analysis())
        PnL = round(cerebro.broker.get_value() - 10000, 2)
        print(PnL)
        b = Bokeh(style='bar', plot_mode='single')
        cerebro.plot(b)
        print('Finished!')
    except Exception as e:
        print(f'Run Error {str(e)}')


    # try:
    #     print('Running...')
    #     optimized_runs = cerebro.run(runonce=False, exactbars=-1)
    #     print('Finished!')
    #     final_results_list = []
    #     # Iterate through list of lists
    #     for run in optimized_runs:
    #         for strategy in run:
    #             PnL = round(cerebro.broker.get_value() - 10000, 2)
    #             sharpe = strategy.analyzers.sharpe_ratio.get_analysis()
    #             sqn = strategy.analyzers.sqn_ratio.get_analysis()
    #             final_results_list.append(
    # 				[
    # 					strategy.params.pfast,
    # 					strategy.params.pslow,
    #                     strategy.params.rsi,
    #                     strategy.params.rsiLow,
    #                     strategy.params.rsiHigh,
    # 					PnL,
    # 					sharpe['sharperatio'],
    #                     sqn['sqn'],
    # 				]
    # 			)

    #     sort_by_sharpe = sorted(final_results_list, key=lambda x: x[7], reverse=True)
    #     # Print top 5 results sorted by Sharpe Ratio
    #     #c=0
    #     print('Results')
    #     #print(final_results_list)
    #     for line in sort_by_sharpe[:5]:
    #         print(line)

    # except Exception as e:
    #     print(f'Error {str(e)}')
    #     print(logger.exception(e))
