import pandas as pd
from os import path as path

# stock = 'AMD'
# #if path.exists(f'E:\\senior_project\\ema_csv\\{stock}_EMAS.csv') == True:
# df = pd.read_csv(f'E:\\senior_project\\ema_csv\\{stock}_EMAS.csv')

# ema5 = df['ema5'][df.index[-1]]
# print(ema5)
prices = [30.4, 32.5]#, 31.7, 31.2, 32.7, 34.1, 35.8, 37.8, 36.3, 36.3, 35.6]

price_series = pd.Series(prices)
priceChange = price_series.pct_change()
print(priceChange[1])