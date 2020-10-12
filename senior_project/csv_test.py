import pandas as pd
from os import path as path

stock = 'AMD'
#if path.exists(f'E:\\senior_project\\ema_csv\\{stock}_EMAS.csv') == True:
df = pd.read_csv(f'E:\\senior_project\\ema_csv\\{stock}_EMAS.csv')

ema5 = df['ema5'][df.index[-1]]
print(ema5)