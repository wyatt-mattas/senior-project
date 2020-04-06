import pandas as pd

csv_data = pd.read_csv('ticker_data.csv')
for col in csv_data.columns:
    print(col)