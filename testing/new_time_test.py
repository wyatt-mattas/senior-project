import pandas as pd
from datetime import timedelta, date
import datetime
import alpaca_trade_api as tradeapi
import time

# iso_day = datetime.now()

#print(pd.Timestamp(year=2018,month=1,day=3,tz='US/Eastern').isoformat())
#print(pd.Timestamp(iso_day,tz='US/Eastern').isoformat())

# df = pd.read_csv('E:\senior_project\CEI_get_barset_data.csv', index_col=0)
# print(df)

# ct = datetime.now().strftime('%H:%M:%S')
# print(ct)
base_url = 'https://paper-api.alpaca.markets'
api_key_id = open('C:\\Account IDs\\AlpacaAPIIDOrig.txt', 'r').read()
api_secret = open('C:\\Account IDs\\AlpacaAPISecretOrig.txt', 'r').read()

api = tradeapi.REST(base_url=base_url,key_id=api_key_id,secret_key=api_secret,api_version='v2')
#conn = StreamConn(base_url=base_url,key_id=api_key_id,secret_key=api_secret)
# clock = api.get_clock()
# while True:
#     closingTime = clock.next_close.replace(tzinfo=datetime.timezone.utc).timestamp()
#     currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
#     timeToClose = int((closingTime - currTime) / 60)
#     print(timeToClose)
#     time.sleep(10)

print(type(datetime.datetime.today().weekday()))