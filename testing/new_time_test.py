import pandas as pd
from datetime import datetime
from datetime import date

iso_day = datetime.now()

#print(pd.Timestamp(year=2018,month=1,day=3,tz='US/Eastern').isoformat())
#print(pd.Timestamp(iso_day,tz='US/Eastern').isoformat())

df = pd.read_csv('E:\senior_project\CEI_get_barset_data.csv', index_col=0)
print(df)