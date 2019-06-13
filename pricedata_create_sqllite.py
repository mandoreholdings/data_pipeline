import os
import pandas as pd

from sqlalchemy import create_engine

datadir = '../rawdata/Price_data_32234_1_120_20190225_060847/'
print(os.listdir(datadir))
price_file_name = os.path.join(datadir, '32234_1_120_20190225_060847_dat.txt')
csv_database = create_engine('sqlite:///csv_database.db')

#Create SQLite db
if False:
    chunksize = 100000
    i = 0
    j = 1
    for df in pd.read_csv(price_file_name, chunksize=chunksize, iterator=True,
                         delimiter='|'):
          df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
          df.index += j
          i+=1
          df.to_sql('PriceTable', csv_database, if_exists='append')
          j = df.index[-1] + 1


