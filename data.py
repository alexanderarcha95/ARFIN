%pip install python-binance

import os
from binance.client import Client
import pandas as pd
# Test 
client = Client(api_key, api_secret, testnet=True)

from sqlalchemy.engine import create_engine
import sqlite3
from pandas.io import sql
import subprocess
%pip install ipython-sql
%load_ext sql
%sql sqlite:///arfin.db

header = ['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_time', 'Quote_asset_volume', 'Number_of_trades', 'Taker_buy_base_asset_volume', 'Taker_buy_quote_asset_volume', 'Ignore']
def load():
    for symbol in ['BNBBTC','ETHBTC']:
        data = pd.DataFrame(client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1MINUTE, "1 day ago UTC"),columns = header)
        data['symbol'] = symbol
        data['KeyFigureType'] = 'KLINE_INTERVAL_1MINUTE'
        data['Version'] = 'A'

        conn = sqlite3.connect('arfin.db')
        ##push the dataframe to sql
        print(data['symbol'][1])
        data.to_sql('data', conn, if_exists="replace")

        ##create the table
        conn.execute(
            """
            insert into t_h_data
            select * from data;
            """)
        conn.commit()
        conn.close()
load()
conn = sqlite3.connect('arfin.db')
df = pd.read_sql_query('select * from t_h_data;', conn, parse_dates=["E"])
df
