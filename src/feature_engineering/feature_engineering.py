import pandas as pd
import sqlite3
import os
import yfinance as yf
from sklearn.preprocessing import LabelEncoder

base_path = os.path.abspath(__file__)
DATABASE_PATH = os.getenv("DATABASE_PATH", "data/database/financial_data.db")
SQL_PATH = os.getenv("SQL_PATH", "src/feature_engineering")
conn = sqlite3.connect(DATABASE_PATH)

with open(os.path.join(SQL_PATH, "close_window_30_90_180.sql"), 'r', encoding='utf-8') as file:
    window_query = file.read().strip() 
with open(os.path.join(SQL_PATH, "pct_change.sql"), 'r', encoding='utf-8') as file:
    pct_query = file.read().strip() 

df = pd.read_sql("SELECT * FROM assets;", conn)
df_mean_window = pd.read_sql(window_query, conn)
df_pct_change = pd.read_sql(pct_query, conn)

feature_table = df.merge(df_mean_window, on=['date','ticker'], how='left')
feature_table = feature_table.merge(df_pct_change, on=['date','ticker'], how='left')

# Date Encoding 
feature_table['date'] = pd.to_datetime(feature_table['date'])  
feature_table['day_of_week'] = feature_table['date'].dt.dayofweek  
feature_table['day_of_month'] = feature_table['date'].dt.day
feature_table['month'] = feature_table['date'].dt.month
feature_table['year'] = feature_table['date'].dt.year

# Label Encoding
le = LabelEncoder()
feature_table.loc[:,'ticker_encoded'] = le.fit_transform(feature_table['ticker'])


feature_table.to_sql('feature_table', conn, if_exists='replace', index=False)
print('feature_table updated')
conn.commit()
conn.close()