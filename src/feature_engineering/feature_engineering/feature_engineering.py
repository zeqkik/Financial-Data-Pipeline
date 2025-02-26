import pandas as pd
import sqlite3
import os
import yfinance as yf

base_path = os.path.abspath(__file__)

conn = sqlite3.connect(r"data\database\financial_data.db")

with open(r"src\feature_engineering\feature_engineering\close_window_30_90_180.sql", 'r', encoding='utf-8') as file:
    window_query = file.read().strip() 
with open(r"src\feature_engineering\feature_engineering\pct_change.sql", 'r', encoding='utf-8') as file:
    pct_query = file.read().strip() 

df = pd.read_sql("SELECT * FROM assets;", conn)
df_mean_window = pd.read_sql(window_query, conn)
df_pct_change = pd.read_sql(pct_query, conn)

feature_table = df.merge(df_mean_window, on=['date','ticker'], how='left')
feature_table = feature_table.merge(df_pct_change, on=['date','ticker'], how='left')

feature_table.to_sql('feature_table', conn, if_exists='replace', index=False)
