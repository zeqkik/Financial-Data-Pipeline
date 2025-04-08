import pandas as pd
import yfinance as yf
import sqlite3
from datetime import date
import os

DATABASE_PATH = os.getenv("DATABASE_PATH", "data/database/financial_data.db")



def ingest_data(load_type="full"):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM assets")
    records_before = cursor.fetchone()[0]

    tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "BRK-B", "META", "JNJ", "V", "XOM", "UNH", "PG", "HD", "MA", "CVX", "PEP", "KO", "LLY", "ABBV"]
    tickers_str = " ".join(tickers)

    cursor.execute("SELECT COUNT(*) FROM assets")

    
    if(load_type=="full"):
        # Historic 5 years data
        start_date = "2020-01-01"
    
    elif(load_type=="incremental"):
        cursor.execute("SELECT MAX(date) FROM assets")
        last_date = cursor.fetchone()[0]
        start_date = (pd.to_datetime(last_date) + pd.DateOffset(days=1)).strftime("%Y-%m-%d") #load data since d+1 from last date present in databse
        if(not(last_date)):
            raise ValueError( "Not possible to do incremental load, historical data is missing. Please run a full load first.")
        
    else:
        raise ValueError("Invalid load type - It should be 'full' or 'incremental'.")
    
    end_date = str(date.today())
    print(f"DEBUG - Start Date: {start_date}, End Date: {end_date}")
    tickers_data = yf.download(tickers_str, start=start_date, end=end_date,  actions=True, group_by="ticker")

    # Creating the dataframe in the same schema as the database

    data_list = []

    for ticker in tickers_data.columns.levels[0]:
        # Selecionar o DataFrame para cada ticker
        df = tickers_data[ticker].copy()
        
        # Adicionar o ticker como uma nova coluna
        df['Ticker'] = ticker
        
        # Resetar o índice para incluir as datas como uma coluna
        df = df.reset_index()
        
        # Adicionar o DataFrame ajustado à lista
        data_list.append(df)

    # Concatenar todos os DataFrames ajustados
    final_df = pd.concat(data_list, ignore_index=True)

    final_df = final_df.rename(columns={
        'Price': 'id',
        'Date': 'date',
        'High': 'high',
        'Open': 'open',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume',
        'Dividends': 'dividends',
        'Stock Splits': 'stock_splits',
        'Ticker': 'ticker'
    })[['ticker', 'date', 'high', 'open', 'low', 'close', 'volume', 'dividends', 'stock_splits']]

    # Converter 'date' para string no formato YYYY-MM-DD
    final_df['date'] = final_df['date'].dt.strftime('%Y-%m-%d')

    # Garantir que 'volume' seja do tipo float
    final_df['volume'] = final_df['volume'].astype(float)
    


    final_df.to_sql('assets', conn, if_exists='append', index=False)
    cursor.execute("SELECT COUNT(*) FROM assets")
    records_after = cursor.fetchone()[0]
    records_inserted = records_after - records_before
    print(f"{records_inserted} registros inseridos no banco.")
    conn.close()

if __name__ == "__main__":
    load_type = os.getenv("LOAD_TYPE", "incremental")  # Default: incremental
    ingest_data(load_type)
