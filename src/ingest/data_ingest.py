import pandas as pd
import yfinance as yf
import sqlite3

conn = sqlite3.connect('data/financial_data.db')

tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "BRK-B", "META", "JNJ", "V", "XOM", "UNH", "PG", "HD", "MA", "CVX", "PEP", "KO", "LLY", "ABBV"]
tickers_str = " ".join(tickers)

# Historic 5 years data
start_date = "2020-01-01"
end_date = "2025-01-14"

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
})

# Converter 'date' para string no formato YYYY-MM-DD
final_df['date'] = final_df['date'].dt.strftime('%Y-%m-%d')

# Garantir que 'volume' seja do tipo float
final_df['volume'] = final_df['volume'].astype(float)


final_df.to_sql('assets', conn, if_exists='append', index=False)

conn.close()
