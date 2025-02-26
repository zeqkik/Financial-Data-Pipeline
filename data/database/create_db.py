import sqlite3 
import os

def create_database():
    '''
    Create SQLite database with the table 'assets'
    '''
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir,"financial_data.db")
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS assets (
                    ticker       TEXT, 
                    date         TEXT,
                    high         REAL,
                    open         REAL,
                    low          REAL,
                    close        REAL,
                    volume       REAL,
                    dividends    REAL, 
                    stock_splits REAL,
                    PRIMARY KEY(ticker, date) 
                    );
               
    ''')
    
    print("assets table created")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_database()