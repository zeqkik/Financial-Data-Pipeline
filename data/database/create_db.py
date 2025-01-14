import sqlite3 as sqlite

def create_database():
    '''
    Create SQLite database with the table 'assets'
    '''

    conn = sqlite.connect('data/financial_data.db')
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY,
                    ticker       TEXT, 
                    date         TEXT,
                    high         REAL,
                    low          REAL,
                    close        REAL,
                    volume       REAL,
                    dividends    REAL 
                    );
               
    ''')
    
    print("assets table created")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_database()