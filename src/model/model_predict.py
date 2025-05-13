import pandas as pd
import joblib
import sqlite3
import os 

#DATABASE_PATH = "data\database\financial_data.db"
DATABASE_PATH = os.getenv("DATABASE_PATH", "data/database/financial_data.db")
MODEL_PATH = os.getenv("MODEL_PATH", "src/model/models/model.joblib")

conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# Imports
df = pd.read_sql("SELECT * FROM feature_table WHERE date = (SELECT MAX(date) FROM feature_table);", conn)
xgb_model = joblib.load(MODEL_PATH)

# Set input(X) to the right format  
X = df.drop(columns=['ticker','date', 'dividends', 'stock_splits'])

y_pred_xgb = xgb_model.predict(X)

y_proba = xgb_model.predict_proba(X)[:, 1]
threshold = 0.56
y_pred_threshold = (y_proba > threshold).astype(int)

results_df = df[['ticker', 'date']].copy()
results_df['predicted_target'] = y_pred_threshold
results_df['predicted_proba'] = y_proba

# Save predictions to database
results_df.to_sql('model_predictions', conn, if_exists='replace', index=False)

print(f"Predictions updated to 'model_predictions'")

conn.close()