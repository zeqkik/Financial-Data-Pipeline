import pandas as pd
import sqlite3
import joblib 
import os
from xgboost import XGBClassifier

#Data import

DATABASE_PATH = os.getenv("DATABASE_PATH", "data/database/financial_data.db")
MODEL_PATH = os.getenv("MODEL_PATH", "src/model/models/model.joblib")

conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()
df = pd.read_sql("SELECT * FROM feature_table;", conn)
df['date'] = pd.to_datetime(df['date'])

#Feature Engineering
df['weekly_return_future'] = df.groupby('ticker')['close'].pct_change(periods=5).shift(-5)
df['target_weekly'] = (df['weekly_return_future'] > 0.001).astype(int)
df.fillna(0, inplace=True)


# Train Test Split

df_train = df[df['date']< '2025-01-01'].copy()
df_test = df[df['date']>= '2025-01-01'].copy()


# Getting feature and target datasets

X_train = df_train.drop(columns=['ticker','date', 'dividends', 'stock_splits', 'weekly_return_future', 'target_weekly'])
X_test = df_test.drop(columns=['ticker','date', 'dividends', 'stock_splits', 'weekly_return_future', 'target_weekly'])


y_train = df_train['target_weekly']
y_test = df_test['target_weekly']

# Model creation and predictions

xgb_model = XGBClassifier(n_estimators=30, max_depth=3, eval_metric='mlogloss', random_state=42, tree_method='hist')
xgb_model.fit(X_train, y_train)

joblib.dump(xgb_model, MODEL_PATH)
print("Model trained and updated")

conn.commit()
conn.close()