import streamlit as st
import pandas as pd
import sqlite3
import os
import altair as alt

# Caminho do banco de dados
DATABASE_PATH = os.getenv("DATABASE_PATH", "data/database/financial_data.db")

# Conectar ao banco de dados
@st.cache_data
def load_predictions():
    conn = sqlite3.connect(DATABASE_PATH)
    df = pd.read_sql("SELECT * FROM model_predictions", conn)
    conn.close()
    df['date'] = pd.to_datetime(df['date'])  # Garantir que a coluna seja datetime
    return df

# Carregar os dados
df = load_predictions()

st.title("📈 Model Predictions Panel - S&P")

# Sidebar para filtros
st.sidebar.header("Filters")
min_proba = st.sidebar.slider("Min Uptrend Probabilty:", 0.0, 1.0, 0.56, step=0.01)

# Aplicar filtros
filtered_df = df[df['predicted_proba'] >= min_proba]
sorted_df = filtered_df.sort_values(by="predicted_proba", ascending=False)

# Painel em blocos
st.subheader(f"Uptrend Probabilty: >{min_proba*100:.2f}% ({len(sorted_df)} stocks)")

cols_per_row = 4
for i in range(0, len(sorted_df), cols_per_row):
    row = sorted_df.iloc[i:i+cols_per_row]
    cols = st.columns(len(row))
    for col, (_, stock) in zip(cols, row.iterrows()):
        color = "#2ECC71" if stock['predicted_target'] == 1 else "#E74C3C"
        col.markdown(f"""
            <div style='background-color:{color};padding:20px;border-radius:10px;text-align:center;color:white'>
                <h4>{stock['ticker']}</h4>
                <p>Uptrend Prob: {stock['predicted_proba']:.2%}</p>
                <p>{"Up" if stock['predicted_target'] == 1 else "Down"}</p>
            </div>
        """, unsafe_allow_html=True)