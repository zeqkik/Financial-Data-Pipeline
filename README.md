# Financial Data Pipeline

This project implements an **end-to-end machine learning pipeline** to **predict the probability of price movement (up or down) for 20 selected S&P 500 stocks** over the next **5 business days (1 week)**. The entire pipeline is containerized using **Docker** and **orchestrated by Apache Airflow**, allowing automated **daily and weekly executions**.

## 🚀 Key Features

- **Data Ingestion**  
  Daily ingestion of historical stock data using the [yfinance](https://pypi.org/project/yfinance/) library, stored in a **SQLite** database.

- **Feature Engineering**  
  Automated generation of engineered features to improve model performance.

- **Model Training**  
  Weekly retraining of machine learning models to forecast stock price direction (up or down).

- **Prediction**  
  Daily predictions for the selected 20 S&P 500 stocks, estimating the probability of upward or downward movement over the next 5 business days.

## ⚙️ Architecture Overview

- **Apache Airflow DAGs**
  - `data_ingest_feature_engineering`: Runs **daily** for data ingestion and feature engineering.
  - `model_prediction`: Runs **daily** to generate predictions.
  - `model_training`: Runs **weekly** to retrain the model.

- **Dockerized Services**
  - `data-ingestion`
  - `feature_engineering`
  - `model-training`
  - `model-predict`

## 🗂️ Project Structure

```
financial-data-pipeline/
│
├── data/
│   └── database/
│       ├── create_db.py
│       └── financial_data.db
│
├── docker/
│   ├── Dockerfile
│   └── requirements.txt
│
├── src/
│   ├── dags/
│   │   ├── feature_table_dag.py
│   │   ├── model_predict.py
│   │   └── model_training.py
│   │
│   ├── feature_engineering/
│   ├── ingest/
│   │   ├── __init__.py
│   │   ├── data_ingest.py
│   │   └── data_ingest_nb.ipynb
│   │
│   └── model/
│       ├── models/
│       ├── model_predict.py
│       └── model_training.py
│
├── docker-compose.yaml
└── README.md
```

## 📝 How to Run

1. **Build and Start the Services**
   ```bash
   docker-compose up --build
   ```

2. **Access Airflow UI**
   - Go to `http://localhost:8080`
   - Trigger or monitor the **DAGs**.

3. **Automated Schedule**
   - **Daily**: Data ingestion, feature engineering, and prediction.
   - **Weekly**: Model training.

## 📈 Prediction Objective

Predict the **probability of price increase or decrease** for **20 selected S&P 500 stocks** over the next **5 business days**, helping in short-term market movement analysis.

## 🛠️ Technologies Used

- **Python**, **SQL**
- **yfinance**, **scikit-learn**
- **Apache Airflow**
- **Docker**, **Docker Compose**
- **SQLite Database**
