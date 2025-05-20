from airflow import DAG
from airflow.decorators import task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import docker

default_args = {
    'start_date': datetime(2025,4,1),
    'retries':1
}

project_dir = os.path.abspath(os.path.dirname(__file__))
src_path = "C:/Users/ezequ/Documents/Estudos/Financial-Data-Pipeline/src" # update with the correct path
data_path = "C:/Users/ezequ/Documents/Estudos/Financial-Data-Pipeline/data" # update with the correct path

with DAG(
    dag_id='model_prediction',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    model_prediction_task =  DockerOperator(
        task_id='daily_model_prediction',
        image='financial-data-pipeline-model_prediction',
        api_version="auto",
        auto_remove= 'success',
        command='python /app/src/model/model_predict.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        environment={
                "DATABASE_PATH": "/app/data/database/financial_data.db",  
                "MODEL_PATH": "/app/src/model/models/model.joblib"  
        },
    mounts=[
        docker.types.Mount(
            source=data_path,  # Caminho absoluto do host
            target='/app/data',
            type='bind'
        ),
        docker.types.Mount(
            source=src_path,  # Caminho absoluto do host
            target='/app/src',
            type='bind'
        )
    ],
    mount_tmp_dir=False  # Desabilita montagem de diretórios temporários
    )


model_prediction_task 
