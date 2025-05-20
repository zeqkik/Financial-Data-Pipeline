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
    dag_id='model_training',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False
) as dag:
    model_training_task =  DockerOperator(
        task_id='weekly_model_training',
        image='financial-data-pipeline-model-training',
        api_version="auto",
        auto_remove= 'success',
        command='python /app/src/model/model_training.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        environment={
                "DATABASE_PATH": "/app/data/database/financial_data.db",  
                "MODEL_PATH": "/app/src/model/models/model.joblib"  
        },
    mounts=[
        docker.types.Mount(
            source=data_path,  
            target='/app/data',
            type='bind'
        ),
        docker.types.Mount(
            source=src_path,  
            target='/app/src',
            type='bind'
        )
    ],
    mount_tmp_dir=False  
    )


model_training_task 
