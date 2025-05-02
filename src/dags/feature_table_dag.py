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
#src_path = os.path.abspath(os.path.join(project_dir,'..'))
#data_path = os.path.abspath(os.path.join(project_dir,'..','..','data'))
src_path = "C:/Users/ezequ/Documents/Estudos/Financial-Data-Pipeline/src"
data_path = "C:/Users/ezequ/Documents/Estudos/Financial-Data-Pipeline/data"

with DAG(
    dag_id='data_ingest_feature_engineering',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    data_ingestion_task =  DockerOperator(
        task_id='daily_data_ingestion',
        image='financial-data-pipeline-data-ingestion',
        api_version="auto",
        auto_remove= 'success',
        command='python /app/src/ingest/data_ingest.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        environment={
                "DATABASE_PATH": "/app/data/database/financial_data.db",  
                "LOAD_TYPE": "incremental"  
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

    feature_engineering_task =  DockerOperator(
        task_id='daily_feature_engineering',
        image='financial-data-pipeline-feature-engineering',
        api_version="auto",
        auto_remove= 'success',
        command= "python /app/src/feature_engineering/feature_engineering.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        environment={
                "DATABASE_PATH": "/app/data/database/financial_data.db",
                 "SQL_PATH": "/app/src/feature_engineering"  
        },
        mounts=[
            docker.types.Mount(
                source=data_path,  # Caminho absoluto no sistema de arquivos do host
                target='/app/data',  # Caminho no container
                type='bind'
            ),
            docker.types.Mount(
                source=src_path,  # Caminho absoluto no sistema de arquivos do host
                target='/app/src',  # Caminho no container
                type='bind'
            ),
        ],
        mount_tmp_dir=False  # Desabilita montagem de diretórios temporários
    )

data_ingestion_task >> feature_engineering_task
