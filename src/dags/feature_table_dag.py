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
        image='data-ingestion',
        api_version="auto",
        auto_remove= 'success',
        command='python /app/src/ingest/data_ingest.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        #volumes=[f"{data_path}:/app/data",f"{src_path}:/app/src"],
        #volumes=[
        #"/opt/airflow/project/data:/app/data",
        #"/opt/airflow/project/src:/app/src"
        #],
        mounts=[
            # Mount the data directory
            docker.types.Mount(
                source='/opt/airflow/project/data',
                target='/app/data',
                type='bind'
            ),
            # Mount the src directory
            docker.types.Mount(
                source='/opt/airflow/project/src',
                target='/app/src',
                type='bind'
            )
        ],
        environment={
                "DATABASE_PATH": "/app/data/database/financial_data.db",  
                "LOAD_TYPE": "incremental"  
        }
    )

    feature_engineering_task =  DockerOperator(
        task_id='data_ingest_feature_engineering',
        image='feature-engineering',
        api_version="auto",
        auto_remove= 'success',
        command= "python /app/src/feature_engineering/feature_engineering.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        #volumes=[f"{data_path}:/app/data",f"{src_path}:/app/src"],
        #volumes=[
        #"/opt/airflow/project/data:/app/data",
        #"/opt/airflow/project/src:/app/src"
        #],
        mounts=[
                    # Mount the data directory
                    docker.types.Mount(
                        source='/opt/airflow/project/data',
                        target='/app/data',
                        type='bind'
                    ),
                    # Mount the src directory
                    docker.types.Mount(
                        source='/opt/airflow/project/src',
                        target='/app/src',
                        type='bind'
                    )
                ],
        environment={
                "DATABASE_PATH": "/app/data/database/financial_data.db",
                 "SQL_PATH": "/app/src/feature_engineering"  
        }    
    )

data_ingestion_task >> feature_engineering_task
