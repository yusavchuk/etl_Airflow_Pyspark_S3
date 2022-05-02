#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from Spark_process import Spark_process


with DAG(
    dag_id='etl_dag',
    start_date=datetime(year=2022, month=5, day=1),
    schedule_interval='@once', 
    catchup=False
) as dag:
    
    # 1task
    task_download_title=BashOperator(
        task_id='download_gz_title',
        bash_command='wget https://datasets.imdbws.com/title.basics.tsv.gz', 
        cwd='/home/yu_savchuk/airflow/dags')
    
    task_download_rating=BashOperator(
        task_id='download_gz_rating',
        bash_command='wget https://datasets.imdbws.com/title.ratings.tsv.gz',
        cwd='/home/yu_savchuk/airflow/dags')
    
    task_unzip_rating=BashOperator(
        task_id='unzip_rating',
        bash_command='gunzip /home/yu_savchuk/airflow/dags/title.ratings.tsv.gz')
    
    
    task_unzip_title=BashOperator(
        task_id='unzip_title',
        bash_command='gunzip /home/yu_savchuk/airflow/dags/title.basics.tsv.gz')
    
    task_transform_files=PythonOperator(
        task_id='transform_files',
        python_callable=Spark_process.transform, 
        op_args=['/home/yu_savchuk/airflow/dags/title.basics.tsv',
                 '/home/yu_savchuk/airflow/dags/title.ratings.tsv'])
        
    task_delete_title=BashOperator(
        task_id='delete_title',
        bash_command='rm /home/yu_savchuk/airflow/dags/title.basics.tsv')
    
    task_delete_rating=BashOperator(
        task_id='delete_rating',
        bash_command='rm /home/yu_savchuk/airflow/dags/title.ratings.tsv')
    
    
    task_download_title >> task_download_rating >> \
        [task_unzip_rating, task_unzip_title] >> task_transform_files >> \
            [task_delete_title, task_delete_rating]
    

    
    
    
    
    
    