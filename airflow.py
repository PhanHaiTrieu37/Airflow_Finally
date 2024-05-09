from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from data_clean import clean_data
from send_mail import email

import subprocess
from datetime import datetime, timedelta
# import json
# import pandas as pd
# import smtplib
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from email.mime.application import MIMEApplication

default_args = {
    'owner': 'phanhaitrieu',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crawl_STVL_dags',
    default_args=default_args,
    description='A DAGS to run the crawling website SieuThiViecLam.com',
    schedule_interval='@daily',
    start_date=datetime(2024,5,7),
    catchup=False,
)

crawl_command = """
cd /opt/airflow/crawldata/crawlSTVL/crawlSTVL && scrapy crawl STVL
"""

crawl_data = BashOperator(
    task_id='crawl_data_STVL',
    bash_command=crawl_command,
    dag=dag,
)

clean_data_dags = PythonOperator(
    task_id='clean_data_STVL',
    python_callable=clean_data,
    dag=dag,
)

send_email_dags = PythonOperator(
    task_id='send_email_stock_data',
    python_callable=email,
    dag=dag,
)

crawl_data >> clean_data_dags >> send_email_dags