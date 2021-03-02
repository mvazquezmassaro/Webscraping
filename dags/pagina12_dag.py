# Para poder importar la funcion del archivo p12_scraping.py debo añadir esa ruta al path ya que se encuentra en otra
#carpeta distinta  a la de los dags.
import sys
sys.path.insert(0, "/home/maxi/trabajo_airflow/airflow_env")
from p12_scraping import run_p12_scraping

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0,0,0,0,0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

#caracteristicas del DAG (nombre,cada cuanto tiene que correr, etc)
dag = DAG(
    'pagina12_dag',
    default_args=default_args,
    description='Etl_pagina12_Vazquez_Massaro',
    schedule_interval=timedelta(hours=12),
)

#Esta tarea va a ser un operador de Python
run_etl = PythonOperator(
    task_id='etl_completo',
    python_callable=run_p12_scraping,
    dag=dag,
)

run_etl
