from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import logging
from airflow.models import Variable
from pyspark.sql import SparkSession
from airflow.operators.bash import BashOperator
from pyspark.sql.functions import col, count, when, isnull

default_args = {
    'owner': 'kzinovyeva',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

dag = DAG(
    'metrics_upd',
    default_args=default_args,
    description='Создание витрины данных по товарам и заказам',
    schedule_interval='0 2 * * *',  # Ежедневно в 02:00
    catchup=False,
    tags=['team_9'],
)



def validate_tables(**context):
    import logging

    logging.info("Проверка таблиц...")

    spark = (
        SparkSession.builder
        .appName("metrics_upd")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .getOrCreate()
    )

    url = "jdbc:postgresql://postgres:5432/de_db"
    props = {"user": "de_user", "password": "de_password", "driver": "org.postgresql.Driver"}

    for table in ['dwh.users', 'dwh.drivers', 'dwh.stores', 'dwh.items', 'dwh.orders', 'dwh.order_items']:
        try:
            df = spark.read.jdbc(url=url, table=table, properties=props)
            logging.info(f"✓ {table}: {df.count()} строк")
        except Exception as e:
            logging.error(f"✗ {table}: {str(e)}")
            spark.stop()
            raise

    spark.stop()
    return True



validate_tables_task = PythonOperator(
    task_id='validate_tables',
    python_callable=validate_tables,
    provide_context=True,
    dag=dag,
)



create_data_mart_task = SparkSubmitOperator(
    task_id='create_data_mart',
    application='/opt/airflow/dags/pyspark_scripts.py',
    name='data-mart',
    verbose=True,
    conf={
        'spark.master': 'local[*]',
        'spark.executor.memory': '2g',
        'spark.driver.memory': '1g',
    },
    packages='org.postgresql:postgresql:42.7.4',  # Fixed package format
    dag=dag,
)



validate_tables_task >> create_data_mart_task

