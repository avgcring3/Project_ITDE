from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import glob
import pandas as pd
from sqlalchemy import create_engine

def load_raw_data(**context):

    import os
    DATA_DIR = os.getenv("DATA_DIR", "/data")  # Будет /data из Docker

    DB = os.getenv("POSTGRES_DB", "de_db")
    USER = os.getenv("POSTGRES_USER", "de_user")
    PWD = os.getenv("POSTGRES_PASSWORD", "de_password")
    HOST = os.getenv("POSTGRES_HOST", "postgres")
    PORT = os.getenv("POSTGRES_PORT", "5432")

    engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")

    # Используем DATA_DIR для формирования пути
    parquet_pattern = os.path.join(DATA_DIR, "*.parquet")
    files = sorted(glob.glob(parquet_pattern))
    print(DATA_DIR)

    print(files)
    if not files:
        raise SystemExit("Нет файлов *.parquet в папке data/")

    print("files:", len(files))

    for f in files:
        df = pd.read_parquet(f)
        df.columns = [c.strip() for c in df.columns]

        if "item_replaced_id" in df.columns:
            df["item_replaced_id"] = pd.to_numeric(df["item_replaced_id"], errors="coerce").astype("Int64")

        print("loading", f, "rows", len(df))
        df.to_sql(
            "raw_data",
            engine,
            schema="dwh",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
        break

    print("done")

    # Возвращаем статистику
    total_rows = sum([len(pd.read_parquet(f)) for f in files])
    context['ti'].xcom_push(key='loaded_files', value=len(files))
    context['ti'].xcom_push(key='loaded_rows', value=total_rows)

    return f"Loaded {len(files)} files with {total_rows} total rows"



default_args = {
    'owner': 'kzinovyeva',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=10),
}

dag = DAG(
    'load_data',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['team_9']
)

SCHEMA_NAME = 'dwh'
POSTGRES_CONN_ID = 'de_postgres'



load_raw_data_task = PythonOperator(
    task_id='load_raw_data_from_parquet',
    python_callable=load_raw_data,
    dag=dag,
    provide_context=True
)

create_users = PostgresOperator(
    task_id='create_users',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    INSERT INTO {SCHEMA_NAME}.users (user_id, user_phone)
    SELECT DISTINCT user_id, user_phone
    FROM {SCHEMA_NAME}.raw_data
    WHERE user_id IS NOT NULL
    ON CONFLICT (user_id) DO NOTHING;
    """,
    dag=dag
)

create_drivers = PostgresOperator(
    task_id='create_drivers',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    INSERT INTO {SCHEMA_NAME}.drivers (driver_id, driver_phone)
    SELECT DISTINCT driver_id, driver_phone
    FROM {SCHEMA_NAME}.raw_data
    WHERE driver_id IS NOT NULL
    ON CONFLICT (driver_id) DO NOTHING;
    """,
    dag=dag
)

create_stores = PostgresOperator(
    task_id='create_stores',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    INSERT INTO {SCHEMA_NAME}.stores (store_id, store_address)
    SELECT DISTINCT store_id, store_address
    FROM {SCHEMA_NAME}.raw_data
    WHERE store_id IS NOT NULL
    ON CONFLICT (store_id) DO NOTHING;
    """,
    dag=dag
)

create_items = PostgresOperator(
    task_id='create_items',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    -- Основные товары
    INSERT INTO {SCHEMA_NAME}.items (item_id, item_title, item_category)
    SELECT DISTINCT item_id, item_title, item_category
    FROM {SCHEMA_NAME}.raw_data
    WHERE item_id IS NOT NULL
    ON CONFLICT (item_id) DO NOTHING;

    -- Замененные товары
    INSERT INTO {SCHEMA_NAME}.items (item_id, item_title, item_category)
    SELECT DISTINCT item_replaced_id, 'Unknown Replacement Item', 'Unknown'
    FROM {SCHEMA_NAME}.raw_data
    WHERE item_replaced_id IS NOT NULL
    ON CONFLICT (item_id) DO NOTHING;
    """,
    dag=dag
)

create_orders = PostgresOperator(
    task_id='create_orders',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    INSERT INTO {SCHEMA_NAME}.orders (
        order_id, user_id, store_id, driver_id, address_text,
        created_at, paid_at, delivery_started_at, delivered_at, canceled_at,
        payment_type, delivery_cost, order_discount, order_cancellation_reason
    )
    SELECT DISTINCT
        order_id, user_id, store_id, driver_id, address_text,
        created_at, paid_at, delivery_started_at, delivered_at, canceled_at,
        payment_type, delivery_cost, order_discount, order_cancellation_reason
    FROM {SCHEMA_NAME}.raw_data
    WHERE order_id IS NOT NULL
    ON CONFLICT (order_id) DO NOTHING;
    """,
    dag=dag
)

create_order_items = PostgresOperator(
    task_id='create_order_items',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    INSERT INTO {SCHEMA_NAME}.order_items (
        order_id, item_id, item_quantity, item_price,
        item_discount, item_canceled_quantity, item_replaced_id
    )
    SELECT DISTINCT
        order_id, item_id, item_quantity, item_price,
        item_discount, item_canceled_quantity, item_replaced_id
    FROM {SCHEMA_NAME}.raw_data
    WHERE order_id IS NOT NULL AND item_id IS NOT NULL
    ON CONFLICT (order_id, item_id) DO NOTHING;
    """,
    dag=dag
)



load_raw_data_task >> [create_users, create_drivers, create_stores, create_items]

[create_users, create_drivers, create_stores, create_items] >> create_orders

create_orders >> create_order_items

