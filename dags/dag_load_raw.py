from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import io
import requests
import pandas as pd
import os
import io
import zipfile
import glob
import pandas as pd
from sqlalchemy import create_engine
import requests
from sqlalchemy import create_engine

def load_raw_data(**context):

    import os
    # DATA_DIR = os.getenv("DATA_DIR", "/data")  # Будет /data из Docker
    #
    # DB = os.getenv("POSTGRES_DB", "de_db")
    # USER = os.getenv("POSTGRES_USER", "de_user")
    # PWD = os.getenv("POSTGRES_PASSWORD", "de_password")
    # HOST = os.getenv("POSTGRES_HOST", "postgres")
    # PORT = os.getenv("POSTGRES_PORT", "5432")
    #
    # engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
    #
    # # Используем DATA_DIR для формирования пути
    # parquet_pattern = os.path.join(DATA_DIR, "*.parquet")
    # files = sorted(glob.glob(parquet_pattern))
    # print(DATA_DIR)
    #
    # print(files)
    # if not files:
    #     raise SystemExit("Нет файлов *.parquet в папке data/")
    #
    # print("files:", len(files))
    #
    # for f in files:
    #     df = pd.read_parquet(f)
    #     df.columns = [c.strip() for c in df.columns]
    #
    #     if "item_replaced_id" in df.columns:
    #         df["item_replaced_id"] = pd.to_numeric(df["item_replaced_id"], errors="coerce").astype("Int64")
    #
    #     print("loading", f, "rows", len(df))
    #     df.to_sql(
    #         "raw_data",
    #         engine,
    #         schema="dwh",
    #         if_exists="append",
    #         index=False,
    #         method="multi",
    #         chunksize=5000,
    #     )
    #     break
    #
    # print("done")
    #
    # # Возвращаем статистику
    # total_rows = sum([len(pd.read_parquet(f)) for f in files])
    # context['ti'].xcom_push(key='loaded_files', value=len(files))
    # context['ti'].xcom_push(key='loaded_rows', value=total_rows)
    #
    # return f"Loaded {len(files)} files with {total_rows} total rows"


    # Конфигурация базы данных (остается без изменений)

    # Конфигурация базы данных
    DB = os.getenv("POSTGRES_DB", "de_db")
    USER = os.getenv("POSTGRES_USER", "de_user")
    PWD = os.getenv("POSTGRES_PASSWORD", "de_password")
    HOST = os.getenv("POSTGRES_HOST", "postgres")
    PORT = os.getenv("POSTGRES_PORT", "5432")

    engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")

    # === 1. КОНФИГУРАЦИЯ ПУБЛИЧНОЙ ССЫЛКИ ===
    PUBLIC_URL = os.getenv("YANDEX_PUBLIC_URL", "https://disk.yandex.ru/d/SrJBRwi_hPOYsw")

    # Временные файлы для скачивания и распаковки
    TEMP_ZIP = os.getenv("TEMP_ZIP_PATH", "/tmp/data.zip")
    TEMP_DIR = os.getenv("TEMP_EXTRACT_DIR", "/data")

    if os.path.isdir(TEMP_DIR) and any(os.scandir(TEMP_DIR)):
        print("ДАННЫЕ УЖЕ ЗАГРУЖЕНЫ (data не пустая)")
        return "Данные загружены"
    if not os.path.isfile(TEMP_ZIP):



    # === 2. ПОЛУЧЕНИЕ ПРЯМОЙ ССЫЛКИ НА СКАЧИВАНИЕ ===
        print(f"Получение ссылки для публичного ресурса: {PUBLIC_URL}")

        api_url = "https://cloud-api.yandex.net/v1/disk/public/resources/download"

        try:
            r = requests.get(api_url, params={"public_key": PUBLIC_URL})
            r.raise_for_status()
            download_data = r.json()
            download_url = download_data["href"]
            print(f"Получена прямая ссылка для скачивания")
        except requests.exceptions.RequestException as e:
            raise SystemExit(f"Ошибка при получении ссылки на скачивание: {str(e)}")
        except KeyError:
            raise SystemExit("Не удалось получить ссылку на скачивание из ответа API")

        # === 3. СКАЧИВАНИЕ ZIP-АРХИВА ===
        print(f"Скачивание архива в: {TEMP_ZIP}")

        try:
            with requests.get(download_url, stream=True) as response:
                response.raise_for_status()

                # Проверяем, что это действительно zip-архив
                content_type = response.headers.get('content-type', '')
                if 'zip' not in content_type and 'application/x-zip' not in content_type:
                    print(f"Внимание: Content-Type: {content_type}. Возможно, это не ZIP-архив.")

                total_size = int(response.headers.get('content-length', 0))

                with open(TEMP_ZIP, 'wb') as f:
                    if total_size:
                        print(f"Общий размер: {total_size / (1024 * 1024):.2f} MB")

                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)

                            # Прогресс для больших файлов
                            if total_size and downloaded % (10 * 1024 * 1024) < 1024 * 1024:
                                percent = (downloaded / total_size) * 100
                                print(
                                    f"Прогресс: {percent:.1f}% ({downloaded / (1024 * 1024):.1f}/{total_size / (1024 * 1024):.1f} MB)")

                    print(f"Скачано: {downloaded / (1024 * 1024):.2f} MB")

        except requests.exceptions.RequestException as e:
            raise SystemExit(f"Ошибка при скачивании архива: {str(e)}")

    # === 4. РАСПАКОВКА АРХИВА ===
    print(f"Распаковка в: {TEMP_DIR}")

    try:
        os.makedirs(TEMP_DIR, exist_ok=True)

        # Проверяем, что файл является валидным zip-архивом
        if not zipfile.is_zipfile(TEMP_ZIP):
            raise SystemExit(f"Файл {TEMP_ZIP} не является ZIP-архивом")

        with zipfile.ZipFile(TEMP_ZIP, 'r') as zip_ref:
            # Получаем список файлов в архиве
            file_list = zip_ref.namelist()
            print(f"Найдено файлов в архиве: {len(file_list)}")

            # Фильтруем только parquet файлы
            parquet_files = [f for f in file_list if f.endswith('.parquet')]
            print(f"Parquet файлов в архиве: {len(parquet_files)}")

            if not parquet_files:
                raise SystemExit("Нет файлов *.parquet в архиве")

            # Распаковываем только parquet файлы
            for parquet_file in parquet_files:
                print(f"Распаковка: {parquet_file}")
                zip_ref.extract(parquet_file, TEMP_DIR)

    except zipfile.BadZipFile:
        raise SystemExit(f"Ошибка: файл {TEMP_ZIP} поврежден или не является ZIP-архивом")
    except Exception as e:
        raise SystemExit(f"Ошибка при распаковке архива: {str(e)}")

    # === 5. ОБРАБОТКА PARQUET ФАЙЛОВ ===
    print("Поиск распакованных parquet файлов...")

    # Ищем все parquet файлы в распакованной директории
    parquet_pattern = os.path.join(TEMP_DIR, "**", "*.parquet")
    files = sorted(glob.glob(parquet_pattern, recursive=True))

    print(f"Найдено parquet файлов после распаковки: {len(files)}")

    if not files:
        raise SystemExit("Нет файлов *.parquet после распаковки")

    total_rows = 0

    # === 6. ЗАГРУЗКА В БАЗУ ДАННЫХ ===
    for file_path in files:
        try:
            df = pd.read_parquet(file_path)
            df.columns = [c.strip() for c in df.columns]

            if "item_replaced_id" in df.columns:
                df["item_replaced_id"] = pd.to_numeric(
                    df["item_replaced_id"],
                    errors="coerce"
                ).astype("Int64")

            print(f"Загрузка {os.path.basename(file_path)}, строк: {len(df)}")

            df.to_sql(
                "raw_data",
                engine,
                schema="dwh",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )

            total_rows += len(df)
            break

        except Exception as e:
            print(f"Ошибка при обработке файла {file_path}: {str(e)}")
            continue

    # === 7. ОЧИСТКА ВРЕМЕННЫХ ФАЙЛОВ (опционально) ===
    cleanup = os.getenv("CLEANUP_TEMP_FILES", "true").lower() == "true"
    if cleanup:
        print("Очистка временных файлов...")
        try:
            os.remove(TEMP_ZIP)
            import shutil
            shutil.rmtree(TEMP_DIR)
            print("Временные файлы удалены")
        except Exception as e:
            print(f"Не удалось удалить временные файлы: {str(e)}")

    # === 8. ВОЗВРАТ РЕЗУЛЬТАТОВ ===
    print("Загрузка завершена")

    context['ti'].xcom_push(key='loaded_files', value=len(files))
    context['ti'].xcom_push(key='loaded_rows', value=total_rows)

    return f"Loaded {len(files)} files with {total_rows} total rows from Yandex Disk (public link)"


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

