from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys
import logging
from datetime import datetime
from pyspark.sql.functions import col,rank, trim, avg, current_timestamp, count, when, row_number, asc, desc, isnull, coalesce, lit, countDistinct, year, month, dayofmonth, dayofweek, weekofyear, split, to_date

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def create_spark_session(app_name="E-commerce Data Mart"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .getOrCreate()


def read_data(spark):
    logger = logging.getLogger(__name__)

    logger.info("Чтение таблиц...")

    db_url = "jdbc:postgresql://postgres:5432/de_db"
    properties = {"user": "de_user", "password": "de_password", "driver": "org.postgresql.Driver"}
    orders_df = spark.read.jdbc(url=db_url, table="dwh.orders", properties=properties)

    users_df = spark.read.jdbc(url=db_url, table="dwh.users", properties=properties)
    drivers_df = spark.read.jdbc(url=db_url, table="dwh.drivers", properties=properties)
    stores_df = spark.read.jdbc(url=db_url, table="dwh.stores", properties=properties)
    items_df = spark.read.jdbc(url=db_url, table="dwh.items", properties=properties)
    order_items_df = spark.read.jdbc(url=db_url, table="dwh.order_items", properties=properties)

    logger.info(f"Заказы: {orders_df.count()} записей")
    logger.info(f"Позиции заказов: {order_items_df.count()} записей")

    return   users_df, drivers_df,  stores_df, items_df, orders_df,  order_items_df


def process_data(users_df, drivers_df, stores_df, items_df, orders_df, order_items_df, execution_date):
    logger = logging.getLogger(__name__)

    try:

        spark = orders_df.sparkSession

        logger.info(f"Начало обработки данных для даты: {execution_date}")

        execution_date_obj = to_date(lit(execution_date))

        orders_prepared = orders_df \
            .filter(to_date(col("created_at")) == execution_date_obj) \
            .join(
            stores_df.select("store_id", "store_address"),
            "store_id",
            "left"
        ) \
            .withColumn("year", year(col("created_at"))) \
            .withColumn("month", month(col("created_at"))) \
            .withColumn("day", dayofmonth(col("created_at"))) \
            .withColumn("week", weekofyear(col("created_at"))) \
            .withColumn("order_date", to_date(col("created_at"))) \
            .withColumn(
            "city",
            when(
                col("store_address").isNotNull(),
                trim(split(col("store_address"), ",").getItem(0))
            ).otherwise(lit("Unknown"))
        ) \
            .select(
            "order_id",
            "store_id",
            "store_address",
            "city",
            "year",
            "month",
            "day",
            "week",
            "order_date",
            "created_at",
            "delivered_at",
            "canceled_at",
            "user_id"
        )

        logger.info(f"orders_prepared count: {orders_prepared.count()}")

        order_items_filtered = order_items_df \
            .join(orders_prepared.select("order_id", "user_id"), "order_id", "inner")

        logger.info(f"order_items_filtered count: {order_items_filtered.count()}")

        if order_items_filtered.count() == 0:
            logger.warning(f"Нет позиций заказов для даты {execution_date}")

        enriched_data = order_items_filtered \
            .join(
            items_df.select("item_id", "item_title", "item_category"),
            "item_id",
            "left"
        ) \
            .join(
            orders_prepared.select(
                "order_id", "store_id", "store_address", "city",
                "year", "month", "day", "week", "order_date",
                "created_at", "delivered_at", "canceled_at"
            ),
            "order_id",
            "left"
        )

        logger.info(f"enriched_data count: {enriched_data.count()}")

        required_columns = ["order_id", "item_id", "store_id", "user_id", "item_quantity",
                            "item_price", "item_discount", "item_canceled_quantity"]
        for col_name in required_columns:
            if col_name not in enriched_data.columns:
                logger.error(f"Отсутствует обязательный столбец: {col_name}")
                raise ValueError(f"Отсутствует обязательный столбец: {col_name}")


        enriched_data_fixed = enriched_data \
            .withColumn("item_quantity", col("item_quantity").cast("int")) \
            .withColumn("item_canceled_quantity",
                        when(col("item_canceled_quantity").isNotNull(),
                             col("item_canceled_quantity").cast("int"))
                        .otherwise(lit(0))) \
            .withColumn("item_price", col("item_price").cast("decimal(12,2)")) \
            .withColumn("item_discount",
                        when(col("item_discount").isNotNull(),
                             col("item_discount").cast("decimal(12,2)"))
                        .otherwise(lit(0)))
        group_by_columns = [
            "year",
            "month",
            "day",
            "city",
            "store_id",
            "store_address",
            "item_category",
            "item_id",
            "item_title"
        ]

        daily_metrics = enriched_data_fixed.groupBy(*group_by_columns).agg(

            sum(
                (col("item_quantity") - col("item_canceled_quantity")) *
                (col("item_price") - col("item_discount"))
            ).cast("decimal(12,2)").alias("item_revenue"),

            sum("item_quantity").cast("long").alias("ordered_quantity"),

            sum("item_canceled_quantity").cast("long").alias("canceled_quantity"),

            countDistinct("order_id").cast("long").alias("orders_with_item"),

            countDistinct(col("user_id")).cast("long").alias("unique_customers"),

            sum(when(col("item_canceled_quantity") > 0, 1).otherwise(0))
            .cast("long").alias("orders_with_cancellation"),

            avg(col("item_quantity")).cast("decimal(10,2)").alias("avg_quantity_per_order")
        ).withColumn(
            "net_quantity",
            (col("ordered_quantity") - col("canceled_quantity")).cast("long")
        ).withColumn(
            "cancellation_rate",
            when(
                col("ordered_quantity") > 0,
                round(col("canceled_quantity") / col("ordered_quantity") * 100, 2)
            ).otherwise(lit(0.0)).cast("decimal(5,2)")
        ).withColumn(
            "avg_order_value",
            when(
                col("orders_with_item") > 0,
                round(col("item_revenue") / col("orders_with_item"), 2)
            ).otherwise(lit(0.0)).cast("decimal(12,2)")
        ).withColumn(
            "sales_conversion_rate",
            when(
                col("ordered_quantity") > 0,
                round((col("ordered_quantity") - col("canceled_quantity")) /
                      col("ordered_quantity") * 100, 2)
            ).otherwise(lit(0.0)).cast("decimal(5,2)")
        )

        logger.info(f"daily_metrics count: {daily_metrics.count()}")

        if daily_metrics.count() == 0:
            logger.warning(f"Нет данных для агрегации за {execution_date}")

        window_daily = Window.partitionBy(
            "year", "month", "day", "city", "store_id"
        ).orderBy(col("item_revenue").desc())

        daily_with_rank = daily_metrics.withColumn(
            "daily_rank",
            rank().over(window_daily).cast("int")
        ).withColumn(
            "revenue_share",
            when(
                sum(col("item_revenue")).over(
                    Window.partitionBy("year", "month", "day", "city", "store_id")
                ) > 0,
                round(col("item_revenue") /
                      sum(col("item_revenue")).over(
                          Window.partitionBy("year", "month", "day", "city", "store_id")
                      ) * 100, 2)
            ).otherwise(lit(0.0)).cast("decimal(5,2)")
        )

        result_df = daily_with_rank \
            .withColumn(
            "most_popular_product_daily",
            when(col("daily_rank") == 1, True).otherwise(False).cast("boolean")
        ) \
            .withColumn("load_date", execution_date_obj.cast("date")) \
            .withColumn("created_at", current_timestamp()) \
            .withColumn("data_date", execution_date_obj.cast("date")) \
            .orderBy(
            "year", "month", "day", "city", "store_id", "daily_rank"
        )

        result_df.cache()
        result_count = result_df.count()

        logger.info(f"Создана витрина: {result_count} записей")
        logger.info(f"Уникальных магазинов: {result_df.select('store_id').distinct().count()}")
        logger.info(f"Уникальных товаров: {result_df.select('item_id').distinct().count()}")
        logger.info(f"Уникальных категорий: {result_df.select('item_category').distinct().count()}")

        return result_df

    except Exception as e:
        logger.error(f"Ошибка в функции process_data: {str(e)}", exc_info=True)
        raise


def calculate_popularity_metrics(df, period_col, period_name):
    window_spec_popular = Window.partitionBy(period_col, "city", "store_id") \
        .orderBy(desc(f"total_sold_{period_name}"), desc(f"orders_{period_name}"))

    window_spec_unpopular = Window.partitionBy(period_col, "city", "store_id") \
        .orderBy(asc(f"total_sold_{period_name}"), asc(f"orders_{period_name}"))

    popular_items = df \
        .withColumn("rank_popular", row_number().over(window_spec_popular)) \
        .filter(col("rank_popular") == 1)

    unpopular_items = df \
        .withColumn("rank_unpopular", row_number().over(window_spec_unpopular)) \
        .filter(col("rank_unpopular") == 1)

    return popular_items, unpopular_items


def save_to_postgres(data_mart_df, execution_date):
    """Сохранение витрины данных в PostgreSQL"""
    logger = logging.getLogger(__name__)

    # Параметры подключения к PostgreSQL (замените на свои)
    postgres_url = "jdbc:postgresql://postgres:5432/de_db"
    properties = {
        "user": "de_user",
        "password": "de_password",
        "driver": "org.postgresql.Driver"
    }

    try:
        logger.info("Сохранение витрины данных в PostgreSQL...")

        table_name = "dwh.products"
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!', data_mart_df.show(5))
        # Добавляем дату загрузки
        # data_mart_df = data_mart_df.withColumn(
        #     "load_date",
        #     col("execution_date")
        # ).withColumn(
        #     "created_at",
        #     datetime.now()
        # )

        # Сохраняем в PostgreSQL
        data_mart_df.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", table_name) \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .option("batchsize", 10000) \
            .option("truncate", "true")  # Очистка таблицы перед записью

        logger.info(f"Данные сохранены в таблицу {table_name}")

    except Exception as e:
        logger.error(f"Ошибка при сохранении в PostgreSQL: {str(e)}")
        raise


def main():
    """Основная функция"""
    logger = setup_logging()
    execution_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Дата выполнения: {execution_date}")

    try:

        spark = create_spark_session()

        users_df, drivers_df,  stores_df, items_df, orders_df,  order_items_df = read_data(spark)

        data_mart_df = process_data(users_df, drivers_df,  stores_df, items_df, orders_df,  order_items_df, execution_date)

        save_to_postgres(data_mart_df, execution_date)

        logger.info("Витрина данных успешно создана и сохранена в PostgreSQL!")

        spark.stop()

    except Exception as e:
        logger.error(f"Ошибка при выполнении: {str(e)}")
        raise


if __name__ == "__main__":
    main()