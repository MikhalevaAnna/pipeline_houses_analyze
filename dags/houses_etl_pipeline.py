from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import csv, os
from dotenv import load_dotenv
from airflow.utils.dates import days_ago
from pyspark.sql import functions as func
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")

connection_to_postgres = None
connection_to_clickhouse = None
cursor = None
file_name = "sample_data/russian_houses.csv"
table_name = "houses_data"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
     'start_date': days_ago(1),
}


dag = DAG(
    'houses_etl_pipeline',
    default_args=default_args,
    description='A simple DAG to interact with PySpark and ClickHouse',
    schedule_interval=None,
)

def create_chart(x, y, x_label, y_label, title, chart_name):
    plt.figure(figsize=(15, 10), dpi=100)
    plt.bar(x, y)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.xticks(rotation=25)
    plt.title(title)
    plt.savefig(chart_name)
    plt.show()

def migration_from_spark_to_clickhouse():
    spark = SparkSession.builder \
        .appName("Airflow_PySpark") \
        .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc-0.3.2-patch8-all.jar") \
        .master("local") \
        .getOrCreate()
    try:
        df = spark.read.csv(file_name, header=True, inferSchema=True, encoding='UTF-16LE', multiLine=True)
    except Exception as error:
        raise Exception(
                f'Ошибка при чтении файла {file_name}! '
                f'Ошибка: {error}!')
    print("Начальная структура датафрейма:")
    df.printSchema()
    df.show(10)
    count_total = df.count()
    print(f"Количество записей в датафрейме всего: {count_total}.")
    df_replace = df.na.replace("—", None)
    df_dropna = df_replace.dropna()
    df_filter_by_year = df_dropna.filter("length(maintenance_year) >= 4 AND length(maintenance_year) <= 6 "
                                    "AND maintenance_year >= 1600 AND maintenance_year <= 2025")
    df_data_clear = df_filter_by_year.distinct()
    count_clear = df_data_clear.count()
    print(f"Количество записей в датафрейме всего без дубляжа и пустых записей: {count_clear}.")
    print("Поле maintenance_year для примера:")
    maintenance_year = (df_data_clear.groupby("maintenance_year").
                        agg(func.count("maintenance_year").alias("count_maintenance_year")))
    (maintenance_year.select("maintenance_year", "count_maintenance_year").filter("length(maintenance_year) > 5")
                     .orderBy(func.col("count_maintenance_year").desc()).show())
    df_modify_square = (df_data_clear.withColumn("modify_square",
                                 func.when(func.col("square").contains(" "),
                                           func.regexp_replace(func.col("square"), " ", ""))
                                 .otherwise(func.col("square")))
                         )
    df_modify_square_filter = df_modify_square.filter("modify_square > 1")
    count_clear_by_square = df_modify_square_filter.count()
    print(f"Количество записей в датафрейме, где площадь больше 1: {count_clear_by_square}.")
    square_count = (df_modify_square_filter.groupby("modify_square").
                    agg(func.count("modify_square").alias("count_square")))
    square_count.select("modify_square", "count_square").orderBy(func.col("count_square").desc()).show()

    df_data = (df_modify_square_filter.withColumn("house_id", func.col("house_id").cast('integer'))
                 .withColumn("latitude", func.col("latitude").cast('float'))
                 .withColumn("longitude", func.col("longitude").cast('float'))
                 .withColumn("maintenance_year", func.col("maintenance_year").cast('integer'))
                 .withColumn("square", func.col("square").cast('string'))
                 .withColumn("population", func.col("population").cast('smallint'))
                 .withColumn("region", func.col("region").cast('string'))
                 .withColumn("locality_name", func.col("locality_name").cast('string'))
                 .withColumn("address", func.col("address").cast('string'))
                 .withColumn("full_address", func.col("full_address").cast('string'))
                 .withColumn("communal_service_id", func.col("communal_service_id").cast('float'))
                 .withColumn("description", func.col("description").cast('string'))
                 .withColumn("modify_square", func.col("modify_square").cast('float'))
                 )
    df_data.printSchema()
    count_total = df_data.count()
    print(f"Количество записей в откорректированном датафрейме всего: {count_total}.")
    df_data.show(5)
    print("Средний год постройки домов:")
    df_data.select(func.avg("maintenance_year").cast('integer').alias("avg_maintenance_year")).show()
    print("Средний медианный год постройки домов:")
    df_data.select(func.median("maintenance_year").cast('integer').alias("median_maintenance_year")).show()
    df_max_regions_with_houses = (df_data.groupBy("region").
                                   agg(func.count("house_id").alias("cnt_houses")))
    df_max10_regions_with_houses = df_max_regions_with_houses.select("region", "cnt_houses").orderBy(
                                   func.col("cnt_houses").desc())
    print("Топ-10 областей с наибольшим количеством объектов:")
    df_max10_regions_with_houses.show(10)
    pandas_df_max10_regions_with_houses = df_max10_regions_with_houses.limit(10).toPandas()

    create_chart(pandas_df_max10_regions_with_houses['region'],
                 pandas_df_max10_regions_with_houses['cnt_houses'],
                 'Наименование субъектов РФ',
                 'Количество зданий',
                 'График топ-10 регионов с наибольшим количеством объектов',
                 'sample_data/Top10_chart_by_region_and_quantity.png'
                 )

    df_max_locality_name_with_houses = (df_data.groupBy("locality_name").
                                        agg(func.count("house_id").alias("cnt_houses")))
    df_max10_locality_name_with_houses = df_max_locality_name_with_houses.select("locality_name", "cnt_houses").orderBy(
                                   func.col("cnt_houses").desc())
    print("Топ-10 городов с наибольшим количеством объектов:")
    df_max10_locality_name_with_houses.show(10)
    pandas_df_max10_locality_name_with_houses = df_max10_locality_name_with_houses.limit(10).toPandas()

    create_chart(pandas_df_max10_locality_name_with_houses['locality_name'],
                 pandas_df_max10_locality_name_with_houses['cnt_houses'],
                 'Наименование городов',
                 'Количество зданий',
                 'График топ-10 городов с наибольшим количеством объектов',
                 'sample_data/Top10_chart_by_cities_and_quantity.png'
                 )
    df_max_full_square_in_regions = df_data.groupBy("region").agg(func.max("modify_square").alias("max_square"))
    df_max_square_in_regions = df_max_full_square_in_regions.select("region", "max_square").orderBy(
                                   func.col("max_square").desc())
    print("Здания с максимальной площадью в рамках каждой области:")
    df_max_square_in_regions.show()
    pandas_df_max_square_in_regions = df_max_square_in_regions.limit(10).toPandas()

    create_chart(pandas_df_max_square_in_regions['region'],
                 pandas_df_max_square_in_regions['max_square'],
                 'Наименование городов',
                 'Максимальная площадь объекта',
                 'График топ10 городов с наибольшей площадью объекта',
                 'sample_data/Top10_Chart_by_regions_and_max_square.png'
                 )
    df_min_full_square_in_regions = df_data.groupBy("region").agg(func.min("modify_square").alias("min_square"))
    df_min_square_in_regions = df_min_full_square_in_regions.select("region", "min_square").orderBy(
                                   func.col("min_square").asc())
    print("Здания с минимальной площадью в рамках каждой области:")
    df_min_square_in_regions.show()
    pandas_df_min_square_in_regions = df_min_square_in_regions.limit(10).toPandas()

    create_chart(pandas_df_min_square_in_regions['region'],
                 pandas_df_min_square_in_regions['min_square'],
                 'Наименование городов',
                 'Минимальная площадь объекта',
                 'График топ10 городов с наименьшей площадью объекта',
                 'sample_data/Top10_chart_by_regions_and_min_square.png'
                 )
    df_data = df_data.withColumn("group_year", func.floor((func.col('maintenance_year')-1000)/10))
    print("Датафрейм с новым полем 'group_year':")
    df_data.show()
    df_data_group_year = (df_data.groupBy("group_year")
                   .agg(
                       func.count("house_id").alias("count_houses"),
                       func.min("maintenance_year").alias("min_maintenance_year"),
                       func.max("maintenance_year").alias("max_maintenance_year")
                   ))
    df_count_decades = df_data_group_year.select("min_maintenance_year",
                                                         "max_maintenance_year",
                                                         func.concat(func.col("min_maintenance_year"), func.lit("-"),
                                                                     func.col("max_maintenance_year")).alias("period"),
                                                         "count_houses"
                                                         ).orderBy(func.col("count_houses").desc())
    print("Определим количество зданий по десятилетиям:")
    df_count_decades.show()
    pandas_df_count_decades = df_count_decades.limit(20).toPandas()

    create_chart(pandas_df_count_decades['period'],
                 pandas_df_count_decades['count_houses'],
                 'Диапазон годов',
                 'Количество домов',
                 'График топ20 количества домов по диапазонам годов',
                 'sample_data/Top20_chart_by_counts_houses_by_periods.png'
                 )

    try:
        df_data.write \
        .format("jdbc") \
        .option("url", f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/default") \
        .option("dbtable", table_name) \
        .option("user", "default") \
        .option("password", "") \
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY modify_square") \
        .mode("overwrite") \
        .save()
    except Exception as error:
        raise Exception(
                f'Загрузить данные в таблицу {table_name} Clickhouse не удалось! '
                f'Ошибка: {error}!')

    try:
        query = f"SELECT full_address, square \
                            FROM default.{table_name} \
                            WHERE \
                                modify_square > 60.0 \
                            ORDER BY modify_square DESC"

        data_frame = spark.read.format('jdbc') \
        .option('url', f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/default") \
        .option("user", "default") \
        .option("password", "") \
        .option("dbtable", f'({query}) AS subquery') \
        .load()
        print(f"Выводим топ 25 домов из таблицы {table_name} в ClickHouse, у которых площадь больше 60 кв.м:")
        data_frame.show(25)
    except Exception as error:
        raise Exception(
                f'Выполнить запрос к таблице {table_name} на выгрузку данных из Clickhouse не удалось! '
                f'Ошибка: {error}!')

    spark.stop()


task_migration_from_spark_to_clickhouse = PythonOperator(
    task_id='migration_from_spark_to_clickhouse',
    python_callable=migration_from_spark_to_clickhouse,
    dag=dag,
)


task_migration_from_spark_to_clickhouse

