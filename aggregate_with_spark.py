from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.functions import sum as sum_
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, TimestampType
import os


# def aggregate_with_spark():
# def set_mysql_jdbc():
def aggregate_with_spark():
    # 실행 환경에 따라 JDBC 드라이버 경로를 설정합니다.
    jdbc_driver_path = "C:/Users/thphy/mysql-connector-j-8.2.0/mysql-connector-java-8.2.0.jar"
    if os.path.exists(jdbc_driver_path):
        # Windows 환경에서는 주어진 경로를 사용합니다.
        spark_jars = jdbc_driver_path
    else:
        # 비Windows 환경에서는 Maven 저장소에서 자동으로 드라이버를 다운로드하도록 설정합니다.
        spark_jars = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,mysql:mysql-connector-java:8.0.26"

    spark = SparkSession.builder \
        .appName("sales-analytics") \
        .config("spark.jars.packages", spark_jars) \
        .getOrCreate()

    # 로그 수준을 DEBUG 또는 INFO로 설정 (필요에 따라 조정)
    spark.sparkContext.setLogLevel("DEBUG")

    # return spark_jars

    # .config("spark.jars", "C:/Users/thphy/mysql-connector-j-8.2.0/mysql-connector-java-8.2.0.jar") \
    # .config("spark.jars", "/path/to/mysql-connector-java.jar") \
# async def create_streaming_dataframe() :
    # Kafka 소스에서 스트리밍 데이터프레임을 생성합니다.
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.28.31.155:9092") \
        .option("subscribe", "pension-sales") \
        .option("startingOffsets", "earliest") \
        .load()
    # # .option("kafka.bootstrap.servers", "172.28.31.155:9092") \
    #
    # # 정의된 스키마에 따라 JSON 문자열을 파싱합니다.
    schema = StructType([
        StructField("memberEmail", StringType()),
        StructField("roomType", StringType()),
        StructField("roomPrice", IntegerType()),
        StructField("guestNumber", IntegerType()),
        StructField("timestamp", TimestampType())
    ])

    sales_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    #
    # watermark를 추가합니다.
    sales_df_with_watermark = sales_df.withWatermark("timestamp", "1 minutes")  # 1분의 지연을 허용

    # 다양한 시간대 기반 윈도우를 사용하여 데이터 집계
    sales_hourly = (sales_df_with_watermark
                    .groupBy(window(col("timestamp"), "1 hour"), col("roomType"))
                    .agg(sum_("roomPrice").alias("total_sales"))
                    .select(col("window.start").alias("window_start"),
                            col("window.end").alias("window_end"),
                            col("roomType"),
                            col("total_sales")))
    # Adjusted sales_daily DataFrame
    sales_daily = (sales_df_with_watermark
                   .groupBy(window(col("timestamp"), "1 day"), col("roomType"))
                   .agg(sum_("roomPrice").alias("total_sales"))
                   .select(col("window.start").alias("window_start"),
                           col("window.end").alias("window_end"),
                           col("roomType"),
                           col("total_sales")))

    # Adjusted sales_weekly DataFrame
    sales_weekly = (sales_df_with_watermark
                    .groupBy(window(col("timestamp"), "1 week"), col("roomType"))
                    .agg(sum_("roomPrice").alias("total_sales"))
                    .select(col("window.start").alias("window_start"),
                            col("window.end").alias("window_end"),
                            col("roomType"),
                            col("total_sales")))
    #
    # # MySQL 데이터베이스에 저장하는 쿼리를 정의.
    def write_to_mysql(table_name):
        def to_db(df, epoch_id):
            df.write \
                .format("jdbc") \
                .option("url", "jdbc:mysql://127.0.0.1:3306/db_reservation") \
                .option("dbtable", table_name) \
                .option("user", "user_reservation") \
                .option("password", "1234") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("append") \
                .save()

        return to_db

    # # 각 윈도우 기반 집계에 대해 쓰기 작업을 설정합니다.
    query_hourly = sales_hourly.writeStream.foreachBatch(write_to_mysql("sales_hourly")).outputMode("append").start()
    query_daily = sales_daily.writeStream.foreachBatch(write_to_mysql("sales_daily")).outputMode("append").start()
    query_weekly = sales_weekly.writeStream.foreachBatch(write_to_mysql("sales_weekly")).outputMode("append").start()

    return {"query_hourly": query_hourly.id, "query_daily_id": query_daily.id, "query_weekly_id": query_weekly.id}
