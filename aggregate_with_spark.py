def aggregate_with_spark():

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, window
    from pyspark.sql.types import StringType, StructType, StructField, IntegerType, TimestampType

    spark = SparkSession.builder \
        .appName("sales-analytics") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
        .config("spark.jars", "/path/to/mysql-connector-java.jar") \
        .getOrCreate()

    # Kafka 소스에서 스트리밍 데이터프레임을 생성합니다.
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.28.31.155:9092") \
        .option("subscribe", "pension-sales") \
        .option("startingOffsets", "earliest") \
        .load()

    # 정의된 스키마에 따라 JSON 문자열을 파싱합니다.
    schema = StructType([
        StructField("memberEmail", StringType()),
        StructField("roomType", StringType()),
        StructField("roomPrice", IntegerType()),
        StructField("guestNumber", IntegerType()),
        StructField("timestamp", TimestampType())
    ])

    sales_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # 다양한 시간대 기반 윈도우를 사용하여 데이터 집계
    sales_hourly = sales_df.groupBy(window(col("timestamp"), "1 hour"), col("roomType")).sum("roomPrice")
    sales_daily = sales_df.groupBy(window(col("timestamp"), "1 day"), col("roomType")).sum("roomPrice")
    sales_weekly = sales_df.groupBy(window(col("timestamp"), "1 week"), col("roomType")).sum("roomPrice")

    # MySQL 데이터베이스에 저장하는 쿼리를 정의합니다.
    def write_to_mysql(df, epoch_id):
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost/db_reservation") \
            .option("dbtable", "sales_summary") \
            .option("user", "root") \
            .option("password", "password") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append") \
            .save()
        # .option("url", "jdbc:mysql://mysql-server/db_reservation") \
    # .option("driver", "com.mysql.jdbc.Driver") \

    # 각 윈도우 기반 집계에 대해 쓰기 작업을 설정합니다.
    # 쿼리는 비동기적으로 실행됩니다.
    query_hourly = sales_hourly.writeStream.foreachBatch(write_to_mysql).outputMode("append").start()
    query_daily = sales_daily.writeStream.foreachBatch(write_to_mysql).outputMode("append").start()
    query_weekly = sales_weekly.writeStream.foreachBatch(write_to_mysql).outputMode("append").start()
    # print("query_hourly" + query_hourly, "query_daily" + query_daily, "query_weekly" + query_weekly)
    # 작업이 바로 반환되어 Airflow로 제어가 돌아오도록 합니다.
    return {"query_hourly" : query_hourly.id, "query_daily_id": query_daily.id, "query_weekly_id": query_weekly.id}
