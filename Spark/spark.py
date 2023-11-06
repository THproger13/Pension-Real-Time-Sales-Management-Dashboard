from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window

spark = SparkSession.builder.appName("sales-analytics").getOrCreate()

# Kafka 소스에서 스트리밍 데이터프레임을 생성합니다.
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-server:9092") \
    .option("subscribe", "pension-sales") \
    .load()

# 데이터를 처리합니다. 여기서는 간단한 창(window) 기반 집계를 예로 듭니다.
sales = df.selectExpr("CAST(value AS STRING)") \
    .groupBy(window(col("timestamp"), "1 day"), col("roomType")) \
    .sum("roomPrice")

# MySQL 데이터베이스에 저장하는 쿼리를 정의합니다.
def write_to_mysql(df, epoch_id):
    df.write.format("jdbc") \
        .option("url", "jdbc:mysql://mysql-server/db_name") \
        .option("dbtable", "sales_summary") \
        .option("user", "root") \
        .option("password", "password") \
        .mode("append") \
        .save()

query = sales.writeStream.foreachBatch(write_to_mysql).start()
query.awaitTermination()
