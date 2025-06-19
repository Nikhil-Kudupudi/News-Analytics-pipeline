from pyspark.sql import SparkSession
import os
def consumeData(topic):
    spark = SparkSession.builder \
        .appName("NewsStream") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:41349") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # decode key & value from bytes → strings
    from pyspark.sql.functions import col, expr

    parsed = kafka_df.selectExpr(
        "CAST(key AS STRING)",
        "CAST(value AS STRING)",
        "topic", "partition", "offset", "timestamp"
    )

    os.makedirs("files",exist_ok=True)
    path= os.path.join()
    os.makedirs("files/checkpoint",exist_ok=True)
    # write to console (micro­batch every 1s)
    query = parsed.writeStream \
        .format("parquet") \
        .option("path","./files")\
        .option("checkpointLocation", "./files/checkpoint") \
        .trigger(processingTime="1 second") \
        .start()

    query.awaitTermination()

if __name__ == '__main__':
    consumeData(topic="news-apis")