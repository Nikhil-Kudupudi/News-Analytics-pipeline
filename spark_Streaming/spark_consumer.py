from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json,current_timestamp,expr,to_json,struct
import os
from spark_Streaming.schemas import schemas
from utils.config_loader import get_config
from aws_files.awsUtils import AWSUtils
from utils.utils import formatName

class SparkSessionBuilder:
    def __init__(self):
        self.aws_access_key = get_config("aws", "aws_access_key_id")
        self.aws_secret_key = get_config("aws", "aws_secret_access_key")
        self.region = get_config("aws", "region")
     
    def build_session(self, app_name="NewsStream"):
        return SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
                    "org.apache.hadoop:hadoop-aws:3.4.0,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.782") \
            .config("spark.hadoop.fs.s3a.access.key", self.aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.aws_secret_key) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.endpoint.region", self.region) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.parquet.mergeSchema", "false") \
            .config("spark.hadoop.parquet.enable.summary-metadata", "false") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.hive.metastorePartitionPruning", "true") \
            .getOrCreate()


class KafkaStreamConsumer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bootstrap_servers = get_config('kafka',"bootstrap.servers")
        
    def read_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribePattern", "news-.*") \
            .option("startingOffsets", "earliest") \
            .load()

    def parse_stream(self, df):
        
        return df.selectExpr("topic", "CAST(value AS STRING)","timestamp")\
        









if __name__ == "__main__":
    try:
        sparksession=SparkSessionBuilder()
        spark=sparksession.build_session()
        consumer=KafkaStreamConsumer(spark)
        df=consumer.read_stream()
        parsed_df=consumer.parse_stream(df)
        
        query=parsed_df\
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .start()
        awsutil=AWSUtils()
        bucket_name=get_config("aws","bucket_name")
        #filter schemas based on topic and m,ap and print
        for topic, schema in schemas.items():
            topic_df=parsed_df.filter(col("topic")==topic)
            udf=topic_df \
                .withColumn("parsed", from_json(col("value"), schema)) \
                .select("parsed.*")
            

                # #  S3 output paths
            foldername = formatName(topic)
            s3_base = f"s3a://{bucket_name}/files/"
            output_path = f"{s3_base}/{foldername}"
            checkpoint_path = f"s3a://{bucket_name}/checkpoint/{foldername}"
            awsutil.create_folder(foldername)
            save_df=udf.writeStream.format("parquet")\
                    .option("path",output_path)\
                    .option("checkpointLocation",checkpoint_path)\
                    .trigger(processingTime="1 second")\
                    .start()

        query.awaitTermination()
    except Exception as e:
        query.stop()

