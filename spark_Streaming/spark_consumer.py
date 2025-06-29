from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json
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
     
    def build_session(self, app_name="SparkS3App"):
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
    def __init__(self, spark: SparkSession, topic: str):
        self.spark = spark
        self.topic = topic
        self.bootstrap_servers = get_config('kafka',"bootstrap.servers")
        
    def read_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .load()

    def parse_stream(self, df):
        return df.selectExpr(
            "CAST(key AS STRING)",
            "CAST(value AS STRING)",
            "topic", "partition", "offset", "timestamp"
        )
    
    def structurise_schema(self,raw_df,schema):
        return raw_df\
               .selectExpr("CAST(value as STRING) as json_str")\
               .select(from_json("json_str",schema)).alias("data")
    


def getConsumer(app_name,topic):
    spark_builder = SparkSessionBuilder()
    spark = spark_builder.build_session(app_name)
    consumer = KafkaStreamConsumer(spark, topic)
    return consumer

def run_pipeline(app_name:str,topic: str):
    bucket_name=get_config("aws","bucket_name")
    consumer=getConsumer(app_name,topic)
    raw_df = consumer.read_stream()
    parsed_df = consumer.parse_stream(raw_df)

    # Optional: print schema for debug
    parsed_df.printSchema()
    foldername=formatName(topic)
    # Define output paths
    s3_base = f"s3a://{bucket_name}/files/"
    output_path = f"{s3_base}/{foldername}"
    checkpoint_path = f"{s3_base}/checkpoint/{foldername}"

    # Create folders locally (safe fallback)
    # os.makedirs("files", exist_ok=True)
    # os.makedirs("files/checkpoint", exist_ok=True)
    
    #aws
    awsutil=AWSUtils()
    
    awsutil.create_folder(foldername=foldername)

    # Write stream to S3
    query = parsed_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="1 second") \
        .start()
    return parsed_df
    # query.awaitTermination()



def load_raw_data_from_s3(app_name,topic: str):
    topicSchema=schemas[
        topic
    ]
    consumer = getConsumer(app_name=app_name,topic=topic)
    raw_df=consumer.read_stream()
    parse_df=raw_df.structurise_schema(raw_df,topicSchema)
    return parse_df

if __name__ == "__main__":
    run_pipeline(app_name="NewsStream",topic="news-apis")
