from spark_Streaming.spark_consumer import SparkSessionBuilder
from spark_Streaming.schemas import schemas
from pyspark.sql.functions import col,explode
from utils.config_loader import get_config
from utils.utils import formatName
GET_EVERYTHING=get_config("topics","getEverything")
def mapGetEverything(df):
    results=df
    results_df=df.select("totalResults","status")
    df.write.mode("overwrite").json("temp")

    articles_df=df.withColumn("results_articles",explode("articles"))
    articles_df=articles_df.select("results_articles")
    sources_df=articles_df.withColumn("source_articles_id",col("results_articles.source.id")).withColumn("sources_articles_name",col("results_articles.source.name"))
    sources_df=sources_df.select("source_articles_id","sources_articles_name")
    print(results_df.show())
    print(articles_df.show())
    print(sources_df.show())
    



def getDataframes():
    spark=SparkSessionBuilder()
    sparksession=spark.build_session()

    bucket_name=get_config("aws","bucket_name")
    #read data 
    for topic in schemas:
        
        foldername = formatName(str(topic))
        
        s3_base = f"s3a://{bucket_name}/files/"
        output_path = f"{s3_base}/{foldername}"
        df=sparksession.read.json(output_path)
        if topic == GET_EVERYTHING:
            mapGetEverything(df)

if __name__=="__main__":
    getDataframes()