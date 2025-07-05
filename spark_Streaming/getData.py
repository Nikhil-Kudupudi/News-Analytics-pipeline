from spark_Streaming.spark_consumer import SparkSessionBuilder
from spark_Streaming.schemas import schemas
from utils.config_loader import get_config
from utils.utils import formatName


def getDataframes():
    spark=SparkSessionBuilder()
    sparksession=spark.build_session()

    bucket_name=get_config("aws","bucket_name")
    #read data 
    for topic in schemas:
        
        foldername = formatName(str(topic))
        
        s3_base = f"s3a://{bucket_name}/files/"
        output_path = f"{s3_base}/{foldername}"
        df=sparksession.read.parquet(output_path)
        df.show()

if __name__=="__main__":
    getDataframes()