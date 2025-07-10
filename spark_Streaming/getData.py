from snowflake.snowflake_connector import executeInsertQuery, executeQuery
from spark_Streaming.spark_consumer import SparkSessionBuilder
from spark_Streaming.schemas import schemas
from pyspark.sql.functions import col,explode
from utils.config_loader import get_config
from utils.utils import formatName
GET_EVERYTHING_TOPIC=get_config("topics","getEverything")
TOP_HEADLINES=get_config("topics","getTopHeadlines")
TOP_SOURCES=get_config("topics","getTopSources")
def mapGetEverything(df,apiType="getEverything"):
    try:
        print(apiType)
        # results_df=df.select("totalResults","status")
        df.write.mode("overwrite").json("temp")
        articles_df=df.withColumn("results_articles",explode("articles"))
        articles_df=articles_df.select("results_articles")
        print(articles_df.printSchema())
        for article in articles_df.select("results_articles").collect():
            item=article["results_articles"]
            author=item["author"]
            title=item["title"]
            description=item["description"]
            url=item["url"]
            urlToImage=item["urlToImage"]
            publishedAt=item["publishedAt"]
            content=item["content"]
            apiType=apiType
            source_id=item["source"]["id"]

            source_name=item["source"]["name"]
            record=(author,title,description,url,urlToImage,publishedAt,content,apiType,source_id,source_name)
            query=f"""
               INSERT INTO ARTICLES (
    author, title, description, url, urlToImage,
    publishedAt, content, apiType, source_id, source_name
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
            result=executeInsertQuery(query,record) 
    except Exception as e:
        raise Exception(e)    

def mapTopSources(df):
    try:
        topSources=df.withColumn("topSources",explode("sources"))
        records=[]
        for topsource in topSources.select("topSources").collect():
            source=topsource["topSources"]
            sourceId=source["id"] or None
            category=source["category"]
            name=source["name"]
            description=source["description"]
            url=source["url"]
            category=source["category"]
            language=source["language"]
            country=source["country"]
            record = (
                    sourceId or '',
                    name or '',
                    description or '',
                    url or '',
                    category or '',
                    language or '',
                    country or ''
                )
            records.append(record)

        query = f"""
            INSERT INTO TOPSOURCES (
            sourceId, name, description, url, category, language, country
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

        result=executeInsertQuery(query,records) 
    except Exception as e:
        raise Exception(e)



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
        # if topic == GET_EVERYTHING_TOPIC:
        #     mapGetEverything(df)
        # elif topic == TOP_HEADLINES:
        #     mapGetEverything(df,"getTopHeadlines")
        if topic == TOP_SOURCES:
            mapTopSources(df)

if __name__=="__main__":
    getDataframes()