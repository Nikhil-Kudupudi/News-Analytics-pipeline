
from spark_Streaming.spark_consumer import run_pipeline



def consumeMessages(topic,appName):
    df=run_pipeline(
        app_name=appName,
        topic=topic
    )
    return df



# def push_getTopheadlines():

# if __name__=="__main__":
#     consumeMessages("news-api-getEverything",)    