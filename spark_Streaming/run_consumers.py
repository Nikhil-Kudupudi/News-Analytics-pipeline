
from spark_Streaming.spark_consumer import run_pipeline



def push_getEverything():
    df=run_pipeline(
        app_name="News-APi1",
        topic="getEverythingApi"
    )

    return df


# def push_getTopheadlines():

if __name__=="__main__":
    push_getEverything()    