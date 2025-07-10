from prefect import flow , task

from pipelines.tasks.fetch_news import fetchTopSourcesApi
from pipelines.tasks.publish_to_kafka import publishToKafka



from utils.config_loader import get_config
GET_SOURCES_TOPIC=get_config("topics","getTopSources")


@task(retries=1)
def fetchTopSourcesTask():
    data=fetchTopSourcesApi()
    return data
@task(retries=1)
def publishToKafkaTask(data,topic):
    publishToKafka(data,topic)


# @task(retries=1)
# def consumeTopSources(topic):
#     df=consumeandParseMessages(topic,appName="topSources")
#     return df

# @task(retries=1)
# def parseData(topic):
#     parse_data(topic,appName='topSources')

@flow(name="GetTopSources",log_prints=True)
def getSourcesFLow():
    data=fetchTopSourcesTask()
    publishToKafkaTask(data,GET_SOURCES_TOPIC)
    # consumeTopSources(topic=GET_SOURCES_TOPIC)
    # parseData(GET_SOURCES_TOPIC)


if __name__=="__main__":
    getSourcesFLow()