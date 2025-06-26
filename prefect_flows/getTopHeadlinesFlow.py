from prefect import flow , task

from pipelines.tasks.fetch_news import fetchTopHeadlinesApi
from pipelines.tasks.publish_to_kafka import publishToKafka

from utils.config_loader import get_config
GET_HEADLINES_TOPIC=get_config("topics","getTopHeadlines")


@task(retries=1)
def fetchTopHeadlinesTask():
    data=fetchTopHeadlinesApi()
    return data

@task(retries=1)
def publishToKafkaTask(data,topic):
    publishToKafka(data,topic)


@flow(name="GetTopHeadlines",log_prints=True)
def getTopHeadlinesFLow():
    data=fetchTopHeadlinesTask()
    publishToKafkaTask(data,GET_HEADLINES_TOPIC)



if __name__=="__main__":
    getTopHeadlinesFLow()