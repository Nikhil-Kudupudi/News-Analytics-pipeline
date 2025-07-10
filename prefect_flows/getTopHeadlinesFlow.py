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

# @task(retries=1)
# def consumeTopHeadlines(topic):
#     df=consumeandParseMessages(topic,appName="topHeadlines")
#     return df
# @task(retries=1)
# def parseData(topic):
#     parse_data(topic,appName='topHeadlines')

@flow(name="GetTopHeadlines",log_prints=True)
def getTopHeadlinesFLow():
    data=fetchTopHeadlinesTask()
    publishToKafkaTask(data,GET_HEADLINES_TOPIC)
    # df=consumeTopHeadlines(GET_HEADLINES_TOPIC)
    # parseData(GET_HEADLINES_TOPIC)
    return "sucess"


if __name__=="__main__":
    getTopHeadlinesFLow()