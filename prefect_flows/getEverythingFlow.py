from prefect import flow , task

from pipelines.tasks.fetch_news import fetchEverythingAPIData
from pipelines.tasks.publish_to_kafka import publishToKafka

from utils.config_loader import get_config
GET_EVERYTHING_TOPIC=get_config("topics","getEverything")
@task(retries=1)
def fetchEverythingTask():
    data=fetchEverythingAPIData()
    return data


@task(retries=1)
def publishToKafkaTask(data,topic):
    publishToKafka(data,topic)


@flow(name="GetEverything",log_prints=True)
def getEverythingFlow():
    data=fetchEverythingTask()
    publishToKafkaTask(data,GET_EVERYTHING_TOPIC)



if __name__=="__main__":
    getEverythingFlow()