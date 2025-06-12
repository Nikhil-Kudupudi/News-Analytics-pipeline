from prefect_flows import flow , task
from pipelines.tasks.fetch_news import fetchNewsApiData
from pipelines.tasks.publish_to_kafka import publishToKafka


@task(retries=1)
def fetchNewsTask():
    data=fetchNewsApiData()
    return data

@task(retries=1)
def publishToKafkaTask(data,topic):
    publishToKafka(data,topic)


@flow(name="NewsIngstionPipeline",log_prints=True)
def newsIngestion():
    data=fetchNewsTask()
    publishToKafkaTask(data,"news-api")

if __name__=="__main__":
    newsIngestion()