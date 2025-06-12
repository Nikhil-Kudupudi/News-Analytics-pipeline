from airflow.decorators import task, dag
from datetime import datetime
import sys 
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.tasks.fetch_news import fetchNewsApiData
from pipelines.tasks.publish_to_kafka import publishToKafka

@dag(
    dag_id="RawDataingestionPipeline",
    catchup=False,
    start_date=datetime(2024, 1, 1),
    schedule="@daily"
)
def news_fetch_pipeline():
    @task(task_id="fetch-news")
    def fetchNewsTask():
        return fetchNewsApiData()

    @task(task_id="push-to-kafka")
    def publishToKafkaTask(data, topic="news-apis"):
        return publishToKafka(data, topic)

    data = fetchNewsTask()
    publishToKafkaTask(data, "news-apis")

# 👇 This is required!
dag = news_fetch_pipeline()
