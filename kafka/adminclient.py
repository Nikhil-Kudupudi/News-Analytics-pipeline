from confluent_kafka.admin import AdminClient
from apis.logs import logging
config={
    'bootstrap.servers': 'localhost:34811',
}
client=AdminClient(config)
def existingTopics():
    try:
      metadata=client.list_topics()
      return  metadata.topics or []
    except Exception as e:
        return Exception(e)
def createTopic(topics):
    try:
        logging.info(f"{topics} are being created")
        client.create_topics(topics)
        logging.info("topics are created")
    except Exception as e:
        logging.error(f"Error while creating topics {topics}")
        return Exception(e)