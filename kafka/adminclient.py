from confluent_kafka.admin import AdminClient
from utils.config_loader import get_config
from utils.logs import logging
PORT_NUMBER=get_config("kafka","bootstrap.servers")
config={
    'bootstrap.servers': PORT_NUMBER,
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