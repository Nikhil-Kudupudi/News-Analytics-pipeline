from kafka.adminclient import createTopic,existingTopics
from kafka.producer import NewsProducer
from utils.config_loader import get_config
config={
    'bootstrap.servers': get_config('kafka','bootstrap.servers'),
    'group.id':get_config('kafka','group.id'),
     'auto.offset.reset': get_config('kafka','auto.offset.reset')
}

def publishToKafka(data:list, topic="news-apis"):
    try:
        producer= NewsProducer()    
        for record in data:
            producer.send_message(record,topic)   
        producer.flush_producer() 
    except Exception as e:
        raise Exception(e)