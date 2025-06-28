#!/usr/bin/env python
from confluent_kafka import Producer 
import json

from kafka.adminclient import createTopic, existingTopics
from utils.config_loader import get_config


PORTNUMBER=get_config("kafka",'bootstrap.servers')
conf= {
    'bootstrap.servers':PORTNUMBER,
    'acks':"all"
}

class NewsProducer:
    def __init__(self):
        self.producer=Producer(conf)

    def acked(self,err,msg):
        if err is not None:
            print(f"Failed to deliver message: {str(msg)}: {str(err)}")
        else:
            print(f"Message produced: {str(msg)}")
    def send_message(self,data,topic="news-apis"):
        try:
            value=json.dumps(data).encode("utf-8")
            topics=existingTopics()
            if topic not in topics:
                createTopic(topic)
            self.producer.produce(topic,value,callback=self.acked)
        
        except BufferError:
            self.producer.poll(1000)
            self.producer.produce(topic,value,callback=self.acked)

    def flush_producer(self):
        try:
            remaining=self.producer.flush(30)
            if remaining > 0:
                print(f"Warning: {remaining} messages were not delivered")
        except Exception as e:
            print(f"error {e}")

if __name__== '__main__':

    producer=NewsProducer()
    data={
        "name": "Nikhil",
        "age":12
    }
    producer.send_message(data=data,topic="news-apis")
    producer.flush_producer() 


    

