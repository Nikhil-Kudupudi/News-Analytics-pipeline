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
            #kafkajson  data should be send in bytes 
            value=json.dumps(data).encode('utf-8') 
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
  "status": "ok",
  "totalResults": 10177,
  "articles": [
    {
      "source": {
        "id": "null",
        "name": "Gizmodo.com"
      },
      "author": "Luc Olinga",
      "title": "Elon Musk Deflects Bill Gates’ Foreign Aid Alarm With Personal Swipe",
      "description": "After Gates warned that U.S. aid cuts would cost millions of lives, Musk’s response was a personal jab fueled by an old grudge over a Tesla stock bet.",
      "url": "https://gizmodo.com/elon-musk-deflects-bill-gates-foreign-aid-alarm-with-personal-swipe-2000624554",
      "urlToImage": "https://gizmodo.com/app/uploads/2023/09/d671148892956d273b6e43ae841e1d74.jpg",
      "publishedAt": "2025-07-06T16:55:32Z",
      "content": "The tech worlds most consequential feud has flared up again. Elon Musk and Bill Gates, two men who have shaped the modern world, are locked in a bitter public dispute, this time over foreign aid cuts… [+4274 chars]"
    }
  ]
}
    producer.send_message(data=data,topic="news-apis")
    producer.flush_producer() 


    

