#!/usr/bin/env python
from confluent_kafka import Consumer
import socket
import time
import json
config={
    'bootstrap.servers': 'localhost:34811',
    'group.id':'kafka-python-getting-started',
     'auto.offset.reset': 'earliest'
}
TIME_OUT=60
class NewsConsumer:
    def __init__(self):
        self.consumer=Consumer(config)
        self.initialTime=time.time()
    def readConsumer(self,topic="news-apis"):
        try:
            self.consumer.subscribe([topic])
        
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    currentTime=time.time()
                    if currentTime-self.initialTime > TIME_OUT:
                        print("Stopping Consumer")
                        break
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting...")
                elif msg.error():
                    print("ERROR: %s".format(msg.error()))
                else:
                    article = json.loads(msg.value().decode('utf-8'))
                    
                    print("Article:", article["title"])
                    # Extract the (optional) key and value, and print.
                    # print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    #     topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.consumer.close()

if __name__ == "__main__":
    consumer=NewsConsumer()
    consumer.readConsumer()

        
        