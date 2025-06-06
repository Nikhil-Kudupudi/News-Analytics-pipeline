#!/usr/bin/env python
from confluent_kafka import Consumer
import socket
config={
    'bootstrap.servers': 'localhost:37841',
    'group.id':'kafka-python-getting-started',
     'auto.offset.reset': 'earliest'
}

class NewsConsumer:
    def __init__(self):
        self.consumer=Consumer(config)
        self.topic="news-apis"
    def readConsumer(self):
        try:
            self.consumer.subscribe([self.topic])
        
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting...")
                elif msg.error():
                    print("ERROR: %s".format(msg.error()))
                else:
                    print(msg.topic(),msg.value())
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

        
        