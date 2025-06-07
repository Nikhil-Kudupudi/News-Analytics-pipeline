from apis.logs import logging
from newsapi import NewsApiClient
import configparser
from datetime import datetime
from kafka.adminclient import createTopic, existingTopics
from kafka.consumer import NewsConsumer

#Load config
config=configparser.ConfigParser()
config.read('config.conf')



# create news api connector
API_KEY=config['news_api']['api_key']

news=NewsApiClient(api_key=API_KEY)

news_consumer=NewsConsumer()
config={
    'bootstrap.servers': 'localhost:34811',
    'group.id':'kafka-python-getting-started',
     'auto.offset.reset': 'earliest'
}
# /v1/everything
def getEverything(q):
    """
    search every article published by over 150,000 diffrent sources
    """
    try:
        from_date=datetime(2025,10,20)
        to_date=datetime.now()
        topic="getEverything"
        data=news_consumer.readConsumer(topic=topic)
        logging.info("/everything api is called")
        response=news.get_everything(q)
        return response
    except Exception as e:
        logging.error("An error occured at /everything api",e)
        print(e)

#/v1/top-headlines
def getTopheadlines():
    try:
        response=news.get_top_headlines()
        logging.info("/top-headlines api is called")
        return response
    except Exception as e:
        logging.error("An error occured at /top-headlines api")
        return Exception(e)
    
#/v1/top-headlines/sources 
def getSources():
    try:
        response=news.get_sources()
        logging.info("/top-headlines/sources api is called")
        return response
    except Exception as e:
        logging.error("An error occured at /top-headlines/sources api")
        return Exception(e)
    
