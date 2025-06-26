from utils.logs import logging
from newsapi import NewsApiClient
import configparser
from kafka.adminclient import createTopic, existingTopics
import json
from kafka.producer import NewsProducer
from utils.config_loader import get_config
#Load config
config=configparser.ConfigParser()
config.read('config.conf')



# create news api connector
API_KEY=get_config('news_api','api_key')

news=NewsApiClient(api_key=API_KEY)
config={
    'bootstrap.servers': 'localhost:33009',
    'group.id':'kafka-python-getting-started',
     'auto.offset.reset': 'earliest'
}
# /v1/everything
def getEverything(q):
    """
    search every article published by over 150,000 diffrent sources
    """
    try:

        topic="news-apis"
        logging.info("/everything api is called")
        response=news.get_everything(q)
        if not response:
            logging.warning("No response found")
        # existingTopcis=existingTopics()
        # # if topic not in existingTopcis.keys():
            # createTopic([topic])
        # news_producer=NewsProducer()
        articles=response.get('articles',[])
        # print(articles)
        # for article in articles:
        #     news_producer.send_message(article,topic=topic)
        #     news_producer.flush_producer()
        with open("getEverything.json",'w') as file:
            json.dump(response,file)
        return response
    except Exception as e:
        logging.error("An error occured at /everything api",e)
        print(e)

#/v1/top-headlines
def getTopheadlines():
    try:
        response=news.get_top_headlines()
        logging.info("/top-headlines api is called")
        with open("getTopHeadlines.json",'w') as file:
            json.dump(response,file)
        return response
    except Exception as e:
        logging.error("An error occured at /top-headlines api")
        return Exception(e)
    
#/v1/top-headlines/sources 
def getSources():
    try:
        response=news.get_sources()
        logging.info("/top-headlines/sources api is called")
        with open("getSources.json",'w') as file:
            json.dump(response,file)
        return response
    except Exception as e:
        logging.error("An error occured at /top-headlines/sources api")
        return Exception(e)
    
