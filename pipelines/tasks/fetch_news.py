from pipelines.utils.logs import logging
from newsapi import NewsApiClient
import configparser

from utils.config_loader import get_config

# from kafka.adminclient import createTopic, existingTopics

# from kafka.producer import NewsProducer
#Load config
config=configparser.ConfigParser()
config.read('config.conf')



# create news api connector
API_KEY=get_config('news_api','api_key')

news=NewsApiClient(api_key=API_KEY)
# -------------------------------------------------APIS----------------------------------------------------------
# /v1/everything
def getEverything(q):
    """
    search every article published by over 150,000 diffrent sources
    """
    try:
        logging.info("/everything api is called")
        response=news.get_everything(q)
        if not response:
            logging.warning("No response found")
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
    
# -------------------------------------------------APIS END----------------------------------------------------------\

# -------------------------------------------------FECTH TASK --------------------------------------------------------

def fetchEverythingAPIData():
    try:
        response=getEverything(q="data,finance,tech")
        return response
    except Exception as e:
        logging.error("Error while fetching apis data")
        raise Exception(e)
    
def fetchTopHeadlinesApi():
    try:
        response=getTopheadlines()
        return response 
    except Exception as e:
        logging.error("Error while fetching  Top Headlines")
        raise Exception(e)
def fetchTopSourcesApi():
    try:
        response=getSources()
        return response 
    except Exception as e:
        logging.error("Error while fetching top sources")
        raise Exception(e)