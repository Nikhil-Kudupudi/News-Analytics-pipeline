from newsapi import NewsApiClient
import configparser
from datetime import datetime

#Load config
config=configparser.ConfigParser()
config.read('config.conf')

# create news api connector
API_KEY=config['news_api']['api_key']

news=NewsApiClient(api_key=API_KEY)


# /v2/everything
def getEverything(q):
    """
    search every article published by over 150,000 diffrent sources
    """
    try:
        from_date=datetime(2025,10,20)
        to_date=datetime.now()
        response=news.get_everything(q)
        return response
    except Exception as e:
        print(e)
