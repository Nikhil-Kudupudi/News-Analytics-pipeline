

import requests
from fastapi import Request,APIRouter
from apis.controller import getEverything,getTopheadlines,getSources
import json
from kafka.adminclient import createTopic, existingTopics
from kafka.producer import NewsProducer
router=APIRouter()

news_producer=NewsProducer()
@router.post("/v1/getEverything")
async def GetEverything(q:str,request:Request):
    try:
        topic="getEverything"
        existingTopcis=existingTopics()
        if topic not in existingTopcis.keys():
            createTopic([topic])
        news_producer.send_message({"q":q},topic="getEverything")
        response=getEverything(q)
        
        return response
    except Exception as e:
        raise Exception(e)
    
@router.post("/v1/top-headlines")
def getTopHeadlines(request:Request):
    """
      Returns breaking news headlines for countries, categories, and singular publishers.
      This is perfect for use with news tickers or anywhere you want to use live up-to-date news headlines
    """
    try:
        topic="top-headlines"
        existingTopcis=existingTopics()
        if topic not in existingTopcis.keys():
            createTopic([topic])
        news_producer.send_message({},topic="top-headlines")
        response= getTopheadlines()
        return response
    except Exception as e:
        return  Exception(e)

@router.post('/v1/top-headlines/sources')
def getHeadlineSources(request:Request):
    """
     Returns information (including name, description, and category) about the most notable sources available for obtaining top headlines from. 
     This list could be piped directly through to your users when showing them some of the options available. 
     This is a minor endpoint
    """
    try:
        news_producer.send_message({},topic="top-headlines-sources")
        response= getSources()
        return response
    except Exception as e:
        return  Exception(e)

