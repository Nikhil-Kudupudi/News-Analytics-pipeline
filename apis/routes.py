

import requests
from fastapi import Request,APIRouter
from apis.controller import getEverything
import json
router=APIRouter()

@router.post("/v1/getEverything")
async def GetEverything(q:str,request:Request):
    try:
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
        pass
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
        pass
    except Exception as e:
        return  Exception(e)

