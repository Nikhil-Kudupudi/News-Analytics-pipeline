

import requests
from fastapi import Request,APIRouter
from apis.controller import getEverything
import json
router=APIRouter()

@router.post("/getEverything")
async def GetEverything(q:str,request:Request):
    try:
        response=getEverything(q)
        
        return response
    except Exception as e:
        raise Exception(e)