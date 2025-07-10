from fastapi import FastAPI
from apis.routes import router
import uvicorn
app=FastAPI(
    title="News Sentiment API",
    description="Fetches and analyzes news in real-time using NewsAPI",
    version="1.0.0",
    docs_url="/docs"
)


app.include_router(router=router)



