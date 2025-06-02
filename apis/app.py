from fastapi import FastAPI
from apis.routes import router
app=FastAPI(
    title="News Sentiment API",
    description="Fetches and analyzes news in real-time using NewsAPI",
    version="1.0.0"
)


app.include_router(router=router)