from prefect import flow
from prefect_flows.getEverythingFlow import getEverythingFlow
from prefect_flows.getSourcesFlow import getSourcesFLow
from prefect_flows.getTopHeadlinesFlow import getTopHeadlinesFLow
@flow(timeout_seconds=5000,name="AllFlows",log_prints=True)
def newsIngestion():
    getEverythingFlow()
    getSourcesFLow()
    getTopHeadlinesFLow()


if __name__=="__main__":
    newsIngestion.serve(
        name="News-Ingestion",
        tags=["news-api"]
        )