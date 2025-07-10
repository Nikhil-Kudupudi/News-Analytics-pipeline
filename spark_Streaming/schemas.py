from utils.config_loader import get_config
from spark_Streaming.models.getEverythingSchema import Everything
from spark_Streaming.models.getSources import TopSources
from spark_Streaming.models.getTopHeadlines import TopHeadlines

GET_EVERYTHING_TOPIC=get_config("topics","getEverything")
TOP_HEADLINES=get_config("topics","getTopHeadlines")
TOP_SOURCES=get_config("topics","getTopSources")

schemas={
    GET_EVERYTHING_TOPIC:Everything,
    TOP_HEADLINES:TopHeadlines,
    TOP_SOURCES:TopSources
}