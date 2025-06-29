from pyspark.sql.types import *


sources=StructType([
    StructField("id",StringType()),
    StructField("name",StringType())
])

articles=StructType([
    StructField("source",sources,False),
    StructField("author",StringType()),
    StructField("title",StringType()),
    StructField("description",StringType()),
    StructField("url", StringType(), True),
    StructField("urlToImage", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("content", StringType(), True)
])


TopHeadlines=StructType([
    StructField("status",StringType()),
    StructField("totalResults",StringType()),
    StructField("articles",ArrayType(articles))
])