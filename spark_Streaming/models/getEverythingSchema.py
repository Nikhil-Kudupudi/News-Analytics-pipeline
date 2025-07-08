from pyspark.sql.types import *

# Source schema
sources = StructType([
    StructField("id", StringType(), True),  
    StructField("name", StringType(), True)
])

# Articles schema
articles = StructType([
    StructField("source", sources, True), 
    StructField("author", StringType(), True),  
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("urlToImage", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("content", StringType(), True)
])

# Main schema
Everything = StructType([
    StructField("status", StringType(), True),
    StructField("totalResults", IntegerType(), True),  
    StructField("articles", ArrayType(articles), True)
])