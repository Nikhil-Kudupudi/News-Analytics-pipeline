from pyspark.sql.types import *

source=StructType([
    StructField("id",StringType()),
    StructField("name",StringType()),
    StructField("description",StringType()),
    StructField("url",StringType()),
    StructField("category",StringType()),
    StructField("language",StringType()),
    StructField("country",StringType())
])

TopSources=StructType([
    StructField("status",StringType()),
    StructField("sources",ArrayType(source),True)
])