#c:\Users\joperez\spark-3.0.0-preview-bin-hadoop2.7\bin\spark-submit spark_query_executor.py ./query.sql ./result.csv

from pyspark.sql import SparkSession

import sys
import pandas as pd
from functools import reduce

spark = SparkSession.builder.appName("QueryExecutor").getOrCreate()
sc = spark.sparkContext

queryPath = sys.argv[1] #"./query.sql"
outputFile = sys.argv[2] #"./result.csv"

df = spark.read.csv("./database.csv", header=True)
columnsRenamed = [ (c, c.replace(" ", "")) for c in df.columns]
df = reduce(lambda df, c: df.withColumnRenamed(c[0], c[1]), columnsRenamed, df)
df.createOrReplaceTempView("homicides")

file = open(queryPath, "r")
query = file.read()
file.close()
query = query.replace("\n", " ")

spark.sql(query).toPandas().to_csv(path_or_buf=outputFile)

spark.stop()