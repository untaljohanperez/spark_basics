# c:\Users\joperez\spark-3.0.0-preview-bin-hadoop2.7\bin\spark-submit --jars spark-redis-2.4.0-jar-with-dependencies.jar spark_query_executor.py ./query.sql ./result.csv
# /mnt/c/Users/joperez/spark-3.0.0-preview-bin-hadoop2.7/bin/spark-submit --jars spark-redis-2.4.0-jar-with-dependencies.jar spark_query_executor.py  ./query.sql ./result1.csv

from pyspark.sql import SparkSession

import sys
import pandas as pd
from pyspark.sql.functions import udf, broadcast
from functools import reduce

spark = SparkSession.builder.appName("QueryExecutor").getOrCreate()
sc = spark.sparkContext

queryPath = sys.argv[1] #"./query.sql"
outputFile = sys.argv[2] #"./result.csv"

df = spark.read.csv("./database.csv", header=True)
columnsRenamed = [ (c, c.replace(" ", "")) for c in df.columns]
df = reduce(lambda df, c: df.withColumnRenamed(c[0], c[1]), columnsRenamed, df)

#def getStateCode(state):
#	import redis
#	r = redis.Redis(host='localhost', port=6379, db=0)
#	r.hget("state_codes", state)
#	
#getStateCodeUDF = udf(getStateCode)
#df = df.withColumn("State", getStateCodeUDF(df.State))

# Data enrichment
states = spark.read.format("org.apache.spark.sql.redis").option("table", "states").option("key.column", "State").load()

df = df.join(broadcast(states), df.State == states.State, "left_outer").drop("State").withColumnRenamed("Abbreviation", "State")

df.createOrReplaceTempView("homicides")

file = open(queryPath, "r")
query = file.read()
file.close()
query = query.replace("\n", " ")

#spark.sql(query).toPandas().to_csv(path_or_buf=outputFile)
spark.sql(query).repartition(1).write.csv(outputFile, mode="overwrite", header=True)


spark.stop()