# c:\Users\joperez\spark-3.0.0-preview-bin-hadoop2.7\bin\spark-submit redis_populator.py
# /mnt/c/Users/joperez/spark-3.0.0-preview-bin-hadoop2.7/bin/spark-submit redis_populator.py

import pandas as pd
import redis

r = redis.Redis(host='localhost', port=6379, db=0)
r2 = redis.Redis(host='localhost', port=6379, db=0)

states = pd.read_csv("state_codes.csv", header=0)

for state in states.values:
	print('Caching ' + state[0] + ' -> ' + state[1])
	r.hset("state_codes", state[0], state[1])
	

for state in states.values:
	print('Retriving ' + state[0] + ' -> ' + r.hget("state_codes", state[0]))
	

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("redis_data_enrichment2").getOrCreate()

states = spark.read.csv("state_codes.csv", header=True, inferSchema=True)
states.write.format("org.apache.spark.sql.redis").option("table", "states").option("key.column", "State").save()