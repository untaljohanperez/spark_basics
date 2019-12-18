#c:\Users\joperez\spark-3.0.0-preview-bin-hadoop2.7\bin\spark-submit homicide_job.py

from pyspark.sql import SparkSession

from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col, udf
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

import sys

spark = SparkSession.builder.appName("Homicide Report").getOrCreate()
sc = spark.sparkContext
outputDir = "./output"

# https://www.kaggle.com/murderaccountability/homicide-reports/data
df = spark.read.csv("./database.csv", header=True)

from functools import reduce
columnsRenamed = [ (c, c.replace(" ", "")) for c in df.columns]
df = reduce(lambda df, c: df.withColumnRenamed(c[0], c[1]), columnsRenamed, df)

df.printSchema()

### 1. Which are the 5 states with higher number of unsolved crimes
unsolvedCrimesPerState = df.where(df.CrimeSolved == 'No').groupBy("State").count().sort(F.desc("count")).limit(5)
unsolvedCrimesPerState.toPandas().to_csv(path_or_buf=f"{outputDir}/1_unsolvedCrimesPerState.csv")

### 2. Which has been the most violent years of the 21st century
columnToNumber = udf(lambda x: int(x), IntegerType())
df = df.withColumn("Year", columnToNumber("Year"))
mostViolentYear21thCentury = df.where(df.Year >= 2000).groupBy("Year").count().withColumnRenamed("count", "Count").sort(F.desc("Count")).limit(5)
mostViolentYear21thCentury.toPandas().to_csv(path_or_buf=f"{outputDir}/2_mostViolentYear21thCentury.csv")

### 3. Number of crimes per gender
crimesPerGender = df.groupBy("VictimSex").count().withColumnRenamed("count", "Count").sort(F.desc("Count"))
crimesPerGender.toPandas().to_csv(path_or_buf=f"{outputDir}/3_crimesPerGender.csv")

### 4. Which was the most violent month of each year
yearMonth = df.select(df.Year, df.Month).groupBy(df.Year, df.Month).count().withColumnRenamed("count", "Count").sort(df.Year)
agg = yearMonth.groupBy(yearMonth.Year).agg(F.max(yearMonth.Count)).withColumnRenamed("max(Count)", "Crimes")
mostViolentMonthPerYear = agg.join(yearMonth, [agg.Crimes == yearMonth.Count, agg.Year == yearMonth.Year], 'inner').select(agg.Year, yearMonth.Month, agg.Crimes)
mostViolentMonthPerYear.toPandas().to_csv(path_or_buf=f"{outputDir}/4_mostViolentMonthPerYear.csv")

### 5. Number of crimes against minors and percentage over total crimes per year
df = df.withColumn("VictimAge", columnToNumber(df.VictimAge))
crimeMinors = df.where(df.VictimAge <= 18).groupBy(df.Year).agg(F.count(F.lit(1)).alias("CrimesMinors"))
totalCrimes = df.groupBy(df.Year).agg(F.count(F.lit(1)).alias("TotalCrimes"))
crimesAgainstMinors = crimeMinors.join(totalCrimes, crimeMinors.Year == totalCrimes.Year, 'inner') \
    .select(crimeMinors.Year, crimeMinors.CrimesMinors, totalCrimes.TotalCrimes, \
    ((crimeMinors.CrimesMinors * 100) / totalCrimes.TotalCrimes).alias("PercentageOverTotal")) \
    .sort(F.desc("Year"))
crimesAgainstMinors.toPandas().to_csv(path_or_buf=f"{outputDir}/5_crimesAgainstMinors.csv")

### 6. State with most number of murders with explosives
stateExplosives = df.where(df.Weapon == "Explosives").groupBy(df.State).count().sort(F.desc("count")).limit(5)
stateExplosives.toPandas().to_csv(path_or_buf=f"{outputDir}/6_stateExplosives.csv")

### 7. Which States are Best at Solving Murders?
stateSolvedCrimes = df.where(df.CrimeSolved == 'Yes').groupBy("State").count().sort(F.desc("count")).limit(5)
stateSolvedCrimes.toPandas().to_csv(path_or_buf=f"{outputDir}/7_stateSolvedCrimes.csv")

### 8. Does Victim Race Affect Whether a Murder is Solved? 🤔
unsolvedByRace = df.where(df.CrimeSolved == 'No').groupBy("victimRace").count().sort(F.desc("count")).limit(5)
unsolvedByRace.toPandas().to_csv(path_or_buf=f"{outputDir}/8_unsolvedByRace.csv")

### 9. Can We Predict the Age of a Killer?
def ageRange(age):
    if(age > 0 and age <= 10):
        return "0-10"
    elif(age > 10 and age <= 20):
        return "11-20"
    elif(age > 20 and age <= 30):
        return "21-30"
    elif(age > 30 and age <= 40):
        return "31-40"
    elif(age > 40 and age <= 50):
        return "41-50"   
    elif(age > 50 and age <= 60):
        return "51-60"
    elif(age > 60 and age <= 70):
        return "61-70"
    elif(age > 70):
        return "71-99"
    else:
        return "Unknown"
           
df = df.withColumn("PerpetratorAge", columnToNumber(df.VictimAge))
ageRangeUDF = udf(ageRange)
crimesByPerpetratorAgeRange = df.withColumn("PerpetratorAgeRange", ageRangeUDF(df.PerpetratorAge)).groupBy("PerpetratorAgeRange").agg(count(F.lit(1)).alias("Crimes")).sort(col("Crimes").desc())
crimesByPerpetratorAgeRange.toPandas().to_csv(path_or_buf=f"{outputDir}/9_crimesByPerpetratorAgeRange.csv")

### 10. What about the races of the most violent age range... 🤔
mostViolentRaceByAgeRange = df.withColumn("PerpetratorAgeRange", ageRangeUDF(df.PerpetratorAge)).where(col("PerpetratorAgeRange") == "21-30").groupBy(df.PerpetratorRace).count().sort(F.desc("count"))
mostViolentRaceByAgeRange.toPandas().to_csv(path_or_buf=f"{outputDir}/10_mostViolentRaceByAgeRange.csv")