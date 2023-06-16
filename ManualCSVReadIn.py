# Databricks notebook source
sc = spark.sparkContext
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, LongType, StringType
import pyspark.sql.functions as f

# COMMAND ----------

playersRDD = sc.textFile('FileStore/tables/Master-2.csv').map(lambda l: l.split(","))
#print(playersRDD.collect())
header = playersRDD.first()
print(header)

# COMMAND ----------

playersRDD = playersRDD.filter(lambda row: row != header)
# print(playersRDD.first())
# print(playersRDD.count())
# Some of the entries in the csv file do not contain height information. Please do not include these in the DataFrame. 
playersRDD = playersRDD.filter(lambda row: row[17] != '')
# print(playersRDD.count())

# COMMAND ----------

#  playerID, birthCountry, birthState, and height will be needed
playersRowsRDD = playersRDD.map(lambda l: Row(l[0], l[4], l[5], int(l[17])))
# print(playersRowsRDD.count())

# COMMAND ----------

# Create schema and struct type
playersSchema = StructType( [\
                            StructField('playerID', StringType(), True), \
                            StructField('birthCountry', StringType(), True), \
                            StructField('birthState', StringType(), True), \
                            StructField('height', LongType(), True)
                           ])

# Make the DF
playersDF = spark.createDataFrame(playersRowsRDD, playersSchema) 
playersDF.show()
playersDF.printSchema()

# COMMAND ----------

playersDF.createOrReplaceTempView("players")

# COMMAND ----------

# Find the number of players who were born in the state of Colorado. SQL Version
spark.sql('SELECT COUNT(playerID) AS count FROM players WHERE birthState = "CO"').show()

# COMMAND ----------

# # Find the number of players who were born in the state of Colorado. DF functions
playersDF.filter(playersDF['birthState'] == 'CO').select(f.count('playerID').alias("count")).show()

# COMMAND ----------

# List the average height by birth country of all players, ordered from highest to lowest. SQL version
spark.sql("SELECT birthCountry, AVG(height) AS avgheight FROM players GROUP BY birthCountry ORDER BY avgheight DESC").show(playersDF.count())

# COMMAND ----------

# List the average height by birth country of all players, ordered from highest to lowest. DF functions
playersDF.select(f.col('birthCountry'), f.col('height')).groupBy(playersDF['birthCountry']).agg(f.avg(playersDF['height']).alias('avgheight')).orderBy(f.desc('avgheight')).show(playersDF.count())

