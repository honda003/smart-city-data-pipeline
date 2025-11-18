#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


# In[2]:


spark = SparkSession.builder \
    .appName("SmartCityProject") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()


# In[3]:


# vehicle schema
vehicleSchema = StructType([
    StructField("id", StringType(), True),
    StructField("deviceId", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("speed", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("make", StringType(), True),
    StructField("model", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("fuelType", StringType(), True),
])

gpsSchema = StructType([
    StructField("id", StringType(), True),
    StructField("deviceId", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("speed", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("vehicleType", StringType(), True),

])

trafficSchema = StructType([
StructField("id", StringType(), True),
StructField("deviceId", StringType(), True),
StructField("cameraId", StringType(), True),
StructField("location", StringType(), True),
StructField("timestamp", TimestampType(), True),
StructField("vehicleType", StringType(), True),

])


weatherSchema = StructType([
StructField("id", StringType(), True),
StructField("deviceId", StringType(), True),
StructField("location", StringType(), True),
StructField("timestamp", TimestampType(), True),
StructField("temperature", StringType(), True),
StructField("weatherCondition", StringType(), True),
StructField("precipitation", DoubleType(), True),
StructField("windSpeed", DoubleType(), True),
StructField("humidity", IntegerType(), True),
StructField("airQualityIndex", DoubleType(), True),

])


emergencySchema = StructType([
StructField("id", StringType(), True),
StructField("deviceId", StringType(), True),
StructField("incidentId", StringType(), True),
StructField("type", StringType(), True),
StructField("location", StringType(), True),
StructField("timestamp", TimestampType(), True),
StructField("description", StringType(), True),
StructField("status", StringType(), True),

])


# In[4]:


vehicle_df = spark.readStream.format("kafka") \
.option("kafka.bootstrap.servers", "broker:29092") \
.option("subscribe", "vehicle_data") \
.option("startingOffsets", "earliest") \
.load() \
.selectExpr("CAST(value AS STRING)") \
.select(from_json(col("value"), vehicleSchema).alias("data")) \
.select("data.*") \
.withWatermark("timestamp", "2 minutes")


# In[ ]:


vehicle_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3a://smart-city-project1/checkpoints/vehicle_data") \
    .option("path", "s3a://smart-city-project1/data/vehicle_data") \
    .outputMode("append") \
    .start()


# In[ ]:




