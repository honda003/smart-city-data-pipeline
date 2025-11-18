from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
#config is the file where you would put your AWS Credentials (IAM --> Users --> Credntials --> Create credentials for apps outside AWS)
from config import configuration 


def main():
    AWS_ACCESS_KEY = configuration.get("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = configuration.get("AWS_SECRET_KEY")

    spark = (
        SparkSession.builder
        .appName("SmartCityProject")
        # Credentials
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        
        # AWS endpoint and region
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        
        # Path style MUST be disabled for AWS
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        
        # Enable SSL for AWS (MinIO was false, AWS must be true)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")

        # Use the correct committer (safe for Parquet to S3)
        .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        
        .getOrCreate()
    )



    # adjust the log level to minimize the console output on executer
    spark.sparkContext.setLogLevel("WARN")

    
    # schemas
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

    def read_kafka_topic(topic, schema):
        """
        Reads a Kafka topic as a streaming DataFrame and applies a schema.
        """
        return (
            spark.readStream
            .format("kafka")  # Use Spark Structured Streaming Kafka connector
            .option("kafka.bootstrap.servers", "broker:29092")  # Kafka broker address to connect
            .option("subscribe", topic)  # Kafka topic to subscribe to
            .option("startingOffsets", "earliest")  # Start reading from the earliest available message
            .load()  # Load the streaming DataFrame from Kafka
            .selectExpr("CAST(value AS STRING)")  # Convert Kafka 'value' from binary to string
            .select(from_json(col("value"), schema).alias("data"))  # Parse JSON string into structured columns using provided schema
            .select("data.*")  # Flatten the nested structure to get individual columns
            .withWatermark("timestamp", "2 minutes")  # Set watermark on 'timestamp' for handling late data
        )


    def streamWriter(input: DataFrame, checkpointFolder, output):
        """
        Writes a streaming DataFrame to storage in Parquet format with checkpointing.
        """
        return (
            input.writeStream
            .format("parquet")  # Output format; Parquet is columnar and optimized for analytics
            .option("checkpointLocation", checkpointFolder)  # Folder to store streaming checkpoint info for fault tolerance
            .option("path", output)  # Destination folder where the Parquet files will be written
            .outputMode("append")  # Append mode: only new rows are written; required for streaming
            .start()  # Starts the streaming query
        )

    vehicle_df = read_kafka_topic("vehicle_data", vehicleSchema)
    gps_df = read_kafka_topic("gps_data", gpsSchema).alias("gps")
    traffic_df = read_kafka_topic("traffic_data", trafficSchema)
    weather_df = read_kafka_topic("weather_data", weatherSchema)
    emergency_df = read_kafka_topic("emergency_data", emergencySchema)


    # Streaming Mode for production
    query1 = streamWriter(vehicle_df, "s3a://smart-city-project1/checkpoints/vehicle_data", "s3a://smart-city-project1/data/vehicle_data")
    query2 = streamWriter(gps_df, "s3a://smart-city-project1/checkpoints/gps_data", "s3a://smart-city-project1/data/gps_data")
    query3 = streamWriter(traffic_df, "s3a://smart-city-project1/checkpoints/traffic_data", "s3a://smart-city-project1/data/traffic_data")
    query4 = streamWriter(weather_df, "s3a://smart-city-project1/checkpoints/weather_data", "s3a://smart-city-project1/data/weather_data")
    query5 = streamWriter(emergency_df, "s3a://smart-city-project1/checkpoints/emergency_data", "s3a://smart-city-project1/data/emergency_data")
    
    query5.awaitTermination()
    
if __name__ == "__main__":
    main()