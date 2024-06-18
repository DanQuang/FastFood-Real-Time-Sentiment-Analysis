from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, IntegerType, DateType
from pyspark.ml import PipelineModel
from pymongo import MongoClient
import logging
import os
import re
from datetime import datetime, date

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# Config logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server = 'kafka:9092'
topic_name = 'test_topic'
mongo_host = 'host.docker.internal'

spark_conn = SparkSession.builder \
    .appName("KafkaStreamWithMLPredictions") \
    .getOrCreate()

# Load pre-trained Logistic Regression model
lr_model_path = "/app/consumer/lr_model"
lr_model = PipelineModel.load(lr_model_path)

# Define schema for data
schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Entity", StringType(), True),
    StructField("Content", StringType(), True),
    StructField("Cleaned Content", StringType(), True)
])

# Reading data from kafka
df = None
try:
    df = spark_conn.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", server) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()
    df = df.selectExpr("CAST(value AS STRING)")
    df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")
except Exception as e:
    logger.error(f"Cannot connect to Kafka: {e}")
    exit(1)

# UDF to convert DenseVector to list and calculate max probability
def dense_vector_to_list(vector):
    probabilities = vector.toArray().tolist()
    max_probability = max(probabilities)
    return probabilities, max_probability

convert_udf = udf(dense_vector_to_list, ArrayType(FloatType()))

# Dictionary to map numeric labels to sentiment text
label_to_sentiment = {0: "Neutral", 1: "Positive", 2: "Negative", 3: "Irrelevant"}
sentiment_mapping_udf = udf(lambda x: label_to_sentiment[x], StringType())

# Apply the pre-trained model to the stream
predictions = lr_model.transform(df)

# Convert DenseVector to list and find max probability
predictions = predictions.withColumn('Confidence', convert_udf(predictions['probability']))
predictions = predictions.withColumn('Confidence_Score', predictions['Confidence'][1] * 100)

# Map numeric prediction to sentiment text
predictions = predictions.withColumn('Predicted_Sentiment', sentiment_mapping_udf(predictions['prediction']))

# Select necessary columns for storage
predicted_data = predictions.select(
    col('ID'),
    col('Entity'),
    col('Content'),
    col('Predicted_Sentiment'),
    col('Confidence_Score'),
)

# Connect to MongoDB
client = MongoClient(f"mongodb://{mongo_host}:27017/")
db = client["fastfood_db"]
collection = db["fastfood_collection"]

# Define a function to save the results to MongoDB
def save_to_mongodb(epoch, epoch_id):
    predicted_data_dict = [row.asDict() for row in epoch.collect()]
    if predicted_data_dict:
        collection.insert_many(predicted_data_dict)
        logger.info("Predicted data stored in MongoDB")
    else:
        logger.info("No data to store")

# Write the results to MongoDB
query = predicted_data.writeStream \
    .outputMode("append") \
    .foreachBatch(save_to_mongodb) \
    .start() \
    .awaitTermination()

# Await termination
query.awaitTermination()