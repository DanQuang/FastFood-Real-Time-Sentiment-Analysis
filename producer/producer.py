from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import logging
import json
import csv
import time 
import re

# config logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server = 'kafka:9092'
topic_name = 'test_topic'

# Set up Kafka admin
admin_client = KafkaAdminClient(
    bootstrap_servers= server,
    client_id= "fastfood_producer"
)

# Create topic if not exist
try:
    exist_topic = admin_client.list_topics()
    if topic_name not in exist_topic:
        topic = NewTopic(name= topic_name,
                         num_partitions= 1,
                         replication_factor= 1)
        admin_client.create_topics(new_topics=[topic])
        logger.info(f"Created new topic: {topic_name}")
    else:
        logger.info(f"Topic {topic_name} already exists")
except Exception as e:
    logger.error(f"Failed to create topic: {e}")
    exit(1)

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=server,
    api_version=(0, 11, 5),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

def clean_content(content):
    cleaned_content = re.sub(r'[^a-zA-Z\s]', '', content)
    cleaned_content = cleaned_content.lower()
    return cleaned_content

# Read CSV file and send messages to Kafka
try:
    with open("data.csv", 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            message = {
                "ID": row["ID"],
                "Entity": row["Entity"],
                "Content": row["Content"],
                "Cleaned Content": clean_content(row['Content'])
            }
            producer.send(topic_name, message)
            logger.info(f"Sent message: {message}")
            time.sleep(1)

    # Ensure all messages are sent
    producer.flush()
except Exception as e:
    logger.error(f"Failed to send messages: {e}")
finally:
    producer.close()
