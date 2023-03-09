from time import sleep
from json import dumps
import logging

from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic



admin_client = KafkaAdminClient(
    bootstrap_servers="broker:9092", 
    client_id='test'
)

topic_name = "messages"
try:
    admin_client.create_topics(new_topics=[NewTopic(name=topic_name, num_partitions=1, replication_factor=1)], validate_only=False)
except TopicAlreadyExistsError:
    logging.info(f"topic {topic_name} already exist")

producer = KafkaProducer(bootstrap_servers=['broker:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

for i in range(1000):
    data = {'message': i}
    producer.send(topic_name, value=data)
    logging.info(f"message sent: {data}")
    sleep(3)
