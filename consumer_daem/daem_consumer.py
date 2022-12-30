import daemon


import sys
import os
from dotenv import load_dotenv
import time
from loguru import logger

import json
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata


from instance_csv import write_in_gp


load_dotenv()

logger.add(sys.stdout, format="{time} {level} {message}", level="INFO")
server = os.getenv("SERVER")
topic = os.getenv("TOPIC")
group = os.getenv("GROUP")


with daemon.DaemonContext(stderr=sys.stderr):
    while True:
        """Creating a background process for reading messages"""
        consumer = KafkaConsumer(
                "test-topic",
                group_id = "Rus",
                bootstrap_servers = "localhost:9092, localhost:39092",
                value_deserializer = lambda x : json.loads(x.decode('utf-8')),
                api_version = (0, 10),
                consumer_timeout_ms = 1000,
                enable_auto_commit = False,
                fetch_min_bytes = 300)
        logger.info("the consumer is ready")


        while True:
            try:
                q = []
                for msg in consumer:
                    print(msg.value)
                    record = msg.value
                    q.append(record)
                    tp = TopicPartition(msg.topic, msg.partition)
                    om = OffsetAndMetadata(msg.offset+1, msg.timestamp)
                    consumer.commit({tp:om})

                z = len(q)
                logger.info(f"the consumer read {z} messages")
                write_in_gp(q, z)
                time.sleep(10) 
            except Exception as er:
                logger.exception("problem with write")
            finally:
                consumer.close  
                logger.info("consumer connection was closed")
                time.sleep(4)