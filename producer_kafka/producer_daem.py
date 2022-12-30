import daemon
import sys
from kafka import KafkaProducer
from generate_message import generate_radom, publish_message
import time
import json
import os
from dotenv import load_dotenv
import sys
import logging
import sys


logger = logging.getLogger()
streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)
logger.setLevel(logging.INFO)


load_dotenv()
server= os.getenv("SERVER")
topic = os.getenv("TOPIC")


count = 0
with daemon.DaemonContext(stdout=sys.stdout, stderr=sys.stderr):
    while True:
        try:
            kafka_producer = KafkaProducer(bootstrap_servers=server, api_version=(0, 10), value_serializer = lambda x : json.dumps(x).encode('utf-8'))
            value = generate_radom()
            key = value["id_sensor"]
            print(kafka_producer)
            print(key)
            publish_message(kafka_producer, "test-topic", json.dumps(key).encode(), value)
            count += 1
            logger.info(f"{count} messages were sent")
        except Exception as er:
            logger.exception("in send")
        finally:
            kafka_producer.close
            logger.info(f"Connection was closed")
            time.sleep(8)

