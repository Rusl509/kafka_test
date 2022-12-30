from random import choice, randint
from loguru import logger
import json
import sys
import datetime


def generate_radom():
    """
    creating data for a message
    """
    event_time = datetime.datetime.now()
    print(event_time)
    id_sensor = randint(10000000, 10000007)
    latitude_and_longitude = ["53.72, 91.43", "43.43, 39.92", "55.81, 37.96", "56.14, 40.4", "44.56, 38.08", "42.07, 48.29", "55.6, 38.12", "57, 40.97"]
    dict_with_id_and_lati = dict(zip(range(10000000, 10000008), latitude_and_longitude))
    ll_for_senseser = dict_with_id_and_lati[id_sensor]
    temperature = choice(range(-20, 20))
    controller_id = hash(id_sensor)
    message_for_topic = {"event_time": f"{event_time}", "id_sensor": id_sensor, "sensor_location": ll_for_senseser, "temperature" : temperature, "controller_id" : controller_id}
    return message_for_topic


def publish_message(kafka_producer, topic_name, key, value):
    try:
        kafka_producer.send(topic_name, key=key, value=value)
        kafka_producer.flush()
    except Exception as ex:
         logger.exception("Exception during sending")
print(generate_radom()["id_sensor"])