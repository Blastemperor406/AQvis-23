import paho.mqtt.subscribe as subscribe
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9093',security_protocol="PLAINTEXT")
topic_name = 'sensor_data'

try:
    while True:
        msg = subscribe.simple("sensor_data", hostname="localhost", port = 1884)
        print("%s %s" % (msg.topic, msg.payload))
        data = json.dumps(msg.payload.decode("utf-8"))
        producer.send(topic_name, data.encode('utf-8'))
except:
    producer.close()
