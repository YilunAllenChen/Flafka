import datetime
import json
import time
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
future = producer.send('account', json.dumps(
    {"method": "get", "step": "1", "type": "test", "testName": "kafka",
     "cid": "{0}".format(datetime.datetime.now().strftime('%Y%m%d%H%M%S')),
     "info": "demo{}".format(1)}).encode())
record_metadata = future.get(timeout=10)
print( record_metadata, datetime.datetime.now().strftime('%Y%m%d%H%M%S'))


producer.send