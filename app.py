import json
from flask import request, Flask, jsonify
import datetime
import time
from kafka import KafkaProducer

app = Flask(__name__)

# Localhost testing
#producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')


# MA Kafka
producer = KafkaProducer(bootstrap_servers='192.168.30.91:9092',api_version=(0,11))


@app.route('/send_message/', methods=['POST'])
def send():
    req = request.json

    attrsStr = ['topic', 'value', 'key',
                'partition', 'headers', 'timestamp_ms']
    topic, value, key, partition, headers, timestamp_ms = (
        None for _ in range(6))


    '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                                MUST HAVES
    '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    topic = req['topic']
    value = json.dumps(req['value']).encode()

    '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                                OPTIONALS
    '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    if 'key' in req:
        key = req['key'].encode()       # Not Required
    if 'partition' in req:
        partition = req['partition']    # 0 by default if left blank
    if 'headers' in req:
        headers = req['headers']        # Not Required
    if 'timestamp_ms' in req:
        timestamp_ms = req['timestamp_ms']  # Timestamp can be auto-generated.

    res = producer.send(topic, value=value, key=key, headers=headers,
                        partition=partition, timestamp_ms=timestamp_ms).get(timeout=10)

    return(jsonify({
        "code": "200",
        "message": "success",
        "result": {
            "callback": str(res),
            "timestamp": str(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        }
    }))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
