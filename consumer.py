from kafka import KafkaConsumer
from kafka.structs import TopicPartition

consumer = KafkaConsumer(bootstrap_servers=['192.168.30.91:9092'])

consumer.assign(
    [TopicPartition(topic='001_charging_starcharge_startcharge', partition=0)])

consumer.seek(TopicPartition(
    topic='001_charging_starcharge_startcharge', partition=0), 0)


for msg in consumer:
    recv = "%s:%d:%d: key=%s value=%s" % (
        msg.topic, msg.partition, msg.offset, msg.key, msg.value)
    print(recv)
