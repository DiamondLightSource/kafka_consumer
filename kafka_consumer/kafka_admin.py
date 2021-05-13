from time import time

from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient


def num_partitions_for_topic(broker, topic):
    conf = {"bootstrap.servers": broker}
    kafka_admin = AdminClient(conf)
    kafka_topic_metadata = kafka_admin.list_topics().topics
    num_partitions = len(kafka_topic_metadata.get(topic).partitions)
    return num_partitions


def offsets_after_time(consumer, topic, num_partitions=1, secs_since_epoch=time()):
    search_ms = secs_since_epoch * 1000 + 1
    topic_partions_to_search = [
        TopicPartition(topic, p, int(search_ms)) for p in range(num_partitions)
    ]
    offsets = consumer.offsets_for_times(topic_partions_to_search, timeout=1.0)
    return offsets
