import sys
from time import time

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from kafka_consumer.h5_file import H5File


def get_num_partitions_for_topic(broker, topic):
    conf = {"bootstrap.servers": broker}
    kafka_admin = AdminClient(conf)
    kafka_topic_metadata = kafka_admin.list_topics().topics
    num_partitions = len(kafka_topic_metadata.get(topic).partitions)
    return num_partitions


def find_offsets_from_time(consumer, topic, num_partitions=1, secs_since_epoch=time()):
    search_ms = secs_since_epoch * 1000 + 1
    topic_partions_to_search = [
        TopicPartition(topic, p, int(search_ms)) for p in range(num_partitions)
    ]
    offsets = consumer.offsets_for_times(topic_partions_to_search, timeout=1.0)
    return offsets


def consume_and_write(
    broker, group, topic, filepath, filename, num_arrays, timestamp=time()
):
    """Simple kafka consumer

    Args:
        broker: Desc
        group: Desc
        topics: Desc
    """

    def print_assignment(consumer, partitions):
        print("Assignment:", partitions)

    num_partitions = get_num_partitions_for_topic(broker, topic)

    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        "bootstrap.servers": broker,
        "group.id": group,
        "session.timeout.ms": 6000,
        "auto.offset.reset": "earliest",
    }
    c = Consumer(conf)

    offsets = find_offsets_from_time(c, topic, num_partitions, timestamp)
    c.assign(offsets)
    for offset in offsets:
        c.seek(offset)

    h5file = H5File()
    h5file.create(filepath, filename, num_arrays)
    num_msgs_consumed = 0

    try:
        while num_msgs_consumed < num_arrays:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                num_msgs_consumed += 1
                print(
                    f"Topic: {msg.topic()} "
                    f"Partition: [{msg.partition()}] "
                    f"Offset: {msg.offset()} "
                    f"Key: {msg.key()} "
                    f"Time: {time()}"
                )
                h5file.add_array_from_flatbuffer(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write("%% Aborted by user\n")

    finally:
        # Close down consumer to commit final offsets.
        c.close()
