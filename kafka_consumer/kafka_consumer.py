import sys
import time

from confluent_kafka import Consumer, KafkaException

from kafka_consumer.h5_file import H5File


def consume_and_write(broker, group, topics, filepath, filename, num_arrays):
    """Simple kafka consumer

    Args:
        broker: Desc
        group: Desc
        topics: Desc
    """

    def print_assignment(consumer, partitions):
        print("Assignment:", partitions)

    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        "bootstrap.servers": broker,
        "group.id": group,
        "session.timeout.ms": 6000,
        "auto.offset.reset": "latest",
    }
    c = Consumer(conf)
    c.subscribe([topics], on_assign=print_assignment)

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
                    f"Time: {time.time()}"
                )
                h5file.add_array_from_flatbuffer(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write("%% Aborted by user\n")

    finally:
        # Close down consumer to commit final offsets.
        c.close()
