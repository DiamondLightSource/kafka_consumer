import sys
from time import time

from confluent_kafka import Consumer, KafkaException, TopicPartition

from kafka_consumer.h5_file import H5File
from kafka_consumer.kafka_admin import get_topic_metadata
from kafka_consumer.utils import array_from_flatbuffer


class KafkaConsumer:
    def __init__(self, broker, group, topic):
        self.broker = broker
        self.group = group
        self.topic = topic
        topic_metadata = get_topic_metadata(broker, topic)
        self.num_partitions = len(topic_metadata.partitions)

        # Consumer configuration
        # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        self.conf = {
            "bootstrap.servers": broker,
            "group.id": group,
            "session.timeout.ms": 6000,
            "auto.offset.reset": "earliest",
        }

    def _offsets_after_time(self, consumer, secs_since_epoch):
        search_ms = secs_since_epoch * 1000 + 1
        topic_partions_to_search = [
            TopicPartition(self.topic, p, int(search_ms))
            for p in range(self.num_partitions)
        ]
        offsets = consumer.offsets_for_times(topic_partions_to_search, timeout=1.0)
        return offsets

    def _first_array_id_from_offsets(self, consumer, topic_partition_offsets):
        min_array_id = None
        for topic_partition_offset in topic_partition_offsets:
            consumer.assign([topic_partition_offset])
            if topic_partition_offset.offset == -1:
                # No msgs after this timestamp for this offset
                break
            msg = consumer.consume()[0]
            array_id = array_from_flatbuffer(msg.value()).Id()
            if not min_array_id:
                min_array_id = array_id
            else:
                min_array_id = min(min_array_id, array_id)
            consumer.unassign()
        return min_array_id

    def consume_and_write(
        self,
        filepath,
        filename,
        num_arrays,
        start_offsets=None,
        secs_since_epoch=None,
        first_array_id=None,
    ):
        """Simple kafka consumer

        Args:
            filepath:
            filename:
            num_arrays:
        """

        if start_offsets and secs_since_epoch:
            raise ValueError(
                "start_offsets and secs_since_epoch are mutually exclusive"
            )

        if start_offsets and len(start_offsets) != self.num_partitions:
            raise ValueError(
                "Length of provided offsets not equalt to number of partitions in topic"
            )

        c = Consumer(self.conf)

        if secs_since_epoch:
            topic_partition_start_offsets = self._offsets_after_time(
                c, secs_since_epoch
            )
        elif start_offsets:
            topic_partition_start_offsets = [
                TopicPartition(self.topic, p, o) for p, o in enumerate(start_offsets)
            ]
        else:
            topic_partition_start_offsets = [
                TopicPartition(self.topic, p, 0) for p in range(self.num_partitions)
            ]

        for tp in topic_partition_start_offsets:
            if tp.error:
                raise KafkaException(tp.error)

        if not first_array_id:
            first_array_id = self._first_array_id_from_offsets(
                c, topic_partition_start_offsets
            )

        print(f"Assigning to {topic_partition_start_offsets}")
        c.assign(topic_partition_start_offsets)

        h5file = H5File()
        h5file.create(filepath, filename, num_arrays, first_array_id)

        try:
            while h5file.array_count < num_arrays:
                msg = c.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    # Proper message
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
            print(f"Total write time was {h5file.total_write_time}")
            # Close down consumer to commit final offsets.
            c.close()
