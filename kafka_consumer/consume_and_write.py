import logging
import sys
from pathlib import Path
from typing import List, Optional

from confluent_kafka import Consumer, KafkaException, TopicPartition

from kafka_consumer.h5_file import H5File
from kafka_consumer.kafka_admin import get_topic_metadata
from kafka_consumer.utils import array_from_flatbuffer

log = logging.getLogger(__name__)


class KafkaConsumer:
    """Consume serialized NDArrays from kafka and write to h5file

    Args:
        broker: Broker name
        group: Consumer group ID
        topic: Topic name
    """

    def __init__(self, broker: str, group: str, topic: str):
        self.broker = broker
        self.group = group
        self.topic = topic
        topic_metadata = get_topic_metadata(broker, topic)
        self.num_partitions = len(topic_metadata.partitions)

        # Consumer configuration
        # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        # Add "debug": "consumer,cgrp,topic,fetch", for debug info from kafka
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
            try:
                while True:
                    msg_list = consumer.consume(timeout=1.0)
                    if msg_list:
                        msg = msg_list[0]
                        break
            except KeyboardInterrupt:
                log.error("User aborted whilst waiting for array")
                raise
            array_id = array_from_flatbuffer(msg.value()).Id()
            if not min_array_id:
                min_array_id = array_id
            else:
                min_array_id = min(min_array_id, array_id)
            consumer.unassign()
        return min_array_id

    def consume_and_write(
        self,
        filepath: Path,
        filename: str,
        num_arrays: int,
        start_offsets: Optional[List[int]] = None,
        secs_since_epoch: Optional[int] = None,
        first_array_id: Optional[int] = None,
    ):
        """Consume serialized NDArrays from kafka and write to h5file

        Args:
            filepath: Path to h5file
            filename: Name of h5file
            num_arrays: Number of arrays to write to file
            start_offsets: First offset to consume from each partition
            secs_since_epoch: Starting array timestamp (unix epoch time) to consume from
            first_array_id: ID of first NDArray to write
        """

        if start_offsets and secs_since_epoch:
            raise ValueError(
                "start_offsets and secs_since_epoch are mutually exclusive"
            )

        if start_offsets and len(start_offsets) != self.num_partitions:
            raise ValueError(
                "Length of provided offsets not equal to number of partitions in topic"
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

        log.info(
            f"Set consumer partition assignment to {topic_partition_start_offsets}"
        )
        c.assign(topic_partition_start_offsets)

        h5file = H5File()
        h5file.create(filepath, filename, num_arrays, first_array_id)

        try:
            unassigned = 0
            while h5file.array_count < num_arrays:
                msg = c.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    # Proper message
                    log.debug(
                        f"Topic: {msg.topic()} "
                        f"Partition: [{msg.partition()}] "
                        f"Offset: {msg.offset()} "
                        f"Key: {msg.key()}"
                    )
                    valid_array = h5file.add_array_from_flatbuffer(msg.value())
                    if not valid_array:
                        log.debug(f"Unassigning partition id {msg.partition()}")
                        unassigned += 1
                        c.incremental_unassign(
                            [TopicPartition(self.topic, msg.partition())]
                        )
                        if unassigned == self.num_partitions:
                            log.warning(
                                f"{num_arrays - h5file.array_count} Frames missing from kafka"
                            )
                            break
                    log.debug(f"Num written is {h5file.array_count}")
                    log.debug(f"Number of unassigned partitions {unassigned}")

        except KeyboardInterrupt:
            sys.stderr.write("%% Aborted by user\n")

        finally:
            log.info(f"Total write time was {h5file.total_write_time}")
            # Close down consumer to commit final offsets.
            c.close()
