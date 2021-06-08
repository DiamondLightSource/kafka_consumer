import pytest
from confluent_kafka.admin import TopicMetadata

from kafka_consumer.consume_and_write import KafkaConsumer


@pytest.fixture
def consumer():
    return KafkaConsumer("", "", "")


@pytest.fixture(autouse=True)
def mock_metadata(mocker):
    metadata = TopicMetadata()
    metadata.topic = "test_topic"
    metadata.partitions = {0: "partition_0", 1: "partition_1", 2: "partition_2"}
    mocker.patch(
        "kafka_consumer.consume_and_write.get_topic_metadata", return_value=metadata
    )


@pytest.fixture(autouse=True)
def mock_consumer(mocker):
    mocker.patch("kafka_consumer.consume_and_write.Consumer")


def test_mutually_exclusive_args(consumer):
    with pytest.raises(ValueError):
        consumer.consume_and_write(
            "", "", 10, start_offsets=[10, 10, 10], secs_since_epoch=1623081172
        )


def test_num_partitions(consumer):
    with pytest.raises(ValueError):
        consumer.consume_and_write("", "", 10, start_offsets=[10, 10, 10, 10])
