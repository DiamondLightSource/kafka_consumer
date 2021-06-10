from confluent_kafka.admin import AdminClient


def get_topic_metadata(brokers, topic):
    conf = {"bootstrap.servers": ",".join(brokers)}
    kafka_admin = AdminClient(conf)
    topic_metadata = kafka_admin.list_topics().topics.get(topic)
    if topic_metadata is None:
        raise ValueError(
            f"Topic: {topic} does not exist on broker(s) {','.join(brokers)}"
        )
    return topic_metadata
