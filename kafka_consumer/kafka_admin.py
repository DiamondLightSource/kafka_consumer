from confluent_kafka.admin import AdminClient


def get_topic_metadata(broker, topic):
    conf = {"bootstrap.servers": broker}
    kafka_admin = AdminClient(conf)
    topic_metadata = kafka_admin.list_topics().topics.get(topic)
    if topic_metadata is None:
        raise ValueError(f"Topic: {topic} does not exist on broker {broker}")
    return topic_metadata
