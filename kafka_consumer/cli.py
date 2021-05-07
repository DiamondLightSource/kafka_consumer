from argparse import ArgumentParser

from kafka_consumer import __version__, consume_and_write


def main(args=None):
    parser = ArgumentParser()
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("broker", type=str, help="Broker")
    parser.add_argument("group", type=str, help="Group")
    parser.add_argument("topic", type=str, help="Topic")
    args = parser.parse_args(args)
    consume_and_write(
        args.broker,
        args.group,
        args.topic,
        "/dls/science/users/wqt58532/kafka_consumer",
        "test_consume.h5",
        50,
    )
