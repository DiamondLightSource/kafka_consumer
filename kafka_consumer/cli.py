from argparse import ArgumentParser

from kafka_consumer import KafkaConsumer, __version__

import cProfile
import pstats

def main(args=None):
    parser = ArgumentParser()
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("broker", type=str, help="Broker")
    parser.add_argument("group", type=str, help="Group")
    parser.add_argument("topic", type=str, help="Topic")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-t",
        "--timestamp",
        type=int,
        help="Timestamp as secs since epoch to start consuming from",
        required=False,
    )
    group.add_argument(
        "-o",
        "--offsets",
        type=int,
        nargs="+",
        help="Offsets to start consuming from - must be one for each partition",
        required=False,
    )
    parser.add_argument(
        "-i", "--array_id", type=int, help="ID of first array to write", required=False
    )
    args = parser.parse_args(args)
    kafka_consumer = KafkaConsumer(args.broker, args.group, args.topic)
    kafka_consumer.consume_and_write(
        "/dls/science/users/wqt58532/kafka_consumer",
        "test_consume.h5",
        100,
        start_offsets=args.offsets,
        secs_since_epoch=args.timestamp,
        first_array_id=args.array_id,
    )


if __name__ == "__main__":
    prof = cProfile.Profile()
    prof.run('main()')
    prof.dump_stats('output.prof')

    stream = open('output.txt', 'w')
    stats = pstats.Stats('output.prof', stream=stream)
    stats.sort_stats('cumtime')
    stats.print_stats()
