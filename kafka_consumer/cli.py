import cProfile
import logging
import pstats
from argparse import ArgumentParser
from pathlib import Path

from kafka_consumer import KafkaConsumer, __version__


def main(args=None):
    parser = ArgumentParser()
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("brokers", type=str, help="List of brokers", nargs="+")
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
    parser.add_argument(
        "-d",
        "--directory",
        type=Path,
        default=Path.cwd(),
        help="Output file directory, default is cwd",
    )
    parser.add_argument(
        "-f",
        "--filename",
        type=str,
        default="data.h5",
        help="Name of output file, default is data.h5",
    )
    parser.add_argument(
        "-n", "--num_arrays", type=int, default=100, help="Number of arrays to write",
    )
    parser.add_argument(
        "--log_level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="WARNING",
        type=str,
        help="Log level",
    )

    args = parser.parse_args(args)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    kafka_consumer = KafkaConsumer(args.brokers, args.group, args.topic)
    kafka_consumer.consume_and_write(
        args.directory,
        args.filename,
        args.num_arrays,
        start_offsets=args.offsets,
        secs_since_epoch=args.timestamp,
        first_array_id=args.array_id,
    )


if __name__ == "__main__":
    prof = cProfile.Profile()
    prof.run("main()")
    prof.dump_stats("output.prof")

    stream = open("output.txt", "w")
    stats = pstats.Stats("output.prof", stream=stream)
    stats.sort_stats("cumtime")
    stats.print_stats()
