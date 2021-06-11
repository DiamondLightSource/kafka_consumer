import logging
from argparse import ArgumentParser
from pathlib import Path

from kafka_consumer import KafkaConsumer, __version__
from kafka_consumer.utils import profile


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
    parser.add_argument(
        "--prof_directory",
        type=Path,
        default=Path.cwd(),
        help="Profiling results directory, default is cwd",
    )
    parser.add_argument(
        "--prof_filename",
        type=str,
        default="profile_results",
        help="Stem of profiling results filenaem, default is profile_results",
    )

    args = parser.parse_args(args)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    @profile(args.prof_directory, args.prof_filename)
    def main_inner(args):
        kafka_consumer = KafkaConsumer(args.brokers, args.group, args.topic)
        kafka_consumer.consume_and_write(
            args.directory,
            args.filename,
            args.num_arrays,
            start_offsets=args.offsets,
            secs_since_epoch=args.timestamp,
            first_array_id=args.array_id,
        )

    main_inner(args)
