import argparse
import os
import sys
import traceback

import mummi_core
import mummi_ras
import yaml

import mummi_operator
import mummi_operator.utils as utils
from mummi_operator.config import load_config

from .runner import MLRunner


def get_parser():
    parser = argparse.ArgumentParser(
        description="Mummi Operator Machine Learning Runner",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--debug",
        help="debug mode",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--quiet",
        help="quiet mode",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--version",
        help="show software version.",
        default=False,
        action="store_true",
    )
    subparsers = parser.add_subparsers(
        help="actions",
        title="actions",
        dest="command",
    )
    subparsers.add_parser("version", description="show software version")
    start = subparsers.add_parser(
        "start",
        description="start the machine learning job",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    start.add_argument(
        "--outdir",
        help="Output directory",
        default=os.getcwd(),
    )
    start.add_argument(
        "--jobid",
        help="Sample identifiers",
        action="append",
    )
    start.add_argument(
        "--tag",
        help="Tag to push to",
    )
    start.add_argument(
        "--registry",
        help="Registry to push to",
    )
    start.add_argument(
        "--plain-http",
        help="Use plain http for push.",
        default=False,
        action="store_true",
    )
    start.add_argument(
        "--tls-verify",
        help="Use tls verify for push.",
        default=False,
        action="store_true",
    )
    start.add_argument(
        "--feedback",
        help="Do feedback",
        default=False,
        action="store_true",
    )
    start.add_argument(
        "--device",
        help="Choose gpu or cpu device",
        default="cpu",
        choices=["cpu", "gpu"],
    )
    start.add_argument(
        "--workspace",
        help="Mummi workspace",
        required=True,
    )
    start.add_argument(
        "--encoder-model",
        help="Encoder model path",
        required=True,
    )
    start.add_argument(
        "--interpolator",
        help="Sampler interpolator",
        default="ot_feedback",
    )
    start.add_argument(
        "--ml-outdir",
        help="Generator, sampler, and validator output directory",
        required=True,
    )
    start.add_argument(
        "-k",
        "--kneighbors",
        dest="kneighbors",
        help="Number of neighbors for sampler (defaults to 10)",
        type=int,
        default=10,
    )
    start.add_argument("--lambda-lowerbound", type=int, default=0)
    start.add_argument("--lambda-upperbound", type=int, default=1)
    start.add_argument(
        "--max-iterations",
        type=int,
        default=1000000,
    )
    start.add_argument(
        "--sub-sample-frac",
        type=float,
        default=0.051,
    )
    start.add_argument(
        "--no-healing",
        help="Disable healing",
        default=False,
        action="store_true",
    )
    start.add_argument(
        "--no-cleanup",
        help="Disable cleanup",
        default=False,
        action="store_true",
    )
    start.add_argument(
        "--resources",
        help="Validator resources",
        required=True,
    )
    start.add_argument(
        "--complex",
        help="Complex (gro) filename.",
        required=True,
    )
    return parser


def main():
    parser = get_parser()

    def help(return_code=0):
        version = mummi_operator.__version__
        print("\nMummi Operator Machine Learning Runner v%s" % version)
        parser.print_help()
        sys.exit(return_code)

    # If the user didn't provide any arguments, show the full help
    if len(sys.argv) == 1:
        help()

    # If an error occurs while parsing the arguments, the interpreter will exit with value 2
    args, extra = parser.parse_known_args()

    # Show the version and exit
    if args.command == "version" or args.version:
        print(mummi_operator.__version__)
        sys.exit(0)

    mummi_core.init()
    mummi_core.create_root()

    # This is an easy (but not the best API surface) for passing args
    try:
        runner = MLRunner(args)
        runner.setup()
        runner.run()
    except Exception as e:
        print(f"> Exiting ML runner due to error ({e})")
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
