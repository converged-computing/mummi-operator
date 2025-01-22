import os
import traceback

import mummi_core
import mummi_ras
import yaml

import mummi_operator
from mummi_operator.client import get_subparser_helper
from mummi_operator.config import load_config
from mummi_operator.logger import setup_logger

from .runner import MLRunner


def get_parser():
    parser = argparse.ArgumentParser(
        description="Mummi Operator Machine Learning Job",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--debug",
        help="logger debug mode",
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
        "--mlconfig",
        help="Machine Learning config filename",
        default="mlserver.yaml",
    )
    start.add_argument(
        "--config-dir",
        help="Directory with configuration files.",
    )
    return parser


def load_mlrunner_config(config_dir, config_file):
    """
    load and validate content of MLRunner config
    """
    config = load_config(config_dir, config_file)
    if config.get("encoder") is None or config["encoder"].get("path") is None:
        raise ValueError("No encoder specified")
    if config.get("workspace") is None or config["workspace"].get("path") is None:
        raise ValueError("No workspace specified")
    if config.get("sampler") is None:
        raise ValueError("No sampler specified")
    if config["sampler"]["feedback"].get("do_feedback") and (
        config["sampler"]["feedback"].get("database") is None
        or config["sampler"]["feedback"].get("frame_database") is None
    ):
        val = config["sampler"]["feedback"].get("do_feedback")
        raise ValueError(
            f"For sampler, if do_feedback = {val}, you must specify database and frame_database"
        )

    # Load paths from eval strings in config
    gdict = {"mummi_ras": mummi_ras}
    for key in ["workspace", "encoder"]:
        if type(config[key]["path"]) is dict and "eval" in config[key]["path"]:
            config[key]["path"] = eval(config[key]["path"]["eval"], gdict)

    for key in ["sampler", "generator", "validator"]:
        for val in ["inpath", "outpath"]:
            if (
                config[key].get(val)
                and type(config[key][val]) is dict
                and "eval" in config[key][val]
            ):
                config[key][val] = eval(config[key][val]["eval"], gdict)

    return config


# -----------------------------------------------------------------------------
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

    # retrieve subparser (with help) from parser
    helper = get_subparser_helper(args, parser)

    mummi_core.init()
    mummi_core.create_root()

    config = load_mlrunner_config(args.config_dir, args.config)
    mummi_core.init_logger(config=config["config"])
    try:
        server = MLRunner(config=config, logger=LOGGER, number_samples=args.number_samples)
        server.setup()
        server.run()
    except Exception as e:
        LOGGER.error(f"> Exiting ML runner due to error ({e})")
        traceback.print_exc()
        exit(1)


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
