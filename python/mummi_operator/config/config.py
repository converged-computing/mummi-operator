import importlib
import os
import random
import sys
import shutil

import mummi_operator.schema as schema
import mummi_operator.utils as utils
import jsonschema

from mummi_operator import schema


def load_workflow_config(config_path, config_dir=None, debug=False):
    """
    Load the workflow config path, validating with the schema
    """
    cfg = load_config(config_dir, config_path)

    # On the fly debugging
    if debug:
        if "logging" not in cfg:
            cfg["logging"] = {}
        cfg["logging"]["debug"] = True

    jsonschema.validate(cfg, schema=schema.mummi_workflow_config_schema)
    return cfg


def load_config(config_dir, config_file):
    """
    Find and load a named configuration file.

    1. First check path provided.
    2. Then check path within context of config directory.
    """
    config_found = config_file
    if config_dir and not os.path.exists(config_found):
        config_found = os.path.join(config_dir, config_file)
    if not os.path.exists(config_found) and config_dir:
        sys.exit(f"Did not find {config_file} as provided or in {config_dir}")
    elif not os.path.exists(config_found):
        sys.exit(f"Did not find {config_file} as provided")
    return utils.read_yaml(config_found)
