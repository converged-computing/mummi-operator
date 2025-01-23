import importlib
import os
import random
import sys
import shutil

import mummi_operator.schema as schema
import mummi_operator.utils as utils
import jsonschema

from mummi_operator import schema


def load_workflow_config(config_path, config_dir=None, debug=False, validate=True):
    """
    Load the workflow config path, validating with the schema
    """
    # On the fly debugging
    if debug:
        if "logging" not in cfg:
            cfg["logging"] = {}
        cfg["logging"]["debug"] = True

    workflow = WorkflowConfig(config_path, config_dir)
    if validate:
        workflow.validate()
    return workflow


def find_config(config_dir, config_file):
    """
    Find the path of the config file
    """
    config_found = config_file
    if config_dir and not os.path.exists(config_found):
        config_found = os.path.join(config_dir, config_file)
    if not os.path.exists(config_found) and config_dir:
        sys.exit(f"Did not find {config_file} as provided or in {config_dir}")
    elif not os.path.exists(config_found):
        sys.exit(f"Did not find {config_file} as provided")
    return config_found


def load_config(config_dir, config_file):
    """
    Find and load a named configuration file.

    1. First check path provided.
    2. Then check path within context of config directory.
    """
    return utils.read_yaml(find_config(config_dir, config_file))


class WorkflowConfig:
    """
    A Mummi Operator Workflow config holds a series of steps
    """

    def __init__(self, config_path, config_dir=None):
        self.load(config_path, config_dir)
        self.jobs = {}
        self.load_jobs()

    def load(self, config_path, config_dir=None):
        """
        Load the main workflow config
        """
        self.filename = find_config(config_dir, config_path)
        self.cfg = read_yaml(self.filename)
        self.config_dir = config_dir or self.cfg.get("config_dir")

    def load_jobs(self):
        """
        Load jobs into the workflow manager and ensure configs exist.
        """
        if "jobs" not in self.cfg or not self.cfg["jobs"]:
            raise ValueError("Workflow is missing job configs.")

        # As of Python 3.7, dictionaries are ordered
        for job_config in self.cfg["jobs"]:
            # This will fail if config is not found
            job = load_config(self.config_dir, job_config["config"])

            # TODO: validate schema of job config here once done
            if "job_type" not in job:
                self.jobs[job["job_type"]] = job
            else:
                self.jobs[job_config["name"]] = job

    def validate(self):
        jsonschema.validate(self.cfg, schema=schema.mummi_workflow_config_schema)

    @property
    def max_size(self):
        return self.cfg.get("cluster", {}).get("max_size")

    @property
    def first_step(self):
        return self.cfg["jobs"][0]["name"]

    @property
    def last_step(self):
        return self.cfg["jobs"][-1]["name"]

    def next_step(self, current_name):
        """
        Get the next step based on the current step name.
        """
        last_step = None
        for job in self.cfg["jobs"]:
            if last_step is not None and last_step == current_name:
                return job["name"]

    def config_for_step(self, step_name):
        """
        Get the config for a step
        """
        if step_name not in self.jobs:
            raise ValueError(f"Step {step_name} is not known")
        return self.jobs[step_name].get("config", {})

    def nodes_for_step(self, step_name):
        """
        Get the number of nodes for a step
        """
        return self.config_for_step(step_name).get("nnodes", 1)
