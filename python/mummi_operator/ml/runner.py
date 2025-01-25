#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Copyright (c) 2023, Lawrence Livermore National Security, LLC. All rights
# reserved. LLNL-CODE-827655. This work was produced at the Lawrence Livermore
# National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44)
# between the U.S. Department of Energy (DOE) and Lawrence Livermore National
# Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers,
# notice of U.S. Government Rights and license terms and conditions.
# -----------------------------------------------------------------------------
# @authors
#           Loic Pottier <pottier1@llnl.gov>
# ------------------------------------------------------------------------------

import datetime
import fcntl
import glob
import logging
import multiprocessing
import os
import pathlib
import pickle
import signal
import tempfile
import time
import traceback
from logging import Logger, getLogger
from timeit import default_timer as timer
from typing import Any, List, Union

import mummi_core
import numpy as np
import yaml
from filelock import FileLock
from mummi_core.workflow.flux_env import flux_uri
from mummi_ras import Naming, get_named_specfile
from mummi_ras.ml import ls_point as lsp
from mummi_ras.ml.autoencoders import (
    FullDenseAutoencoder,
    FullMiniDenseAutoencoder,
    HierarchicalCGAutoencoder,
)
from mummi_ras.ml.feedback_frames import FeedbackFrames
from mummi_ras.ml.samplers import get_interpolator, ls_sampler
from mummi_ras.ml.samplers.interpolators.feedback_interpolator import FeedbackOTInterpolator
from mummi_ras.ml.samplers.interpolators.ot_interpolator import OTInterpolator
from mummi_ras.ml.validators import CGValidator

import mummi_operator.manager.registry as registry

# Print debug for now
logging.basicConfig()
LOGGER = getLogger(__name__)
logging.root.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)


def write_patches(
    outpath: str, iteration_id: int, new_positions: np.array
) -> Union[List[str], List[Any]]:
    """
    From positions, write a patch as a npz file and return its path.
    """
    RPATH = os.path.join(outpath, iteration_id)
    if not os.path.isdir(RPATH):
        os.makedirs(RPATH, exist_ok=True)

    LOGGER.info(f"Write patches in {RPATH}")
    npatches = new_positions.shape[0]
    files = np.empty(npatches, dtype="object")
    positionsArray = []
    namesArray = []
    all_structure_names = []

    # We want to avoid erasing previously generated structures
    offset = _latest_patch_id_generated(RPATH)

    # Create an .npz for each new patch
    for sidx in range(npatches):
        positions = new_positions[sidx]
        structure_id = "{:012d}".format(offset + sidx)

        structure_name = f"{iteration_id}_{structure_id}"
        all_structure_names.append(structure_name)
        outfile_positions = os.path.join(RPATH, f"{structure_name}.npz")
        files[sidx] = f"{structure_name}.npz"
        LOGGER.info(f"Writing {outfile_positions}")
        np.savez_compressed(outfile_positions, data=positions)

        positionsArray.append(positions)
        namesArray.append(structure_name)

    return namesArray, positionsArray


def _latest_patch_id_generated(RPATH: str) -> int:
    """
    Find in the directory containing potentially already some structures
    like structure_iter00_X.gro. This function finds the highest X so we can
    write the patch X+1 safely without erasing any structures.
    """
    nsamples = len(glob.glob(RPATH + "/*.npz"))
    nsamples = nsamples - 2  # the - 2 is to remove master_iterX.npz and table_all_predictions.npz
    return max(nsamples, 0)  # to avoid returning a negative number if RPATH was empty


def create_sampling_db(database: str, model_name: str):
    """
    Create an empty sampling feeback DB.
    """
    dirname = os.path.dirname(database)
    ts = datetime.datetime.now().strftime("%d.%m.%Y-%H:%M:%S")
    if dirname != "":
        os.makedirs(dirname, exist_ok=True)
    data = {
        "model_name": model_name,
        "created_at": ts,
        "updated_at": ts,
        "structure_names": np.empty(0, dtype=str),
        "ls_coords": np.empty(0, dtype=float),
        "lambda_values": np.empty(0, dtype=bool),
        "validation_status": np.empty(0, dtype=bool),
        "createsims_status": np.empty(0, dtype=int),
    }
    np.savez_compressed(database, **data)
    LOGGER.info(f"Created new DB at {database}")


def checking_sampling_db(database: str, model_name: str) -> bool:
    """
    Update sampling feeback DB with new samples.
    """
    if not os.path.isfile(database):
        return True
    try:
        sampling_db = np.load(database, allow_pickle=True)
    except Exception as e:
        LOGGER.error(f"{database} {e}")
        return False
    prev_model_name = sampling_db["model_name"]
    if model_name != prev_model_name:
        LOGGER.error(f"Old sampling DB cannot be used with different ML model.")
        LOGGER.error(f"This DB {database} has been created with ML model {prev_model_name}")
        LOGGER.error(f"You are currently used Latent Space {model_name}")
        return False
    return True


def update_sampling_db(
    database: str,
    num_new_sample: int,
    model_name: str,
    structure_names: List[str],
    ls_coords: List[List[float]] = [],
    lambda_values: List[float] = [],
    validation_status: List[bool] = [],
    new_createsims_status: List[bool] = [],
):
    """
    Update sampling feeback DB with new samples.
    """

    # File we will be writing (just in case to not corrupt the file in case of interruption)
    timestr = time.strftime("%Y%m%d-%H%M%S")
    db_path = os.path.splitext(database)
    database_tmp = f"{db_path[0]}-{timestr}{db_path[1]}"

    if not os.path.isfile(database):
        create_sampling_db(database, model_name)

    try:
        with np.load(database, allow_pickle=True) as sampling_db:
            prev_model_name = sampling_db["model_name"]
            created_at = sampling_db["created_at"]
            prev_struct = sampling_db["structure_names"]
            prev_coords = sampling_db["ls_coords"]
            prev_lambda = sampling_db["lambda_values"]
            prev_validation = sampling_db["validation_status"]
            prev_createsims = sampling_db["createsims_status"]
    except Exception as e:
        LOGGER.error(f"{database} {e}")
        return

    new_records = len(structure_names)
    ts = datetime.datetime.now().strftime("%d.%m.%Y-%H:%M:%S")
    if model_name != prev_model_name:
        LOGGER.error(f"Old sampling DB cannot be used with different ML model.")
        LOGGER.error(f"This DB {database} has been created for LS coordinates of LS:")
        LOGGER.error(f" - {prev_model_name}")
        LOGGER.error(f"You are currently used Latent Space {model_name}")
        LOGGER.error(f"Feedback is deactivated.")
        return

    data = {"model_name": model_name, "created_at": created_at, "updated_at": ts}

    # Update sampling DB for feedback
    data["structure_names"] = np.append(prev_struct, structure_names)
    if ls_coords != []:
        if len(prev_coords) > 0:
            data["ls_coords"] = np.append(prev_coords, ls_coords, axis=0)
        else:
            data["ls_coords"] = ls_coords
    else:
        data["ls_coords"] = prev_coords
    if lambda_values != []:
        data["lambda_values"] = np.append(prev_lambda, lambda_values)
    else:
        data["lambda_values"] = prev_lambda
    if validation_status != []:
        data["validation_status"] = np.append(prev_validation, validation_status)
    else:
        data["validation_status"] = prev_validation
    # 0 means False, 1 True and 2 is unknown state for createsims
    # As no createsims for these structures are running, they are all unknowns
    if new_createsims_status != []:
        data["createsims_status"] = new_createsims_status
    else:
        data["createsims_status"] = np.append(
            prev_createsims, np.full(num_new_sample, 2, dtype=int)
        )

    np.savez_compressed(database_tmp, **data)
    # We make sure the new file is not corrupted somehow
    try:
        with np.load(database_tmp, allow_pickle=True) as test:
            LOGGER.debug(f"{database_tmp} is valid {test.files}")
    except Exception as e:
        LOGGER.warning(f"{database_tmp} seems to be corrupted. We keep the old {database} intact")
        return

    os.replace(database_tmp, database)

    LOGGER.debug(f"Added new structures: {structure_names}")
    LOGGER.info(f"Upgraded DB at {database} with {new_records} new records")


def create_sets(training_data_files: List[str]) -> List[List]:
    sets = {}
    assert len(list(training_data_files)) != 0

    # detect number of sets
    for fn in training_data_files:
        data = np.load(fn)
        state_labels = sorted(np.unique(data["labels"]))
        for label in state_labels:
            bary = data["labels"] == label
            if label in sets:
                sets[label] = np.concatenate((sets[label], data["x_test_encoded"][bary]))
            else:
                sets[label] = data["x_test_encoded"][bary]

    counter = 0
    lsp_sets = []
    for label in sets:
        lsp_sets.append(
            [
                lsp.LSPoint(
                    counter + sets[label].shape[0],
                    parent=str(label),
                    ls_coord=sets[label][i],
                    valid=True,
                )
                for i in range(sets[label].shape[0])
            ]
        )
        counter += sets[label].shape[0]

    return lsp_sets


class MLRunner:
    """
    ML runner implementation for Mummi Operator

    Intended to be run as a job.
    """

    def __init__(self, config: dict, ids, outdir: str) -> None:
        self.config = config
        self.logger = LOGGER
        self.outdir = outdir
        self.ids = ids
        self.hostname = mummi_core.get_hostname(contract_hostname=False)
        # Could be process if we choose to activate feedback
        self.feedback = None
        self.filelock = tempfile.mkstemp(prefix="mummi-sampling-", suffix=".lock", dir=None)[1]
        # time between two checks for createsims status (in seconds)
        self.waittime = 180
        self.mini_mummi = False

    @property
    def n_max_selected_patch_buffer(self):
        return self.config["sampler"].get("n_max_selected_patch_buffer") or 0

    @property
    def database_dir(self):
        return os.path.join(self.config["sampler"]["outpath"], "feedback")

    @property
    def sampling_db(self):
        return os.path.join(self.database_dir, self.config["sampler"]["feedback"]["database"])

    @property
    def feedbackframe_db(self):
        return os.path.join(self.database_dir, self.config["sampler"]["feedback"]["frame_database"])

    @property
    def sampler_interpolator(self):
        return self.config["sampler"]["interpolator"]

    @property
    def pickle_interpolator(self):
        return self.config["sampler"].get("pre_computed")

    @property
    def encoder_name(self):
        return self.config["encoder"]["model"]

    @property
    def encoder_path(self):
        return os.path.join(self.config["encoder"]["path"], self.encoder_name)

    def read_specs(self):
        self.logger.info(f"ML Server launched on host: {self.hostname}")
        os.makedirs(self.database_dir, exist_ok=True)
        self.do_feedback = bool(self.config["sampler"]["feedback"]["do_feedback"])
        self.mini_mummi = bool(self.config["config"].get("mini_mummi", False))
        if self.pickle_interpolator and os.path.isfile(self.pickle_interpolator):
            self.logger.info(f"Pre-computed interpolator found: {self.pickle_interpolator}")

        self.logger.info(f"> Initializing MuMMI ML Runner")
        self.logger.info(f"  > Mini-Mummi                 {self.mini_mummi}")
        self.logger.info(f"  > Sampler")
        self.logger.info(f"    > Interpolator             {self.sampler_interpolator}")
        self.logger.info(f"    > Run with feedback        {self.do_feedback}")
        self.logger.info(f"    > Createsims Feedback DB   {self.sampling_db}")
        self.logger.info(f"    > CG frames Feedback DB    {self.feedbackframe_db}")
        self.logger.info(f"  > Generator")
        self.logger.info(f"    > Encoder                  {self.encoder_path}")
        self.logger.info(f"  > Validator")
        self.logger.info(f"  > nMaxSelectedPatchBuffer    {self.n_max_selected_patch_buffer}")

    def _setup_training_sets(self):
        training_dir = os.path.join(self.encoder_path, "training")
        # PC3: round 0: validation data are contained in many different files
        # training_data = pathlib.Path(training_dir).glob("validation_data_r*.npz")
        # PC3: round 1: If Konstantia generates new validation data for MuMMI which are contained in one file
        training_data = list(pathlib.Path(training_dir).glob("validation_data_all.npz"))
        LOGGER.info(f"Training data {training_dir} => {len(training_data)} files")
        states = create_sets(training_data)

        if self.mini_mummi:
            # Only two states in mini MuMMI
            states = states[:2]
        else:
            # states 4 and 5 are not useful for know (A''' and unknown state)
            states = states[:3]

        sub_sample_frac = float(self.config["sampler"]["sub_sample_frac"])
        assert 0.001 < sub_sample_frac <= 1

        # Needed for OTInterpolator because size of sets must be equals
        min_size = len(states[0])
        for s in states:
            if min_size > len(s):
                min_size = len(s)

        mask = np.random.uniform(size=(min_size,)) <= sub_sample_frac
        for i in range(len(states)):
            if len(states[i]) > min_size:
                states[i] = states[i][:min_size]
            # We sub-sample uniformly because POT is too slow for the whole states
            states[i] = list(np.array(states[i])[mask])

        return states

    def setup_sampler(self):
        """
        Setup the sampler.
        """
        # Assume we aren't doing feedback
        feedback_db_path = None
        feedbackframe_db = None

        if self.do_feedback:
            # Setting feedback DB for sampler
            feedback_db_path = self.sampling_db
            feedbackframe_db = self.feedbackframe_db

            # self.sampling_db is the feedback_db_path
            if not os.path.isfile(feedback_db_path):
                create_sampling_db(feedback_db_path, self.encoder_path)

            # Fall back to not doing feedback if invalid
            if not checking_sampling_db(feedback_db_path, self.encoder_path):
                feedback_db_path = None
                feedbackframe_db = None
                LOGGER.warning(f"Feedback {feedback_db_path} is not valid for this ML model.")
                LOGGER.warning(f"All feedback is deactivated for this run.")
            else:
                LOGGER.info(f"Feedback DB for sampling located in {feedback_db_path}")
        else:
            LOGGER.info(f"All feedback is deactivated for this run.")

        start = time.time()
        states = self._setup_training_sets()

        # Don't try if not defined / does not exist.
        # This won't catch another kind of error - I'd like to see what that is
        does_not_exist = not self.pickle_interpolator or not os.path.exists(
            self.pickle_interpolator
        )
        if not does_not_exist:
            with open(self.pickle_interpolator, "rb") as file:
                self.interpolator = pickle.load(file)
                end = time.time() - start
                LOGGER.info(
                    f"Loaded pre-computed interpolator {self.pickle_interpolator} in {end:.03f} seconds for {self.interpolator.size()} LS points"
                )
        else:
            LOGGER.warning(f"Could not load pre-computed interpolator. Computing interpolator")
            interpolator = get_interpolator(self.sampler_interpolator)
            self.interpolator = interpolator(
                states=states,
                kneigh=int(self.config["sampler"].get("kneighbors", 10)),
                lowerbound=float(self.config["sampler"]["lambda_lowerbound"]),
                upperbound=float(self.config["sampler"]["lambda_upperbound"]),
                num_iter_max=int(self.config["sampler"].get("num_iter_max", 1000000)),
            )
            end = time.time() - start
            LOGGER.info(
                f"Interpolator {self.sampler_interpolator} created in {end:.03f} seconds for {self.interpolator.size()} LS points"
            )

        return ls_sampler.LSSampler(
            states=states,
            interpolator=self.interpolator,
            feedback_file=feedback_db_path,
            feedback_frame=feedbackframe_db,
        )

    def setup_generator(self):
        if self.mini_mummi:
            return FullMiniDenseAutoencoder(model_path=self.encoder_path)
        else:
            return FullDenseAutoencoder(model_path=self.encoder_path)
            # @TODO add code to swtich between encoders
            # generator = HierarchicalCGAutoencoder(
            #     model_path = model_path
            # )

    def setup_validator(self):
        resource_name = self.config["validator"]["resources"]
        complex_name = self.config["validator"]["complex"]
        healing = bool(self.config["validator"]["healing"])
        cleanup = bool(self.config["validator"]["cleanup"])
        resource_path = Naming.dir_res(resource_name)

        return CGValidator(
            iteration_path=self.outdir,
            resource_path=resource_path,
            complex_name=complex_name,
            healing=healing,
            cleanup=cleanup,
            mini_mummi=self.mini_mummi,
        )

    def setup(self) -> None:
        """
        Setup the MLRunner
        """
        self.read_specs()
        self.sampler = None
        self.generator = self.setup_generator()
        self.validator = self.setup_validator()
        # # Will gather result
        self.sampler = self.setup_sampler()
        # TODO the feedback would be here.

    def run(self) -> None:
        """
        Run the mlserver to generate some number of samples.
        """
        oras = self.config.get("oras") if self.config["config"].get("use_oras") is True else None
        LOGGER.info(f"Oras setup {oras}")

        # Generate new samples and push to registry
        for jobid in self.ids:
            sample = self.generate_new_sample(jobid)
            push_artifact(
                sample[0],
                name=jobid,
                host=oras["host"],
                tls_verify=oras["tls_verify"],
                plain_http=oras["plain_http"],
            )

    def generate_new_sample(self, jobid):
        """
        Generate a sample structure that passes validation.

        Note that saving to the feedback database is removed since we are running as a job
        and won't use it, but we do need to address how to get some kind of randomness.
        """
        new_ls_coords = []
        new_lambda = []
        new_validation_status = []
        num_new_sample = 0

        # Keep going until we have a valid sample
        while True:
            sample_start = time.time()
            ls_coords = self.sampler.get_new_ls_points(1)
            sample_end = time.time() - sample_start
            LOGGER.info(f"Sampled 1 in {sample_end} seconds")
            for pts in ls_coords:
                new_ls_coords.append(pts.get_coordinates())
                new_lambda.append(pts.get_lamda())
            num_new_sample += len(ls_coords)

            new_positions = self.generator.decode(ls_coords)
            names_array, positions_array = write_patches(
                outpath=self.config["generator"]["outpath"],
                iteration_id=jobid,
                new_positions=new_positions,
            )
            LOGGER.debug(f"generated structures done. new_positions = {new_positions.shape}")

            # Our jobid looks like structure_<number> and we need to pass just the number here
            # This isn't great, but I don't want to copy over all the validator code
            iteration_id = int(jobid.replace("structure_", ""))
            return_array = self.validator.validateArray(iteration_id, names_array, positions_array)
            is_valid = return_array[0][0]

            # if not valid, try again
            if not is_valid:
                continue

            # Write npz to file
            _, valid_files = self.validator.write_validation_info(
                iteration_id=iteration_id,
                return_array=return_array,
                all_structure_names=names_array,
            )
            valid_files = [os.path.join(self.validator.current_rpath, f) for f in valid_files]
            LOGGER.debug(
                f"mlrunner {jobid} => valid structures={[os.path.join(self.validator.current_rpath, f) for f in valid_files]}"
            )
            if is_valid:
                break

        return valid_files


def push_artifact(path, name, host, tls_verify=None, plain_http=None):
    """
    Push a named artifact to an OCI compliant registry
    """
    artifact = registry.RegistryArtifact()

    # The is the path and mediaType. I'm assuming this is a binary format
    artifact.add_archive(path, "application/octet-stream")
    artifact.summary()

    # Push to a URI that is cleaned / parsed.
    # registry-0.mini-mummi.default.svc.cluster.local:5000/structure_iter00_000000000388:latest
    uri = registry.generate_uri(host, name=name)
    LOGGER.info(f"Request to push {path} to oras registry {uri}")
    artifact.push(uri, tls_verify=tls_verify, plain_http=plain_http)
