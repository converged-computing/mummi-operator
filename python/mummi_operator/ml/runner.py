#!/usr/bin/env python3

import datetime
import glob
import json
import logging
import os
import pathlib
import pickle
import platform
import time
from logging import getLogger
from typing import Any, List, Union

import numpy as np
from mummi_ras import Naming
from mummi_ras.ml import ls_point as lsp
from mummi_ras.ml.autoencoders import FullMiniDenseAutoencoder
from mummi_ras.ml.feedback_frames import FeedbackFrames
from mummi_ras.ml.samplers import get_interpolator, ls_sampler

from .utils import timed
from .validator import CGValidator

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
        LOGGER.error("Old sampling DB cannot be used with different ML model.")
        LOGGER.error(f"This DB {database} has been created with ML model {prev_model_name}")
        LOGGER.error(f"You are currently used Latent Space {model_name}")
        return False
    return True


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
    ML runner implementation for Mummi Operator.

    Intended to be run as a job. The config file is removed in favor of command line arguments.
    Timings are added for different steps.
    """

    def __init__(self, args) -> None:
        self.args = args
        self.args.tag = self.args.tag or "mlrunner"
        self.logger = LOGGER

        # The MLRunner is currently designed for Mini Mummi
        self.mini_mummi = True

        # Save total times along with timestamps of events
        self.times = {}
        self.timestamps = {}
        self.function_times = {}

    @property
    def database_dir(self):
        return os.path.join(self.args.ml_outdir, "feedback")

    @property
    def sampling_db(self):
        return os.path.join(self.database_dir, "db-feedback-sampling.npz")

    @property
    def feedbackframe_db(self):
        return os.path.join(self.database_dir, "db-feedback-frames.npz")

    @property
    def pickle_interpolator(self):
        return getattr(self.args, "pre_computed", "")

    @property
    def do_feedback(self):
        return self.args.feedback

    @property
    def encoder_name(self):
        """
        The encoder model name is directory the encoder path is in

        E.g., below, we want to return "chonky-model"
        /opt/clones/mummi_resources/ml/chonky-model/CG_pos_data_summary_pos_dis_C1_v1.npz
        /opt/clones/mummi_resources/ml/<encoder-name>/<encoder-model>
        """
        return os.path.basename(self.encoder_path)

    @property
    def encoder_path(self):
        """
        Directory the model is in.
        """
        return os.path.dirname(self.args.encoder_model)

    def add_timestamp(self, name, timestamp=None):
        """
        Add a timestamp to times. This assumes unique names.
        """
        if name in self.timestamps:
            raise ValueError(f"Already seen {name}, this should not happen.")
        self.timestamps[name] = timestamp or time.time()

    def add_time(self, name, duration):
        """
        Add a time duration to times. This assumes unique names.
        """
        if name in self.times:
            raise ValueError(f"Already seen {name}, this should not happen.")
        self.times[name] = duration

    def show_specs(self):
        """
        Show MLRunner specs for the user
        """
        self.logger.info(f"ML Server launched on host: {platform.node()}")
        os.makedirs(self.database_dir, exist_ok=True)
        if self.pickle_interpolator and os.path.isfile(self.pickle_interpolator):
            self.logger.info(f"Pre-computed interpolator found: {self.pickle_interpolator}")

        self.logger.info("> Initializing MuMMI ML Runner")
        self.logger.info(f"  > Mini-Mummi                 {self.mini_mummi}")
        self.logger.info("  > Sampler")
        self.logger.info(f"    > Interpolator             {self.args.interpolator}")
        self.logger.info(f"    > Run with feedback        {self.do_feedback}")
        self.logger.info(f"    > Createsims Feedback DB   {self.sampling_db}")
        self.logger.info(f"    > CG frames Feedback DB    {self.feedbackframe_db}")
        self.logger.info("  > Generator")
        self.logger.info(f"    > Encoder                  {self.encoder_path}")
        self.logger.info("  > Validator                   CGValidator")

    def _setup_training_sets(self):
        training_dir = os.path.join(self.encoder_path, "training")
        # PC3: round 0: validation data are contained in many different files
        # training_data = pathlib.Path(training_dir).glob("validation_data_r*.npz")
        # PC3: round 1: If Konstantia generates new validation data for MuMMI which are contained in one file
        training_data = list(pathlib.Path(training_dir).glob("validation_data_all.npz"))
        LOGGER.info(f"Training data {training_dir} => {len(training_data)} files")

        self.add_timestamp("create_sets_start")
        states = create_sets(training_data)
        self.add_timestamp("create_sets_complete")

        # Only two states in mini MuMMI
        states = states[:2]
        assert 0.001 < self.args.sub_sample_frac <= 1

        # Needed for OTInterpolator because size of sets must be equals
        min_size = len(states[0])
        for s in states:
            if min_size > len(s):
                min_size = len(s)

        mask = np.random.uniform(size=(min_size,)) <= self.args.sub_sample_frac
        for i in range(len(states)):
            if len(states[i]) > min_size:
                states[i] = states[i][:min_size]
            # We sub-sample uniformly because POT is too slow for the whole states
            states[i] = list(np.array(states[i])[mask])

        return states

    @timed
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
                LOGGER.warning("All feedback is deactivated for this run.")
            else:
                LOGGER.info(f"Feedback DB for sampling located in {feedback_db_path}")
        else:
            LOGGER.info("All feedback is deactivated for this run.")

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
                self.add_time("loaded_pre_computed_interpolator_seconds", end)
        else:
            LOGGER.warning("Could not load pre-computed interpolator. Computing interpolator")
            interpolator = get_interpolator(self.args.interpolator)
            self.interpolator = interpolator(
                states=states,
                kneigh=self.args.kneighbors,
                lowerbound=self.args.lambda_lowerbound,
                upperbound=self.args.lambda_upperbound,
                num_iter_max=self.args.max_iterations,
            )
            end = time.time() - start
            self.add_time("created_interpolator_seconds", end)
            LOGGER.info(
                f"Interpolator {self.args.interpolator} created in {end:.03f} seconds for {self.interpolator.size()} LS points"
            )

        return ls_sampler.LSSampler(
            states=states,
            interpolator=self.interpolator,
            feedback_file=feedback_db_path,
            feedback_frame=feedbackframe_db,
        )

    @timed
    def setup_generator(self):
        return FullMiniDenseAutoencoder(model_path=self.encoder_path)

    @timed
    def setup_validator(self):
        resource_path = Naming.dir_res(self.args.resources)
        return CGValidator(
            iteration_path=self.args.outdir,
            resource_path=resource_path,
            complex_name=self.args.complex,
            healing=not self.args.no_healing,
            cleanup=not self.args.no_cleanup,
        )

    @timed
    def setup(self) -> None:
        """
        Setup the MLRunner
        """
        self.show_specs()
        self.sampler = None
        self.generator = self.setup_generator()
        self.validator = self.setup_validator()

        # This will report that the feedback database is empty...
        self.sampler = self.setup_sampler()

        # Are we doing feedback? If yes, load into model database
        if self.do_feedback:
            feed_path = Naming.dir_root("feedback-cg")
            model = FullMiniDenseAutoencoder(model_path=self.encoder_path, device=self.args.device)
            self.feedback_frames = FeedbackFrames(
                database=self.feedbackframe_db,
                path=feed_path,
                model=model,
                model_name=self.encoder_path,
            )

    @timed
    def run(self) -> None:
        """
        Run the mlserver to generate some number of samples.
        """
        # Generate new samples and push to registry
        for jobid in self.args.jobid:
            if self.do_feedback:
                self.feedback_frames.work()
            sample = self.generate_new_sample(jobid)
            if not self.args.registry:
                continue
            self.add_timestamp(f"push_{jobid}_start")
            push_artifact(
                sample,
                name=jobid,
                host=self.args.registry or None,
                tag=self.args.tag,
                tls_verify=self.args.tls_verify,
                plain_http=self.args.plain_http,
            )
            self.add_timestamp(f"push_{jobid}_complete")

        # Show times collected across jobids
        self.show_times()

    def show_times(self):
        """
        Print final times and timestamps to the console (job log)
        """
        print("=== times\n" + json.dumps(self.times) + "\n===")
        print("=== timestamps\n" + json.dumps(self.timestamps) + "\n===")

    @timed
    def generate_new_sample(self, jobid):
        """
        Generate a sample structure that passes validation.

        Note that saving to the feedback database is removed since we are running as a job
        and won't use it, but we do need to address how to get some kind of randomness.
        """
        new_ls_coords = []
        new_lambda = []
        num_new_sample = 0

        # Keep going until we have a valid sample
        while True:
            sample_start = time.time()
            ls_coords = self.sampler.get_new_ls_points(1)
            sample_end = time.time() - sample_start
            LOGGER.info(f"Sampled 1 in {sample_end} seconds")
            self.add_time(f"sampled_{jobid}", sample_end)
            for pts in ls_coords:
                new_ls_coords.append(pts.get_coordinates())
                new_lambda.append(pts.get_lamda())
            num_new_sample += len(ls_coords)

            self.add_timestamp(f"generator_decode_{jobid}_start")
            new_positions = self.generator.decode(ls_coords)
            self.add_timestamp(f"generator_decode_{jobid}_complete")
            names_array, positions_array = write_patches(
                outpath=self.args.ml_outdir,
                iteration_id=jobid,
                new_positions=new_positions,
            )
            LOGGER.debug(f"generated structures done. new_positions = {new_positions.shape}")

            # These can be set multiple times, but we will always keep the last (successful)
            # valid sample. The entire process to get that is represented in total function time
            self.add_timestamp(f"validate_{jobid}_start")

            # Our jobid looks like structure_<number> and we need to pass just the number here
            # This isn't great, but I don't want to copy over all the validator code
            iteration_id = int(jobid.split("_")[-1])
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
            self.add_timestamp(f"validate_{jobid}_complete")
            if is_valid:
                break

        # Assume just return one
        return valid_files[0]


def push_artifact(path, name, tag, host, tls_verify=None, plain_http=None):
    """
    Push a named artifact to an OCI compliant registry
    """
    import mummi_operator.manager.registry as registry

    artifact = registry.RegistryArtifact()

    # The is the path and mediaType. I'm assuming this is a binary format
    artifact.add_archive(path, "application/octet-stream")
    artifact.summary()

    # Push to a URI that is cleaned / parsed.
    # registry-0.mini-mummi.default.svc.cluster.local:5000/structure_iter00_000000000388:latest
    uri = registry.generate_uri(host, name=name, tag=tag)
    LOGGER.info(f"Request to push {path} to oras registry {uri}")
    artifact.push(uri, tls_verify=tls_verify, plain_http=plain_http)
