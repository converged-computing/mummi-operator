# The manager is intended to be run in a container (as a service) to orchestrate
# a workflow.

import math
import multiprocessing
import os
import random
import signal
import sys
import timeit
import traceback
import uuid
from logging import getLogger
from multiprocessing.managers import SyncManager

import mummi_core
import mummi_ras
import pika
import yaml
from kubernetes import client, config, watch
from mummi_core.utils.timer import Timer
from mummi_core.workflow.flux_env import flux_uri
from mummi_core.workflow.job import JOB_NEXT_QUEUE, JOB_TYPES
from mummi_ras import Naming
from mummi_ras.feedback.feedback_manager_aatocg import FeedbackManager_AA2CG
from mummi_ras.feedback.feedback_manager_macro import FeedbackManagerType, MacroFeedbackManager
from mummi_ras.ml.selectors import CGSelector, CGSelectorType
from mummi_ras.transformations.patch_creator import MacroPatchCreator

import mummi_operator.defaults as defaults
from mummi_operator.config import load_config
from mummi_operator.machine import new_mummi_job

LOGGER = getLogger(__name__)

import mummi_operator.tracker as tracker


class WorkflowManager:
    def __init__(self, cfg, workflow, scheduler=defaults.default_scheduler):
        """
        Initialize the WorkflowManager. Much of this logic used to be in setup,
        but it makes sense to be on the class instance init.

        - mummi_core.init() is removed, we don't need to make a structure here.
          we generally should not be saving any state, it should be derived.
        - the workspace is also removed, we should get state from cluster.
        - multiprocessing/events are removed, will be a part of state machine.
        """
        self.config = cfg
        self.workflow = workflow

        # Job prefix (defaults to structure_, don't change)
        self.set_prefix()

        # Running modes (we only allow kubernetes for now)
        self.scheduler = scheduler
        LOGGER.info(f"  Scheduler: [{self.scheduler}]")

        if self.scheduler not in defaults.supported_schedulers:
            raise ValueError(
                f"{self.scheduler} is not valid, please choose from {defaults.supported_schedulers}"
            )

        # Load connection to kubernetes
        if self.scheduler == "kubernetes":
            self.load_kubernetes_config()

    @property
    def wconfig(self):
        return self.config["wfmanager"]["config"]

    def set_prefix(self):
        """
        Set a prefix for job identifiers
        """
        if "prefix" not in self.wconfig:
            self.wconfig["prefix"] = defaults.default_prefix

    @property
    def prefix(self):
        """
        The job prefix (defaults to mummi)
        """
        return self.wconfig.get("prefix") or "mummi"

    def load_kubernetes_config(self):
        """
        First try for in cluster config, then fall back to external.
        """
        try:
            config.load_incluster_config()
        except:
            config.load_config()

    def _init_cg_selection(self):
        self.cgselector = None

        if self.do_cgselection:
            ml_config = Naming.ml("cg")
            stype = ml_config["selection"]
            assert stype == "importance"

            wspace = os.path.join(Naming.dir_root("ml"), "cg")
            os.makedirs(wspace, exist_ok=True)
            self.cgselector = CGSelector(
                CGSelectorType.Manager, "manager", wspace, ml_config
            )
            self.cgselector.restore()

    def _init_feedback_cg2macro(self):
        self.fb_cg2macro = None

        # ----------------------------------------------------------------------
        if self.do_feedback_cg2mc:
            name = "CG2MacroFeedback_Manager"
            wspace = Naming.dir_root("feedback-cg")

            # should ideally use io interface (here and below)
            # outpath = self.iointerface.namespace('feedback-cg')
            # fbpath = self.fbinterface.namespace('feedback-cg')
            outpath = wspace
            fbpath = wspace

            do_weights = bool(self.wconfig["fbcg_do_wts"])
            pselector = self.pselector if do_weights else None

            # fdinatal -- removal of dbr due to hang
            # self.fbinterface.create_namespace(fbpath)
            self.fb_cg2macro = MacroFeedbackManager(
                FeedbackManagerType.Manager,
                name,
                wspace,
                outpath,
                fbpath,
                self.iointerface,
                self.fbinterface,
                pselector,
            )
            self.fb_cg2macro.restore()

        # ----------------------------------------------------------------------

    def _init_ml_server(self):
        """
        Initialize the client on the workflow side to talk to the ML Server.
        March 2023:
            Current implementation supports only RabbitMQ, currently the ML server connect to
            a RabbitMQ server running on a Kubernetes cluster (defined in the credentials).
            The ML server is responsible of sampling, generating and validating new structures
            for the wfmanager.
        """
        if self.do_mlserver:
            config = mummi_ras.get_named_specfile("mlserver.yaml")

            # Load paths from eval strings in config
            gdict = {"mummi_ras": mummi_ras}
            for key in ["workspace", "encoder"]:
                if type(config[key]["path"]) is dict and "eval" in config[key]["path"]:
                    config[key]["path"] = eval(config[key]["path"]["eval"], gdict)

            if config.get("encoder") is None or config["encoder"].get("path") is None:
                raise ValueError(f"No encoder specified in mlserver.yaml")
            if (
                config.get("workspace") is None
                or config["workspace"].get("path") is None
            ):
                raise ValueError(f"No workspace specified in mlserver.yaml")

            wspace = config["workspace"]["path"]
            credentials_path = os.path.join(wspace, config["workspace"]["credentials"])
            certificate_path = os.path.join(wspace, config["workspace"]["certificate"])

            usr = os.environ.get("USER", "mummiusr")
            routing_key = config["broker"]["queue"] + "_" + usr
            if usr == "mummiusr":
                LOGGER.warning(f"Did not find current user: defaulted to {usr}")
            broker_interface = config["broker"]["interface"]
            # NOTE rpc client used to be here
            LOGGER.info(f"> Initializing RPC client to interact with ML Server")
            LOGGER.info(f"  > Client")
            LOGGER.info(f"    > Interface   {broker_interface}")
            LOGGER.info(f"    > Credentials {credentials_path}")
            LOGGER.info(f"    > Certificate {certificate_path}")
            LOGGER.info(f"    > Routing key {routing_key}")

    # ------------------------------------------------------------------------
    # Main tasks to be done by the wf manager
    # ------------------------------------------------------------------------
    def _task_add_new_patches_to_ml(self, lock_patch_io, lock_patch_select):
        if not self.do_patchselection:
            LOGGER.debug(
                f"No patchselection as do_patchselection = {self.do_patchselection}"
            )
            return 0
        LOGGER.debug(f"Patch selection started")

        p = multiprocessing.current_process()

        # ----------------------------------------------------------------------
        patches = []
        if self.do_gc:
            patches = self.patch_creator.run_for_gc()

        else:
            patch_ids = [
                Naming.pfpatch(self.patchCounter + i)
                for i in range(self.nReadPatchesPerIter)
            ]

            if not lock_patch_io.acquire(False):  # non-blocking acquire
                LOGGER.debug(
                    "Failed to acquire lock on patches ({}, {})".format(p.name, p.pid)
                )
                return 0

            LOGGER.debug("Acquired lock on patches ({}, {})".format(p.name, p.pid))
            patches = self.iointerface.load_patches(self.ns_pfpatches, patch_ids)

            lock_patch_io.release()
            LOGGER.debug("Released lock on patches ({}, {})".format(p.name, p.pid))

        # ----------------------------------------------------------------------
        npatches = len(patches)
        if npatches == 0:
            return npatches

        LOGGER.debug("Acquiring lock on patch selector ({}, {})".format(p.name, p.pid))
        with lock_patch_select:
            LOGGER.debug(
                "Acquired lock on patch selector ({}, {})".format(p.name, p.pid)
            )
            self.pselector.add_candidates(patches)
        LOGGER.debug("Released lock on patch selector ({}, {})".format(p.name, p.pid))

        self.patch_creator.checkpoint()

        self.patchCounter += npatches
        LOGGER.info("Added {} patches to the selector".format(npatches))
        return npatches

    def _task_add_cgframes_to_ml(self):
        if not self.do_cgselection:
            LOGGER.debug(f"No cgselection as do_cgselection = {self.do_patchselection}")
            return 0
        LOGGER.debug(f"CG selection started")

        ctime = timeit.default_timer() - self.timeOfLastCgSelUpdate
        if ctime < self.nSecPerCgSelUpdate:
            return

        try:
            self.cgselector.update_manager()
        except Exception as e:
            LOGGER.error(f"Got error from cgselector.update_manager: {e}")
        self.timeOfLastCgSelUpdate = timeit.default_timer()

    # --------------------------------------------------------------------------
    def _task_select_pfpatches(self, lock_patch_select):
        if not self.do_patchselection:
            return 0

        nPendingPatches = (
            self.job_trackers["cg"].nqueued_sims()
            + self.job_trackers["createsim"].nqueued_sims()
            + self.job_trackers["createsim"].nrunning_sims()
        )

        LOGGER.debug(
            f"pending patches = {nPendingPatches} = {self.job_trackers['cg'].nqueued_sims()} "
            f"+ {self.job_trackers['createsim'].nqueued_sims()} "
            f"+ {self.job_trackers['createsim'].nrunning_sims()}"
        )
        nPatches = min(
            self.nMaxSelectedPatchBuffer - nPendingPatches,
            self.nMaxPatchesSelectionsPerIter,
        )

        LOGGER.debug(
            f"npatches = {nPatches} = min({self.nMaxSelectedPatchBuffer} "
            f"- {nPendingPatches}, {self.nMaxPatchesSelectionsPerIter}"
        )

        if nPatches <= 0:
            return 0

        # select new patches
        p = multiprocessing.current_process()
        LOGGER.debug("Acquiring lock on patch selector ({}, {})".format(p.name, p.pid))
        with lock_patch_select:
            LOGGER.debug(
                "Acquired lock on patch selector ({}, {})".format(p.name, p.pid)
            )
            LOGGER.info("Select {} new patches".format(nPatches))
            selections = self.pselector.select(nPatches)
            LOGGER.debug(
                "Released lock on patch selector ({}, {})".format(p.name, p.pid)
            )

        n = len(selections)
        if n == 0:
            return n

        # test these selections
        # Split a list of sims based on their status.
        # Returns: sims_success, sims_failed, sims_unknown
        _s, _f, _u, _stop = self.job_trackers["createsim"].split_sims_on_status(
            selections
        )

        # this is the correct scenario
        if len(_s) == 0 and len(_f) == 0:
            self.job_trackers["createsim"].add_to_queue(selections)

        # looks like some of these selections were already simulated
        else:
            LOGGER.error(
                "Found (%d %d) selected patches for which createsim was already run. "
                "Were you using old sims with new ml?",
                len(_s),
                len(_f),
            )
            self.job_trackers["createsim"].add_to_queue(_u)
            self.job_trackers["cg"].add_to_queue(_s)

        return n

    # --------------------------------------------------------------------------
    # Instead of using PatchSelector we query the ML Server for structures to
    # to give to createsim
    # --------------------------------------------------------------------------
    def _task_add_new_patches_mlserver(self):
        if not self.do_mlserver:
            return 0

        # Running and Queued CG represent a certain number of createsims that have been executed in the past
        # Running Createsims represent the current createsim running
        # Queued Createsims represent the future createsim that will be run at some point.
        nPendingPatches = (
            self.job_trackers["cg"].nqueued_sims()
            + self.job_trackers["createsim"].nqueued_sims()
            + self.job_trackers["createsim"].nrunning_sims()
        )

        LOGGER.info(
            f"pending patches = {nPendingPatches} = {self.job_trackers['cg'].nqueued_sims()} "
            f"+ {self.job_trackers['createsim'].nqueued_sims()} "
            f"+ {self.job_trackers['createsim'].nrunning_sims()}"
        )
        nPatches = min(
            self.nMaxSelectedPatchBuffer - nPendingPatches,
            self.nMaxPatchesSelectionsPerIter,
        )

        LOGGER.info(
            f"npatches => min({self.nMaxSelectedPatchBuffer} - {nPendingPatches}, "
            f"{self.nMaxPatchesSelectionsPerIter}) = {nPatches}"
        )

        # We have enough patches
        if nPatches <= 0:
            return 0

        # if nPatches is too small and the ML model success rate is low (<40%)
        # Then requesting 5 or 10 will never return valid patches statistically
        if nPatches < self.nMaxPatchesSelectionsPerIter:
            nPatches = self.nMaxPatchesSelectionsPerIter

        # select new patches
        p = multiprocessing.current_process()
        LOGGER.debug(f"> Running RPC client {p.name} on [{self.hostname}/{p.pid}]")
        LOGGER.info(
            f" [x] Requesting generate_new_samples(iter={self.iterCounterMLServer}, k={nPatches})"
        )
        # If strict equal True then call() will ALWAYS return the requested number of VALID patches (but it will take longer to run)
        selections = []
        try:
            selections = self.rpc_client.call(
                {
                    "k_samples": nPatches,
                    "iteration_id": self.iterCounterMLServer,
                    "strict": False,
                }
            )
            if isinstance(selections, str):
                LOGGER.error(f"[.] RPC server sent back an exception: {selections}")
                self.checkpoint()
                # self.stop()
                return -1
            selections = [x.split("/")[-1] for x in selections]
            LOGGER.info(
                f" [.] Got generate_new_samples(iter={self.iterCounterMLServer}, k={nPatches}) = {selections}"
            )
            for sample in selections:
                if sample in self.generated_patches:
                    LOGGER.error(
                        f"Sample {sample} has already been generated. This will cause trouble. {sample} should be removed."
                    )
            self.generated_patches += selections
        except pika.exceptions.AMQPError as e:
            # we try to restart the connection
            LOGGER.warning(f"We got {e}")
            LOGGER.info(f"We try to reinitialize the connection with ML server")
            self._init_ml_server()
            LOGGER.info(f"Connection to ML server reinitialized.")
            return 0

        n = len(selections)
        if n == 0:
            return n

        # test these selections
        # Split a list of sims based on their status.
        # Returns: sims_success, sims_failed, sims_unknown, sims_stop
        sims_success, sims_failed, sims_unknown, sims_stop = self.job_trackers[
            "createsim"
        ].split_sims_on_status(selections)

        LOGGER.debug(
            f"Found sims_success = {sims_success}, sims_failed = {sims_failed}, sims_unknown= {sims_unknown}, sims_unknown= {sims_stop}"
        )

        # this is the correct scenario (the new patches have not been explored yet)
        if len(sims_success) == 0 and len(sims_failed) == 0:
            self.job_trackers["createsim"].add_to_queue(selections)

        # looks like some of these selections were already simulated
        else:
            LOGGER.error(
                "Found (%d %d) selected patches for which createsim was already run. "
                "Were you using old sims with new ML?",
                len(sims_success),
                len(sims_failed),
            )
            self.job_trackers["createsim"].add_to_queue(sims_unknown)
            self.job_trackers["cg"].add_to_queue(sims_success)

        return n

    def _task_add_new_patches_ucgserver(self):
        if not self.do_ucgserver:
            return 0

        # Running and Queued CG represent a certain number of createsims that have been executed in the past
        # Running Createsims represent the current createsim running
        # Queued Createsims represent the future createsim that will be run at some point.
        nPendingPatches = (
            self.job_trackers["cg"].nqueued_sims()
            + self.job_trackers["createsim"].nqueued_sims()
            + self.job_trackers["createsim"].nrunning_sims()
        )

        LOGGER.info(
            f"pending patches = {nPendingPatches} = {self.job_trackers['cg'].nqueued_sims()} "
            f"+ {self.job_trackers['createsim'].nqueued_sims()} "
            f"+ {self.job_trackers['createsim'].nrunning_sims()}"
        )
        nPatches = min(
            self.nMaxSelectedPatchBuffer - nPendingPatches,
            self.nMaxPatchesSelectionsPerIter,
        )

        LOGGER.info(
            f"npatches => min({self.nMaxSelectedPatchBuffer} - {nPendingPatches}, "
            f"{self.nMaxPatchesSelectionsPerIter} = {nPatches})"
        )

        if nPatches <= 0:
            return 0

        # select new patches
        p = multiprocessing.current_process()
        LOGGER.debug(f"> Running RPC client {p.name} on [{self.hostname}/{p.pid}]")
        LOGGER.info(
            f" [x] Requesting generate_new_samples(iter={self.iterCounterUCGServer}, k={nPatches})"
        )
        # If strict equal True then call() will always return the requested number of VALID patches (but it will take longer to run)
        selections = []
        try:
            selections = self.rpc_client.call(
                {
                    "k_samples": nPatches,
                    "iteration_id": self.iterCounterUCGServer,
                    "strict": False,
                }
            )
            if isinstance(selections, str):
                LOGGER.error(f"[.] RPC server sent back an exception: {selections}")
                self.checkpoint()
                # self.stop()
                return -1
            LOGGER.info(
                f" [.] Got generate_new_samples(iter={self.iterCounterUCGServer}, k={nPatches}) = {selections}"
            )
            selections = [x.split("/")[-1] for x in selections]

            for sample in selections:
                if sample in self.generated_patches:
                    LOGGER.warning(
                        f"Sample {sample} has already been generated. This will cause trouble. {sample} should be removed."
                    )
            self.generated_patches += selections
        except pika.exceptions.AMQPError as e:
            # we try to restart the connection
            LOGGER.warning(f"We got {e}")
            LOGGER.info(f"We try to reinitialize the connection with UCG server")
            self._init_ucg_server()
            LOGGER.info(f"Connection to UCG server reinitialized.")
            return 0

        n = len(selections)
        if n == 0:
            return n

        # test these selections
        # Split a list of sims based on their status.
        # Returns: sims_success, sims_failed, sims_unknown
        sims_success, sims_failed, sims_unknown, _ = self.job_trackers[
            "createsim"
        ].split_sims_on_status(selections)

        LOGGER.debug(
            f"Found sims_success = {sims_success}, sims_failed = {sims_failed}, sims_unknown= {sims_unknown}"
        )

        # this is the correct scenario (the new patches have not been explored yet)
        if len(sims_success) == 0 and len(sims_failed) == 0:
            self.job_trackers["createsim"].add_to_queue(selections)

        # looks like some of these selections were already simulated
        else:
            LOGGER.error(
                "Found (%d %d) selected patches for which createsim was already run. "
                "Were you using old sims with new ML?",
                len(sims_success),
                len(sims_failed),
            )
            self.job_trackers["createsim"].add_to_queue(sims_unknown)
            self.job_trackers["cg"].add_to_queue(sims_success)

        return n

    # --------------------------------------------------------------------------
    def _task_select_cgframes(self):
        if not self.do_cgselection:
            return 0

        nPendingFrames = (
            self.job_trackers["aa"].nqueued_sims()
            + self.job_trackers["backmapping"].nqueued_sims()
            + self.job_trackers["backmapping"].nrunning_sims()
        )

        LOGGER.debug(
            f"pending frames = {nPendingFrames} = {self.job_trackers['aa'].nqueued_sims()} "
            f"+ {self.job_trackers['backmapping'].nqueued_sims()} "
            f"+ {self.job_trackers['backmapping'].nrunning_sims()}"
        )

        nFrames = min(
            self.nMaxSelectedCGFrameBuffer - nPendingFrames,
            self.nMaxCGFramesSelectionsPerIter,
        )

        LOGGER.debug(
            f"nframes = {nFrames} = min({self.nMaxSelectedCGFrameBuffer} "
            f"- {nPendingFrames}, {self.nMaxCGFramesSelectionsPerIter}"
        )

        if nFrames <= 0:
            return 0

        # select new frames
        LOGGER.info("Select {} new cg frames".format(nFrames))
        selections = self.cgselector.select(nFrames)

        n = len(selections)
        LOGGER.info("Selected {} cg frames".format(n))
        if n == 0:
            return n

        selections = [s.id for s in selections]

        # test these selections
        # Split a list of sims based on their status.
        # Returns: sims_success, sims_failed, sims_unknown, sims_stop
        _s, _f, _u, _ = self.job_trackers["backmapping"].split_sims_on_status(
            selections
        )

        # this is the correct scenario
        if len(_s) == 0 and len(_f) == 0:
            self.job_trackers["backmapping"].add_to_queue(selections)

        # looks like some of these selections were already simulated
        else:
            LOGGER.error(
                "Found (%d %d) selected patches for which createsim was already run. "
                "Were you using old sims with new ml?",
                len(_s),
                len(_f),
            )
            self.job_trackers["backmapping"].add_to_queue(_u)
            self.job_trackers["aa"].add_to_queue(_s)

        return n

    # --------------------------------------------------------------------------
    def _task_update_jobs(self):
        succeeded = {}
        failed = {}
        started = {}

        # ----------------------------------------------------------------------
        # first, we will update job tracker
        for j in JOB_TYPES:
            succeeded[j], failed[j] = self.job_trackers[j].update()
            LOGGER.debug(
                f"   JOB_TYPES={j} and self.job_trackers[{j}] = {self.job_trackers[j]}"
            )

        LOGGER.debug("succeeded = {} and failed = {}".format(succeeded, failed))

        # ----------------------------------------------------------------------
        # add the finished jobs to the next queue!
        for j in JOB_NEXT_QUEUE.keys():
            fjobs = succeeded[j]
            if len(fjobs) > 0:
                LOGGER.debug(
                    "Found {} successful {} sims. next, queue them to {}!".format(
                        len(fjobs), j, JOB_NEXT_QUEUE[j]
                    )
                )
                self.job_trackers[JOB_NEXT_QUEUE[j]].add_to_queue(fjobs)

        LOGGER.debug("JOB_NEXT_QUEUE = {}".format(JOB_NEXT_QUEUE.keys()))
        # ----------------------------------------------------------------------
        # finally, we will start any new jobs!
        LOGGER.debug(f"JOB_TYPES = {JOB_TYPES} started = {started}")
        for j in JOB_TYPES:
            LOGGER.debug(
                f"   JOB_TYPES={j} and self.job_trackers[{j}] = {self.job_trackers[j]}"
            )
            nJobs, started[j] = self.job_trackers[j].start_jobs(self.nMaxJobsPerIter)
            LOGGER.debug(f"   nJobs={nJobs}, started[{j}]={started[j]}")

        LOGGER.debug(f"started = {started}, succeeded = {succeeded}, failed = {failed}")
        # ----------------------------------------------------------------------
        # return the dictionaries?
        return started, succeeded, failed

    def generate_id(self):
        """
        Generate a job id
        """
        number = random.choice(range(0, 99999999))
        jobid = self.prefix + str(number).zfill(9)
        # This is hugely unlikely to happen, but you never know!
        if jobid in self.trackers:
            return self.generate_id()
        return jobid

    def init_state(self):
        """
        Look at the state of the cluster and initialize trackers to match it.
        """
        self.trackers = {}

        # Determine current state of cluster, create state machine for each job
        # Note this will return steps from across a single state machine. If job:
        #    Successful (at the end) we have the result pushed
        #    Failed we won't continue (and shouldn't make a state machine
        #    Unknown (this shouldn't happen, let's show these)
        #    Running: we assume previous steps successful
        jobs = tracker.list_jobs_by_status()
        print("TODO CHECK LOGIC FOR LIST JOBS BY STATUS")
        import IPython

        IPython.embed()

        # Case 1: The step is running or queued. This means we mark
        # All previous steps successful (assuming we cannot
        # transition if this was not the case)
        for job in jobs["running"] + jobs["queued"]:
            jobid = job.metadata.labels.get(defaults.operator_label)
            step_name = job.metadata.labels["app"]

            # We cannot monitor a job that we didn't submit
            # All jobs we submit have an id and step name
            if not jobid or not step_name:
                continue

            # Get existing or new state machine for it
            if jobid in self.trackers:
                state_machine = self.trackers[jobid]
            else:
                state_machine = new_mummi_job(self.workflow, jobid)()
            state_machine.mark_running(step_name)
            self.trackers[jobid] = state_machine

        # TODO we likely want some logic to cleanup failed
        # But this might not always be desired

    def init_jobs(self):
        """
        Init jobs creates new jobs to track based on space available.

        This assumes that one sequence of steps takes up one cluster "slot"
        and that we can submit up to a maximum number of slots. This works
        well given that each job takes one node, but will need to be tweaked
        if that is not the case. TLDR: this algorithm that can be improved upon.
        """
        # These start at "start" stage (is_started should be false)
        # We will pack into the number nodes available
        step = self.workflow.config_for_step(self.workflow.first_step)
        nodes_needed = step.get("nnodes", 1)
        submit_n = math.floor(self.workflow.max_size / nodes_needed)
        for i in range(0, submit_n):
            jobid = self.generate_id()

            # Create a new state machine with job trackers, and change
            # change goes into the first state (the first step to submit)
            state_machine = new_mummi_job(self.workflow, jobid)()
            state_machine.change()
            self.trackers[jobid] = state_machine

    def start(self):
        """
        Start the workflow manager state machine.

        This previously was run_workflow. Simple algorithm to start:

        1. Populate state machines that match current cluster.
           One state machine is a sequence of jobs. We only care about
           queued and running jobs. Any failure of a job will not continue
           and we don't need to track or care about it (we should cleanup)
        2. Submit new jobs up to a max allowed scaling size.
           This coincides with new state machines, one per submit.
        3. Monitor for changes by watching events.
        """
        # Each tracker is a state machine for one job sequence
        # Here we assess the current state of the cluster (jobs)
        # and fill the self.trackers lookup with state machines
        self.init_state()

        # At this point, we have 1:1 mapping of state machines to job sequences
        # We can now submit new simulations with the space we have. We assume
        # each sequence gets one job running at once (one slot in the cluster)
        # and can submit up to the max size. This algorithm can change.
        self.init_jobs()

        # Now we watch for changes.
        self.watch()

    def watch(self):
        """
        Watch is an event driven means to watch for changes and update job states
        accordingly.
        """
        print("TODO WATCH EVENTS")
        import IPython

        IPython.embed()

        # TODO we should have some kind of timeout that does not rely on an event
        v1 = client.CoreV1Api()
        batch_v1 = client.BatchV1Api()
        w = watch.Watch()
        for event in w.stream(
            batch_v1.list_namespaced_job, namespace=tracker.get_namespace()
        ):
            job = event["object"]
            print(event)
            import IPython

            IPython.embed()

    # --------------------------------------------------------------------------
    # patch creation task
    # --------------------------------------------------------------------------
    def run_patch_creation(self, n, lock_patch_io):
        p = multiprocessing.current_process()
        signal.signal(signal.SIGTERM, self.signal_wrapper("patch_creator", p.pid))

        # ----------------------------------------------------------------------
        try:
            self._wf_ready.wait()
            self._init_patch_creation()

            slp_time = 0
            loop_timer = Timer()
            while not self._exit.wait(slp_time):
                LOGGER.info(
                    "Starting {} iteration {}".format(p.name, self.iterCounterPC)
                )
                LOGGER.profile(
                    "Starting {} iteration {}".format(p.name, self.iterCounterPC)
                )

                loop_timer.start()

                # --------------------------------------------------------------

                patches = self.patch_creator.run_for_macro()

                # if there were some patches created!
                if len(patches) > 0:
                    # write them (use blocking acquire of lock)
                    with lock_patch_io:
                        LOGGER.debug(
                            "Acquired lock on patches ({}, {})".format(p.name, p.pid)
                        )
                        self.iointerface.save_patches(
                            Naming.dir_root("patches"), patches
                        )
                        self.patch_creator.checkpoint()
                    LOGGER.debug(
                        "Released lock on patches ({}, {})".format(p.name, p.pid)
                    )

                # --------------------------------------------------------------
                LOGGER.info(
                    "{} iteration {} finished: {} {}".format(
                        p.name, self.iterCounterPC, self.is_exit(), self.is_error()
                    )
                )

                # --------------------------------------------------------------
                loop_time = loop_timer.elapsed()
                self.iterCounterPC += 1

                slp_time = max(0, self.nSecPerIterPC - loop_time)
                LOGGER.info("{} waiting for {} seconds".format(p.name, slp_time))

            LOGGER.debug("AFTER LOOP: Patch creator loop ending.")

        # ----------------------------------------------------------------------
        except Exception as e:
            # self.exception_queue.put(sys.exc_info())
            traceback.print_exc()
            self.error()
            self.exit()
            raise e

        # ----------------------------------------------------------------------
        if self.is_error():
            LOGGER.info("{} process is exiting due to error flag".format(p.name))
        elif self.is_exit():
            LOGGER.info("{} process is exiting due to exit flag".format(p.name))

    # --------------------------------------------------------------------------
    # feedback tasks
    # --------------------------------------------------------------------------
    def run_feedback_cg2macro(self, n, lock_patch_select):
        if not self.do_feedback_cg2mc:
            return

        p = multiprocessing.current_process()
        signal.signal(signal.SIGTERM, self.signal_wrapper("feedback_cg2macro", p.pid))

        # ----------------------------------------------------------------------
        try:
            self._wf_ready.wait()
            self._init_feedback_cg2macro()

            slp_time = 0
            loopTimer = Timer()
            while not self._exit.wait(slp_time):
                LOGGER.info(
                    "Starting {} iteration {}".format(
                        p.name, self.iterCounterFB_cg2macro
                    )
                )
                LOGGER.profile(
                    "Starting {} iteration {}".format(
                        p.name, self.iterCounterFB_cg2macro
                    )
                )
                loopTimer.start()

                # --------------------------------------------------------------
                self.fb_cg2macro.aggregate(lock_patch_selector=lock_patch_select)
                self.fb_cg2macro.report()
                self.fb_cg2macro.checkpoint()

                # --------------------------------------------------------------
                LOGGER.info(
                    "{} iteration {} finished: {} {}".format(
                        p.name,
                        self.iterCounterFB_cg2macro,
                        self.is_exit(),
                        self.is_error(),
                    )
                )

                # --------------------------------------------------------------
                loop_time = loopTimer.elapsed()
                self.iterCounterFB_cg2macro += 1

                slp_time = max(0, self.nSecPerMacroFeedback - loop_time)
                LOGGER.info("{} waiting for {} seconds".format(p.name, slp_time))

            LOGGER.debug("AFTER LOOP: Feedback[CG2Macro] loop ending.")

        # ----------------------------------------------------------------------
        except Exception as e:
            # self.exception_queue.put(sys.exc_info())
            traceback.print_exc()
            self.error()
            self.exit()
            raise e

        # ----------------------------------------------------------------------
        if self.is_error():
            LOGGER.info("{} process is exiting due to error flag".format(p.name))
        elif self.is_exit():
            LOGGER.info("{} process is exiting due to exit flag".format(p.name))
