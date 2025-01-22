# The manager is intended to be run in a container (as a service) to orchestrate
# a workflow.

import os
import signal
import sys
import yaml
import timeit
import traceback
import multiprocessing
from multiprocessing.managers import SyncManager
import pika

import mummi_core
import mummi_operator.defaults as defaults
from mummi_core.workflow.job import JOB_TYPES, JOB_NEXT_QUEUE
from mummi_core.workflow.flux_env import flux_uri
from mummi_core.utils.timer import Timer

from kubernetes import config

import mummi_ras
from mummi_ras import Naming
from mummi_ras.transformations.patch_creator import MacroPatchCreator
from mummi_ras.ml.selectors import CGSelector, CGSelectorType
from mummi_ras.feedback.feedback_manager_macro import MacroFeedbackManager, FeedbackManagerType
from mummi_ras.feedback.feedback_manager_aatocg import FeedbackManager_AA2CG

from logging import getLogger

LOGGER = getLogger(__name__)

# We need to import the KubernetesTracker to submit jobs as CRDs
# This should (could) eventually be part of mummi_core, for now
# it's an experiment here
from mummi_operator.tracker import KubernetesTracker as Tracker


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
class WorkflowManager:
    def __init__(self, cfg, scheduler=defaults.default_scheduler):
        """
        Initialize the WorkflowManager. Much of this logic used to be in setup,
        but it makes sense to be on the class instance init.

        - mummi_core.init() is removed, we don't need to make a structure here.
          we generally should not be saving any state, it should be derived.
        - the workspace is also removed, we should get state from cluster.
        - multiprocessing/events are removed, will be a part of state machine.
        """
        self.config = cfg

        # Running modes (we only allow kubernetes for now)
        self.do_workflow = bool(self.wconfig["do_workflow"])
        self.do_schedulejobs = bool(self.wconfig["do_schedulejobs"])
        self.scheduler = scheduler
        LOGGER.info(f"  Scheduler: [{self.scheduler}]")

        if self.scheduler not in defaults.supported_schedulers:
            raise ValueError(
                f"{self.scheduler} is not valid, please choose from {defaults.supported_schedulers}"
            )

        # Load connection to kubernetes
        if self.scheduler == "kubernetes":
            self.load_kubernetes_config()

        # Init the state machine with the jobs
        self._init_state_machine()

    @property
    def wconfig(self):
        return self.config["wfmanager"]["config"]

    def load_kubernetes_config(self):
        """
        First try for in cluster config, then fall back to external.
        """
        try:
            config.load_incluster_config()
        except:
            config.load_config()

    # def _init_patch_selection(self):

    #     self.pselector = None
    #     if self.do_patchselection or self.do_feedback_cg2mc:
    #         ml_config = Naming.ml('macro')
    #         stype = ml_config['selection']
    #         assert stype in ['importance', 'random']

    #         wspace = os.path.join(Naming.dir_root('ml'), 'macro')
    #         self.pselector = self.proxy_manager.PatchSelector(stype, wspace, ml_config)
    #         self.pselector.restore()

    def _init_state_machine(self):
        """
        Create the state machine
        """
        print('state machine')
        import IPython
        IPython.embed()
        sys.exit()
        
    def _init_cg_selection(self):
        self.cgselector = None

        if self.do_cgselection:
            ml_config = Naming.ml("cg")
            stype = ml_config["selection"]
            assert stype == "importance"

            wspace = os.path.join(Naming.dir_root("ml"), "cg")
            os.makedirs(wspace, exist_ok=True)
            self.cgselector = CGSelector(CGSelectorType.Manager, "manager", wspace, ml_config)
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
            if config.get("workspace") is None or config["workspace"].get("path") is None:
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

    # --------------------------------------------------------------------------
    def checkpoint(self):
        state = dict(
            flux=self.flux,
            iterCounterWF=self.iterCounterWF,
            iterCounterPC=self.iterCounterPC,
            patchCounter=self.patchCounter,
            jobs_createsim=self.job_trackers["createsim"].status(),
            jobs_backmapping=self.job_trackers["backmapping"].status(),
            jobs_cg=self.job_trackers["cg"].status(),
            jobs_aa=self.job_trackers["aa"].status(),
        )

        # We keep the two last checkpoints
        self.iointerface.save_checkpoint(
            self.chkpt, state, use_tstamp=True, cleanup=True, keep_checkpoint=2
        )

    def restore(self):
        print("TODO RESTORE")
        import IPython

        IPython.embed()

        state = self.iointerface.load_checkpoint(self.chkpt, loader=yaml.UnsafeLoader)
        if len(state) == 0:
            return

        LOGGER.info(f"Restoring workflow as of {state['ts']}")
        sys.stdout.flush()

        # restore the data
        self.iterCounterWF = state["iterCounterWF"]
        self.iterCounterPC = state["iterCounterPC"]
        self.patchCounter = state["patchCounter"]

        # Assume kubernetes is always warm (same cluster)
        warm_restart = True if self.scheduler == "kubernetes" else False
        if self.flux is not None:
            prev_flux = state["flux"]
            warm_restart = self.flux == prev_flux
            wstring = "Warm" if warm_restart else "Cold"
            LOGGER.info(f"{wstring} restart of the Workflow! flx={self.flux}, prev_flx={prev_flux}")

        sims_success = {}
        sims_failed = {}
        for j in JOB_TYPES:
            job_state = state["jobs_" + j]
            sims_success[j], sims_failed[j] = self.job_trackers[j].restore(job_state, warm_restart)
            LOGGER.info(self.job_trackers[j].__str__())

        # if any jobs were found successful finished, need to queue for the next step!
        for j in JOB_NEXT_QUEUE.keys():
            fjobs = sims_success[j]
            if len(fjobs) > 0:
                LOGGER.info(f"Found {len(fjobs)} successful {j} sims!")
                self.job_trackers[JOB_NEXT_QUEUE[j]].add_to_queue(fjobs)

        LOGGER.info(
            f"Restored WFManager from {state['ts']}. "
            f"iterCounterWF = {self.iterCounterWF}, "
            f"iterCounterPC = {self.iterCounterPC}, "
            f"patchCounter = {self.patchCounter}"
        )

    # ------------------------------------------------------------------------
    # Main tasks to be done by the wf manager
    # ------------------------------------------------------------------------
    def _task_add_new_patches_to_ml(self, lock_patch_io, lock_patch_select):
        if not self.do_patchselection:
            LOGGER.debug(f"No patchselection as do_patchselection = {self.do_patchselection}")
            return 0
        LOGGER.debug(f"Patch selection started")

        p = multiprocessing.current_process()

        # ----------------------------------------------------------------------
        patches = []
        if self.do_gc:
            patches = self.patch_creator.run_for_gc()

        else:
            patch_ids = [
                Naming.pfpatch(self.patchCounter + i) for i in range(self.nReadPatchesPerIter)
            ]

            if not lock_patch_io.acquire(False):  # non-blocking acquire
                LOGGER.debug("Failed to acquire lock on patches ({}, {})".format(p.name, p.pid))
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
            LOGGER.debug("Acquired lock on patch selector ({}, {})".format(p.name, p.pid))
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
            self.nMaxSelectedPatchBuffer - nPendingPatches, self.nMaxPatchesSelectionsPerIter
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
            LOGGER.debug("Acquired lock on patch selector ({}, {})".format(p.name, p.pid))
            LOGGER.info("Select {} new patches".format(nPatches))
            selections = self.pselector.select(nPatches)
            LOGGER.debug("Released lock on patch selector ({}, {})".format(p.name, p.pid))

        n = len(selections)
        if n == 0:
            return n

        # test these selections
        # Split a list of sims based on their status.
        # Returns: sims_success, sims_failed, sims_unknown
        _s, _f, _u, _stop = self.job_trackers["createsim"].split_sims_on_status(selections)

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
            self.nMaxSelectedPatchBuffer - nPendingPatches, self.nMaxPatchesSelectionsPerIter
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
                {"k_samples": nPatches, "iteration_id": self.iterCounterMLServer, "strict": False}
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
            self.nMaxSelectedPatchBuffer - nPendingPatches, self.nMaxPatchesSelectionsPerIter
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
                {"k_samples": nPatches, "iteration_id": self.iterCounterUCGServer, "strict": False}
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
            self.nMaxSelectedCGFrameBuffer - nPendingFrames, self.nMaxCGFramesSelectionsPerIter
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
        _s, _f, _u, _ = self.job_trackers["backmapping"].split_sims_on_status(selections)

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
            LOGGER.debug(f"   JOB_TYPES={j} and self.job_trackers[{j}] = {self.job_trackers[j]}")

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
            LOGGER.debug(f"   JOB_TYPES={j} and self.job_trackers[{j}] = {self.job_trackers[j]}")
            nJobs, started[j] = self.job_trackers[j].start_jobs(self.nMaxJobsPerIter)
            LOGGER.debug(f"   nJobs={nJobs}, started[{j}]={started[j]}")

        LOGGER.debug(f"started = {started}, succeeded = {succeeded}, failed = {failed}")
        # ----------------------------------------------------------------------
        # return the dictionaries?
        return started, succeeded, failed

    def __init_machine():
        """
        Initialize the state machine
        """
        TODO

    def start(self):
        """
        Start the workflow manager

        This previously was run_workflow.
        """
        self._init_machine()
        try:
            # TODO need to sumit mlserver jobs
            # this was previously sent like             selections = self.rpc_client.call({'k_samples': nPatches, 'iteration_id': self.iterCounterMLServer, 'strict': False}), just a number and then get back a simulation ID
            # TODO need to implement this
            self.restore()

            previously_ckpt_jobs = 0
            for j in JOB_TYPES:
                previously_ckpt_jobs += (
                    self.job_trackers[j].nrunning_jobs() + self.job_trackers[j].nqueued_sims()
                )

            if previously_ckpt_jobs == 0:
                LOGGER.info(
                    f"No previously checkpointed jobs found (={previously_ckpt_jobs}) in checkpoints, starting workflow"
                )
                self._wf_ready.set()

            slp_time = 0
            loop_timer = Timer()
            while not self._exit.wait(slp_time):
                LOGGER.info("Starting {} iteration {}".format(p.name, self.iterCounterWF))
                LOGGER.profile("Starting {} iteration {}".format(p.name, self.iterCounterWF))

                loop_timer.start()

                # --------------------------------------------------------------
                # in the restore phase, let's focus only on starting the jobs
                if not self._wf_ready.is_set():
                    njobs_remaining = 0
                    for j in JOB_TYPES:
                        self.job_trackers[j].start_jobs(self.nMaxJobsPerIter)
                        njobs_remaining += self.job_trackers[j].njobs_2start()

                    if njobs_remaining <= 0:
                        LOGGER.info("Concluding restore phase")
                        self._wf_ready.set()  # kick off other processes

                # --------------------------------------------------------------
                # otherwise, we need to do the regular tasks
                else:
                    # ----------------------------------------------------------
                    # task 1: read new patches/cg frames and add to ML selectors
                    # Option 1. using ML (Latent Space) server to generate and select patches
                    self._task_add_new_patches_mlserver()
                    # Option 2. Using UCG server to generate and select patches
                    self._task_add_new_patches_ucgserver()
                    # Option 3. Using PatchCreator with macro model to create patches (these patches will be selected below)
                    self._task_add_new_patches_to_ml(lock_patch_io, lock_patch_select)

                    LOGGER.profile(
                        "    > Iteration {}: added new patches".format(self.iterCounterWF)
                    )
                    self._task_add_cgframes_to_ml()
                    if self.do_cgselection:
                        LOGGER.profile(
                            "    > Iteration {}: added CG frame patches".format(self.iterCounterWF)
                        )

                    # ----------------------------------------------------------
                    # 2021.02.07: HB moved this task up
                    # task 2: start the jobs
                    self._task_update_jobs()
                    LOGGER.profile("    > Iteration {}: updated jobs".format(self.iterCounterWF))

                    # ----------------------------------------------------------
                    # task 3: select new candidates for simulations
                    self._task_select_pfpatches(lock_patch_select)
                    if self.do_patchselection:
                        LOGGER.profile(
                            "    > Iteration {}: selected patches".format(self.iterCounterWF)
                        )

                    self._task_select_cgframes()
                    if self.do_cgselection:
                        LOGGER.profile(
                            "    > Iteration {}: select CG frames".format(self.iterCounterWF)
                        )

                    # ----------------------------------------------------------
                    # validate state and checkpoint
                    if self.do_patchselection:
                        self.pselector.test()
                        LOGGER.profile(
                            "    > Iteration {}: patch selector test".format(self.iterCounterWF)
                        )
                    if self.do_cgselection:
                        self.cgselector.test()
                        LOGGER.profile(
                            "    > Iteration {}: CG selector test".format(self.iterCounterWF)
                        )

                # --------------------------------------------------------------
                for j in JOB_TYPES:
                    self.job_trackers[j].test()
                LOGGER.profile("    > Iteration {}: testing all jobs".format(self.iterCounterWF))
                self.checkpoint()

                # ------------------------------------------------------------------
                LOGGER.info(
                    "{} iteration {} finished: {} {}".format(
                        p.name, self.iterCounterWF, self.is_exit(), self.is_error()
                    )
                )

                # --------------------------------------------------------------
                loop_time = loop_timer.elapsed()
                self.iterCounterWF += 1

                # slp_time = 0 # max(0, self.nSecPerIterWF - loop_time)
                slp_time = max(0, self.nSecPerIterWF - loop_time)
                LOGGER.debug("{} waiting for {} seconds".format(p.name, slp_time))

            LOGGER.debug("AFTER LOOP: Workflow loop ending.")

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
                LOGGER.info("Starting {} iteration {}".format(p.name, self.iterCounterPC))
                LOGGER.profile("Starting {} iteration {}".format(p.name, self.iterCounterPC))

                loop_timer.start()

                # --------------------------------------------------------------

                patches = self.patch_creator.run_for_macro()

                # if there were some patches created!
                if len(patches) > 0:
                    # write them (use blocking acquire of lock)
                    with lock_patch_io:
                        LOGGER.debug("Acquired lock on patches ({}, {})".format(p.name, p.pid))
                        self.iointerface.save_patches(Naming.dir_root("patches"), patches)
                        self.patch_creator.checkpoint()
                    LOGGER.debug("Released lock on patches ({}, {})".format(p.name, p.pid))

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
                LOGGER.info("Starting {} iteration {}".format(p.name, self.iterCounterFB_cg2macro))
                LOGGER.profile(
                    "Starting {} iteration {}".format(p.name, self.iterCounterFB_cg2macro)
                )
                loopTimer.start()

                # --------------------------------------------------------------
                self.fb_cg2macro.aggregate(lock_patch_selector=lock_patch_select)
                self.fb_cg2macro.report()
                self.fb_cg2macro.checkpoint()

                # --------------------------------------------------------------
                LOGGER.info(
                    "{} iteration {} finished: {} {}".format(
                        p.name, self.iterCounterFB_cg2macro, self.is_exit(), self.is_error()
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


def load_jobs(config_dir, wfconfig, job_configs):
    """
    Load jobs into the workflow manager and ensure configs exist.
    """
    # As of Python 3.7, dictionaries are ordered
    lookup = {}
    for job_config in job_configs:
        # This will fail if config is not found
        job = load_config(config_dir, job_config)

        # The "job_type" is the name (e.g., createsim)
        lookup[job["job_type"]] = job

    # Add the jobs to the lookup
    wfconfig["jobs"] = lookup
