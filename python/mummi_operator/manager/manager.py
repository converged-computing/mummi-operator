# The manager is intended to be run in a container (as a service) to orchestrate
# a workflow.

import logging
import math
import random

from kubernetes import client, config, watch

import mummi_operator.defaults as defaults
import mummi_operator.tracker as tracker
from mummi_operator.machine import new_mummi_job

# Print debug for now
logging.basicConfig()
LOGGER = logging.getLogger(__name__)
logging.root.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)


class WorkflowManager:
    def __init__(self, cfg, workflow, scheduler=defaults.default_scheduler):
        """
        Initialize the WorkflowManager. Much of this logic used to be in setup,
        but it makes sense to be on the class instance init. State is derived
        from the Kubernetes cluster, and a state machine is created for each
        job sequence.
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
        except Exception:
            config.load_config()

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

    @property
    def stages(self):
        """
        Return stages of workflow.
        """
        return list(self.workflow.jobs.keys())

    @property
    def active_sequences(self):
        """
        Get the number of active jobs.

        We don't want to submit new jobs over that.
        """
        # Get jobs that are in the first stage to determine sequences active
        jobs = tracker.list_jobs_by_status(label_value=self.stages[0])
        in_progress = 0
        active_jobids = set()

        # so we loop through fewer jobs here
        for job in jobs["running"] + jobs["queued"]:
            jobid = job.metadata.labels.get(defaults.operator_label)
            state_machine = self.trackers.get(jobid)
            if not jobid or not state_machine:
                continue

            # Assume the first step (active or completed) means we shouldnt
            # kick off another. We will need to eventually delete this chain
            # of jobs when the entire thing is done.
            if jobid not in self.active_jobids:
                in_progress += 1
                active_jobids.add(jobid)
        return in_progress

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
        jobs["active"] = jobs["running"] + jobs["queued"]

        # We assume that a job sequence that isn't failed is going toward
        # the successful completions. The set of ids can be refreshed
        self.completions = 0
        self.completed_jobids = set()

        # Case 1: The step is running or queued. This means we mark
        # All previous steps successful (assuming we cannot
        # transition if this was not the case)
        for state in ["active", "success"]:
            for job in jobs[state]:
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

                # The job is active, kick off the next steps
                if state == "active":
                    state_machine.mark_running(step_name)
                    self.trackers[jobid] = state_machine

                # Determine if this is a full completion
                elif state == "success":
                    if step_name == self.stages[-1] and jobid not in self.completed_jobids:
                        self.completions += 1
                        self.completed_jobids.add(jobid)

        LOGGER.info(f"Manager running with {self.completions} job sequence completions.")
        # TODO we likely want some logic to cleanup failed
        # But this might not always be desired

    def check_complete(self):
        """
        Check if the entire workflow is complete.

        Here we just exit, and don't stop jobs from running, but eventually
        we can cleanup, etc.
        """
        jobs_needed = self.workflow.completions_needed - self.completions
        if jobs_needed <= 0:
            LOGGER.info(
                "Workflow is complete - {self.completions}/{self.workflow.completions_needed} are done"
            )
            sys.exit(0)

    def new_jobs(self):
        """
        New jobs creates new jobs to track based on space available.

        This assumes that one sequence of steps takes up one cluster "slot"
        and that we can submit up to a maximum number of slots. This works
        well given that each job takes one node, but will need to be tweaked
        if that is not the case. TLDR: this algorithm that can be improved upon.
        """
        # These start at "start" stage (is_started should be false)
        # We will pack into the number nodes available
        step = self.workflow.config_for_step(self.workflow.first_step)
        nodes_needed = step.get("nnodes", 1)
        jobs_needed = self.workflow.completions_needed - self.completions
        submit_n = math.floor(self.workflow.max_size / nodes_needed)

        # Account for active sequences
        submit_n = submit_n - self.active_sequences

        # If submit is > than completions needed, we don't need that many
        # This is OK if submit_n is negative, a 0-> negative range is empty
        # TODO we would also downscale the cluster here
        submit_n = min(jobs_needed, submit_n)
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

        # You never know - we could restore and be done!
        self.check_complete()

        # At this point, we have 1:1 mapping of state machines to job sequences
        # We can now submit new simulations with the space we have. We assume
        # each sequence gets one job running at once (one slot in the cluster)
        # and can submit up to the max size. This algorithm can change.
        self.new_jobs()

        # Now we watch for changes.
        self.watch()

    def watch(self):
        """
        Watch is an event driven means to watch for changes and update job states
        accordingly.
        """
        # TODO we should have some kind of timeout that does not rely on an event
        v1 = client.CoreV1Api()
        batch_v1 = client.BatchV1Api()
        w = watch.Watch()
        for event in w.stream(batch_v1.list_namespaced_job, namespace=tracker.get_namespace()):
            job = event["object"]
            jobid = job.metadata.labels["jobid"]
            step_name = job.metadata.labels["app"]

            # Not a job associated with the workflow, or is ignored
            if not jobid or not step_name or jobid not in self.trackers:
                continue

            # Get the state machine for the job
            state_machine = self.trackers[jobid]

            # The job is active and not finished, keep going
            # This status will trigger when it's created (after submit)
            if job.status.active == 1 and job.status.completion_time is None:
                continue

            # The job just completed and ran successfully, trigger the next step
            if job.status.succeeded == 1 and job.status.completion_time is not None:
                LOGGER.info(f"Job {jobid} completed stage '{state_machine.current_state.id}'")
                state_machine.mark_succeeded()
                state_machine.change()

                # Check to see if we should submit new jobs
                self.new_jobs()

            # The job just completed and failed, clean up.
            if job.status.failed == 1 and job.status.completion_time is not None:
                LOGGER.info(f"Job {jobid} failed stage '{state_machine.current_state.id}'")
                # Marking a job failed deletes all Kubernetes objects associated across stages.
                # We do this because we assume no step should be retried, etc.
                state_machine.mark_failed()
                # Deleting the state machine means we stop tracking it
                del self.trackers[jobid]
                continue

            # TODO: this triggers on events, but we might want to also trigger the check at some frequency
            # This should work if a completion always runs it, however
            self.check_complete()
