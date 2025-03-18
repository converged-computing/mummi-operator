#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights
# reserved. LLNL-CODE-827197. This work was produced at the Lawrence Livermore
# National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44)
# between the U.S. Department of Energy (DOE) and Lawrence Livermore National
# Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers,
# notice of U.S. Government Rights and license terms and conditions.
# -----------------------------------------------------------------------------

# This is a modified variant of the JobTracker provided by mummi-core.
# We use this to replace the JobTracker and instead submit jobs to usernetes.

import json
import os
from enum import Enum
from itertools import count
from logging import getLogger
from typing import List

# These are added just for maestro and the custom adapter
from maestrowf.abstracts.interfaces import SchedulerScriptAdapter
from maestrowf.interfaces.script import SubmissionRecord
from mummi_core.workflow.job import SimulationStatus
from mummi_core.workflow.jobTracker import JobTracker

LOGGER = getLogger(__name__)

from kubernetes import client, config

# This assumes the wfmanager running inside the cluster
config.load_incluster_config()

# This would assume external to it
# config.load_kube_config()
true_options = ["true", True, "1", 1]


# We need to handle conflict
class SubmissionCode(Enum):
    OK = 0
    ERROR = 1
    CONFLICT = 2


class CancelCode(Enum):
    OK = 0
    ERROR = 1


class KubernetesScriptAdapter(SchedulerScriptAdapter):
    """Interface class for Kubernetes."""

    key = "kubernetes"

    def __init__(self, **kwargs):
        """
        Initialize an instance of the KubernetesScriptAdapter

        This adapter is intended to submit jobs to Kubernetes. Instead
        of writing scripts we generate yaml CRDs (in code) and submit them.
        """
        super(KubernetesScriptAdapter, self).__init__(**kwargs)

        self.add_batch_parameter("nodes", kwargs.pop("nodes", "1"))
        self._addl_args = kwargs.get("args", {})

        # Lookup from integer to actual job name
        self.job_name_lookup = {}
        self.job_counter = count(start=1)
        self.job_lookup = {}

        # Header is only for informational purposes.
        self._header = {
            "nodes": "#INFO (nodes) {nodes}",
            "walltime": "#INFO (walltime) {walltime}",
            "version": "#INFO (kubernetes adapter version) {version}",
        }

    @property
    def namespace(self):
        return self.job_desc.get("namespace") or "default"

    # Only here so it validates super class (abstract) structure
    def _write_script(self, ws_path, step):
        pass

    def write_script(self, ws_path, step):
        """
        Generate the script for the Kubernetes job.
        This is combined from write_script (from the super class) and _write_script
        (prototype from the flux script class) but without writing anything to file.
        """
        # This should come from:
        # https://github.com/LLNL/maestrowf/blob/master/maestrowf/abstracts/interfaces/schedulerscriptadapter.py#L255
        to_be_scheduled, cmd, restart = self.get_scheduler_command(step)

        # Instead of writing, assemble into components
        fname = "{}.{}".format(step.name, self._extension)
        script_path = os.path.join(ws_path, fname)
        components = {
            "filename": fname,
            "path": script_path,
            "headers": self.get_header(step),
            "command": cmd,
            "restart": False,
        }

        # How would a restart happen in an ephemeral job?
        restart_path = None
        if restart:
            rname = "{}.restart.{}".format(step.name, self._extension)
            restart_path = os.path.join(ws_path, rname)
            cmd = "\n\n{}\n".format(restart)
            components.update(
                {"restart": True, "restart_path": restart_path, "restart_command": cmd}
            )

        LOGGER.debug(
            "---------------------------------\n"
            "Script path:   %s\n"
            "Restart path:  %s\n"
            "Scheduled?:    %s\n"
            "---------------------------------\n",
            script_path,
            restart_path,
            to_be_scheduled,
        )
        return components

    @property
    def _extension(self):
        return "kubernetes-job.sh"

    @property
    def extension(self):
        return self._extension

    def nqueued_sims(self):
        return len(self.queued)

    def list_jobs(self):
        """
        List all jobs in the namespace regardless of status, etc.
        """
        batch_api = client.BatchV1Api()
        # For now, allow trigger of error (we should not trigger error)
        # We eventually want to wrap this function with a retry
        return batch_api.list_namespaced_job(namespace=self.namespace)

    @property
    def queued(self):
        """
        List jobs that are queued (not running). This overrides the manual self.queued.
        """
        jobs = self.list_jobs()
        return [
            x.metadata.name
            for x in jobs.items
            if x.status.completion_time is None
            and x.status.active == 0
            and x.status.failed != 1
            and x.metadata.labels.get("app") == self.job_desc["job_type"]
        ]

    @queued.setter
    def queued(self, value):
        pass

    @property
    def running(self):
        """
        List jobs that are running. This overrides the manual self.queued.
        """
        jobs = self.list_jobs()
        return [
            x.metadata.name
            for x in jobs.items
            if x.status.completion_time is None
            and x.status.active == 1
            and x.metadata.labels.get("app") == self.job_desc["job_type"]
        ]

    @running.setter
    def running(self, value):
        pass

    def nrunning_jobs(self):
        return len(self.running)

    def get_header(self, step):
        """
        Generate the header that is mostly for informational purposes.

        :param step: A StudyStep instance.
        :returns: A string of the header based on internal batch parameters and
                  the parameter step.
        """
        run = dict(step.run)

        batch_header = dict(self._batch)
        walltime = self.config.get("walltime", None)
        batch_header["walltime"] = convert_walltime_to_seconds(walltime)

        if run["nodes"]:
            batch_header["nodes"] = run.pop("nodes")
        batch_header["job-name"] = step.name.replace(" ", "_")
        batch_header["comment"] = step.description.replace("\n", " ")

        modified_header = ["#!{}".format(self._exec)]
        for key, value in self._header.items():
            if key not in batch_header:
                continue
            modified_header.append(value.format(**batch_header))

        return "\n".join(modified_header)

    def get_parallelize_command(self, procs, nodes=None, **kwargs):
        """
        Generate parallelization metadata for kubernetes. This would previously
        return a string command, but we don't want that for kubernetes. I am
        returning a json dump of all metadata for now.
        """
        ntasks = nodes if nodes else self._batch.get("nodes", 1)
        return json.dumps(
            {"ntasks": ntasks, "procs": procs, **kwargs, **self._addl_args}
        )

    def create_configmap(self, name, content):
        """
        Create a ConfigMap (jobscript) for Kubernetes
        """
        cm = client.V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            metadata=client.V1ObjectMeta(name=name, namespace=self.namespace),
            data={"entrypoint": content},
        )
        with client.ApiClient() as api_client:
            api = client.CoreV1Api(api_client)
            try:
                api.create_namespaced_config_map(namespace=self.namespace, body=cm)
            except Exception as e:
                if e.reason == "Conflict":
                    self.delete_configmap(name)
                    return self.create_configmap(name, content)
                else:
                    raise ValueError(
                        f"Unexpected error with configmap creation: {e.reason}"
                    )

    def cleanup(self, name):
        """
        Try cleaning up the entirety of a job
        """
        try:
            self.delete_configmap(name)
        except:
            LOGGER.warning(f"Issue cleaning up {name}")

        # Use kubernetes API to cancel jobs (delete)
        batch_api = client.BatchV1Api()

        try:
            batch_api.delete_namespaced_job(name=name, namespace=self.namespace)
        except Exception as e:
            LOGGER.warning(f"Issue deleting {name}: {e}")

    def delete_configmap(self, name):
        """
        Delete a ConfigMap from Kubernetes

        We allow flexibility here, meaning an ability to allow
        failure of the deletion, assuming a user / another
        entity deleted it first.
        """
        with client.ApiClient() as api_client:
            api = client.CoreV1Api(api_client)
            try:
                api.delete_namespaced_config_map(namespace=self.namespace, name=name)
            except Exception as e:
                LOGGER.warning(f"Issue deleting configmap {name}: {e}")

    def set_default_int(self, value, default):
        """
        Given a value, ensure it is set or use a default
        """
        if not isinstance(value, int):
            if not value:
                value = default
            else:
                value = int(value)
        return value

    def get_cores_per_task(self, step):
        """
        Helper function to get cores per task from a step.
        """
        cores_per_task = step.run.get("cores per task", None)
        if isinstance(cores_per_task, str):
            try:
                cores_per_task = int(cores_per_task)
            except:
                cores_per_task = 1
        if not cores_per_task:
            cores_per_task = 1  # max((1, ceil(processors / nodes)))

            LOGGER.warning(
                "'cores per task' set to a non-value. Populating with a "
                "sensible default. (cores per task = %d",
                cores_per_task,
            )
        return cores_per_task

    def check_jobs(self, joblist):
        """
        This only needs to be here because the parent method is abstract.
        """
        pass

    def get_gpus(self, step):
        """
        Get the number of gpus from the step
        """
        try:
            ngpus = step.run.get("gpus", "0")
            ngpus = int(ngpus) if ngpus else 0
        except ValueError as val_error:
            msg = f"Specified gpus '{ngpus}' is not a decimal value."
            LOGGER.error(msg)
            raise val_error
        return ngpus

    @property
    def extra_environment(self):
        """
        Get extra environment variables from the job description,
        """
        environment = self.job_desc.get("environment") or {}
        environ = []
        for key, value in environment.items():
            environ.append({"name": key, "value": value})
        return environ

    def generate_batch_job(self, step, configmap_name):
        """
        Generate the job CRD assuming the config map entrypoitn.
        """
        nodes = step.run.get("nodes", 1)
        nodes = self.set_default_int(nodes, 1)

        processors = step.run.get("procs", 0)
        processors = self.set_default_int(processors, 1)
        walltime = convert_walltime_to_seconds(self.config.get("walltime", 0))
        metadata = client.V1ObjectMeta(name=configmap_name)

        # Get variables from job description
        image = self.job_desc.get("image")
        if not image:
            raise ValueError("A container image is required for the Kubernetes job.")

        # Command should just execute entrypoint - keep it simple for now
        command = ["/bin/bash", "/workdir/entrypoint.sh"]

        # Compute cores per task, ngpus, and total ncores
        cores_per_task = self.get_cores_per_task(step)
        ngpus = self.get_gpus(step)
        ncores = cores_per_task * nodes

        # Raise an exception if ncores is 0
        if ncores <= 0:
            msg = (
                "Invalid number of cores specified. "
                "Aborting. (ncores = {})".format(ncores)
            )
            LOGGER.error(msg)
            raise ValueError(msg)

        # Job resources, we care about cores and GPU
        # Note that this is PER container, not across entire job
        # I also don't see memory in the step.run
        # I do see nodes, procs, gpus, cores per task
        resources = {"cpu": cores_per_task}

        # Assume for now nvidia, this can be changed
        if ngpus > 0:
            gpu_label = self.config.get("gpulabel", "nvidia.com/gpu")
            resources[gpu_label] = ngpus

        # Wrap as requests and limits
        resources = {"requests": resources, "limits": resources}

        # Container image pull policy
        pull_policy = self.config.get("pull_policy") or "IfNotPresent"
        print(f"Pull policy for {configmap_name} is {pull_policy}")

        # Job container to run the script
        # Do not define working directory assuming container is built with correct one
        container = client.V1Container(
            image=image,
            name=configmap_name,
            command=[command[0]],
            args=command[1:],
            image_pull_policy=pull_policy,
            volume_mounts=[
                client.V1VolumeMount(
                    mount_path="/workdir",
                    name="entrypoint-mount",
                ),
            ],
            env=self.extra_environment,
            resources=resources,
        )

        # Only add walltime if it's > 0 and not None
        print(f"Walltime is {walltime}")
        if walltime:
            container.active_deadline_seconds = int(walltime)

        # Prepare volumes (with config map)
        volumes = [
            client.V1Volume(
                name="entrypoint-mount",
                config_map=client.V1ConfigMapVolumeSource(
                    name=configmap_name,
                    items=[
                        client.V1KeyToPath(
                            key="entrypoint",
                            path="entrypoint.sh",
                        )
                    ],
                ),
            ),
        ]

        # Job template. The app label will be used to filter later
        template = {
            "metadata": {
                "labels": {
                    "app": self.job_desc["job_type"],
                },
            },
            "spec": {
                "containers": [container],
                "restartPolicy": "Never",
                "volumes": volumes,
                "subdomain": "r",
            },
        }

        # Add node selectors? E.g.,
        # node.kubernetes.io/instance-type: c7a.4xlarge
        node_selector = self.get_node_selector()
        if node_selector is not None:
            template["spec"]["nodeSelector"] = json.loads(node_selector)

        # These options are required for the job to fail if the pod fails
        backoff_limit = 0
        if self.config.get("retry_failure") in true_options:
            backoff_limit = 6

        print(f"Backoff limit is {backoff_limit}")

        # Do we want the job to terminate after failure?
        spec = client.V1JobSpec(
            parallelism=nodes,
            completions=nodes,
            suspend=False,
            template=template,
            backoff_limit=backoff_limit,
        )

        return client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=metadata,
            spec=spec,
        )

    @property
    def properties(self):
        """
        Properties are attributes that are specific to a tracker.
        """
        # Properties can be provided as a string to json load
        props = self.job_desc.get("properties", {})
        if isinstance(props, str):
            props = json.loads(props)
        return props


    def get_node_selector(self):
        """
        Node selector is in properties -> node-selector
        """
        return self.properties.get("node-selector")


    def submit(self, step, path, cwd, job_map=None, env=None):
        """
        Submit a script to the Flux scheduler.

        :param step: The StudyStep instance this submission is based on.
        :param path: Local path to the script to be executed.
        :param cwd: Path to the current working directory.
        :param job_map: A dictionary mapping step names to their job
                        identifiers.
        :param env: A dict containing a modified environment for execution.
        :returns: The return status of the submission command and job
                  identiifer.
        """
        # Create a config map (mounted read only script to run sim)
        configmap_name = step.name.lower().replace("_", "-")
        self.create_configmap(configmap_name, step.run["cmd"])

        # Generate the kubernetes batch job!
        job = self.generate_batch_job(step, configmap_name)
        batch_api = client.BatchV1Api()

        jobid = -1
        retcode = -1
        try:
            result = batch_api.create_namespaced_job(self.namespace, job)
            retcode = 0
            # Store a lookup from the job counter to sim name here
            # the job metadata name is the createsim-structure-NNN name
            # This is only used for cancel, and could be removed
            jobid = next(self.job_counter)
            self.job_name_lookup[jobid] = result.metadata.name
            submit_status = SubmissionCode.OK

        except Exception as e:
            # This means it was submit twice (should not happen, but let's check)
            if e.reason == "Conflict":
                LOGGER.warning(
                    f"Batch job for {step.name} exists, assuming resumed: {e.reason}"
                )
                submit_status = SubmissionCode.CONFLICT
            else:
                LOGGER.info(f"There was a create job error: {e.reason}")
                submit_status = SubmissionCode.ERROR

        return SubmissionRecord(submit_status, retcode, jobid)

    def cancel_jobs(self, joblist):
        """
        For the given job list, cancel each job. This is not currently use,
        but we might have a use case for it. This is the one place where
        we are still relying on the job identifier lookup. We can remove
        it if we don't need it (and just cancel based on the sim name).

        :param joblist: A list of job identifiers to be cancelled.
        :returns: The return code to indicate if jobs were cancelled.
        """
        # If we don"t have any jobs to check, just return status OK.
        if not joblist:
            return CancelCode.OK

        # Use kubernetes API to cancel jobs (delete)
        batch_api = client.BatchV1Api()

        # I'm going to assume a failure to cancel here is OK.
        # Technically if the user cancelled it, it's fine. We can
        # harden this a bit later. The response from the delete namespaced
        # job doesn't seem to have enough information to indicate if it was successful,
        # likely because it's issued and then doesn't confirm deletion (there is a delay)
        # We should look into if there is a parameter like wait or return status.
        for jobid in joblist:
            job_name = self.job_name_lookup.get(jobid)
            if not job_name:
                LOGGER.warning(f"Unknown jobid {jobid} to cancel, skipping")
                continue
            try:
                batch_api.delete_namespaced_job(name=job_name, namespace=self.namespace)
            except Exception as e:
                LOGGER.warning(f"Issue deleting {job_name}: {e}")

            # Delete the associated config map
            self.delete_configmap(job_name)

        return CancelCode.OK

    def _state(self, flux_state):
        raise NotImplementedError(
            "KubernetesScriptAdapter does not use the _state mapping."
        )


def convert_walltime_to_seconds(walltime):
    """
    This is from flux and the function could be shared
    """
    # An integer or float was provided
    if isinstance(walltime, int) or isinstance(walltime, float):
        LOGGER.debug("Encountered numeric walltime = %s", str(walltime))
        return int(float(walltime) * 60.0)

    # A string was provided that will convert to numeric
    elif isinstance(walltime, str) and walltime.isnumeric():
        LOGGER.debug("Encountered numeric walltime = %s", str(walltime))
        return int(float(walltime) * 60.0)

    # A string was provided that needs to be parsed
    elif ":" in walltime:
        LOGGER.debug("Converting %s to seconds...", walltime)
        seconds = 0.0
        for i, value in enumerate(walltime.split(":")[::-1]):
            seconds += float(value) * (60.0**i)
        return seconds

    # Don't set a wall time
    elif not walltime or (isinstance(walltime, str) and walltime == "inf"):
        return 0

    # If we get here, we have an error
    msg = (
        f"Walltime value '{walltime}' is not an integer or colon-" f"separated string."
    )
    LOGGER.error(msg)
    raise ValueError(msg)


class KubernetesTracker(JobTracker):
    """Class for a Kubernetes job tracker

    The adapter_batch group has arguments for our Kubernetes batch job.
    E.g., working directory, container, environment, etc.
    """

    def __init__(
        self, job_desc, total_nodes, iointerface, adapter_batch, enable_scheduling=True
    ):
        super().__init__(
            job_desc, total_nodes, iointerface, adapter_batch, enable_scheduling
        )

        # TODO this envrionment variable has the max nodes we will allow to autoscale to
        # We can use this later...
        self.max_nodes_autoscale = (
            os.environ.get("KUBERNETES_MAX_NODES", total_nodes) or total_nodes
        )

        # This is the portion of nodes calculated by the workflow manager we can use for this
        # job type. If not set, default to 1.
        self.max_jobs_total = max(self.max_jobs_total, 1)

        # We might eventually need to PR to maestrowlf to add a kubernetes adapter, but for
        # now are just adding our own here.
        if enable_scheduling:
            self.do_scheduling = True
            adapter_type = adapter_batch.get("type")

            # Create the kubernetes adapter. This is an approach to bypass maestro for now
            if adapter_type == "kubernetes":
                self.adapter = KubernetesScriptAdapter(**adapter_batch)

                # There is probably a right way to pass these on, this works for now
                # We want to make the config data easily accessible.
                self.adapter.job_desc = job_desc
                self.adapter.config = job_desc["config"]
        else:
            raise ValueError(
                "The Kubernetes adapter type must be used with the Kubernetes tracker."
            )

    # --------------------------------------------------------------------------
    def __str__(self):
        return (
            f"KubernetesTracker[{self.type}]: "
            f"#max_jobs = {self.max_jobs_total}, "
            f"#running = {len(self.running)}, "
            f"#queued = {len(self.queued)}"
        )

    @property
    def name(self):
        """
        Get the job description name
        """
        return self.job_desc["job_type"]

    def list_jobs_by_status(self, convert_jobid=True):
        """
        Return a lookup of jobs by status

        :param convert_jobid: If True, convert jobid back to original simid
        :returns: Dictionary with job lists and total count
        """
        jobs = self.adapter.list_jobs()

        # The above is all jobs, across types (createsim and cg)
        # We need to filter down to those where app matches the job type
        jobs = [x for x in jobs.items if x.metadata.labels.get("app") == self.name]

        # These are the lists we will populate.
        # I think there was a difference between sims and jobs, but this
        # model in Kubernetes has one simulation == one job, so I'm reducing
        sims_success = []  # simulations that have finished successfully
        sims_failed = []  # simulations that have failed
        sims_continue = []  # simulations that need to be continued (running)
        sims_queued = []  # simulations that are queued
        sims_unknown = []  # simulations with unknown (need investigation)

        for job in jobs:
            # Success means we finished with succeeded condition
            if job.status.succeeded == 1 and job.status.completion_time is not None:
                sims_success.append(job.metadata.name)
                continue

            # Failure means we finished with failed condition
            if job.status.failed == 1 and job.status.completion_time is not None:
                sims_failed.append(job.metadata.name)
                continue

            # Not active, and not finished is queued
            if not job.status.active and not job.status.completion_time:
                sims_queued.append(job.metadata.name)
                continue

            # Active, and not finished is running
            if job.status.active == 1 and not job.status.completion_time:
                sims_continue.append(job.metadata.name)
                continue

            # If it didn't fail or succeed, let it keep going to timeout (duration/walltime)
            sims_unknown.append(job.metadata.name)

        # Total is all jobs minus unknown
        total = (
            len(sims_queued) + len(sims_continue) + len(sims_success) + len(sims_failed)
        )
        if sims_unknown:
            LOGGER.warning(
                f"Simulations with unknwon status need investigation: {sims_unknown}"
            )

        jobs = {
            "success": sims_success,
            "failed": sims_failed,
            "queued": sims_queued,
            "continue": sims_continue,
            "unknown": sims_unknown,
        }
        updated = jobs
        if convert_jobid:
            for state, joblist in jobs.items():
                updated[state] = [self.jobid_to_sim(x) for x in joblist]

        # Add the total, no matter what the jobid
        updated["total"] = total
        updated["all"] = (
            updated["success"]
            + updated["failed"]
            + updated["queued"]
            + updated["continue"]
        )
        return updated

    def jobid_to_sim(self, jobid):
        """
        Convert a jobid in Kubernetes back to the sim id. E.g.,

        createsim-structure-iter00-000000001801-6xmgq -> structure_iter00_000000001801_6xmgq
        """
        return jobid.replace(f"{self.type}-", "").replace("-", "_")

    @property
    def queued(self):
        """
        Queued is dynamic - the number of jobs of the type that are in queue.
        These are in queue but not running. Note this over-rides a variable
        that was being manually stored. If it's not submit to the queue, it
        does not exist as a queued job!
        """
        return self.adapter.queued

    @queued.setter
    def queued(self, value):
        """
        Allow parent class JobTracker to faux "set" the property so we don't need
        to edit mummi-core. This only happens on init, trying to set to empty list.
        """
        pass

    @property
    def running(self):
        """
        List jobs that are running. This overrides the manual self.queued.
        """
        return self.adapter.running

    @running.setter
    def running(self, value):
        pass

    @property
    def running_sims(self):
        """
        Return running sims as determed by Kubernetes script adapter
        """
        return self.adapter.running

    def submit_job(self, sim_name):
        """
        Submit a job using the adapter.

        Returns:
            bool:       to indicate if the submit was successful/done or not
        """
        # Note that this doesn't actually write the script to the filesystem
        cmd_script, step = self.write_script(sim_name)
        LOGGER.debug(f"[{self.type}] submitting script {sim_name} {cmd_script}")

        # submit cmd_script to adapter and append (jobid, simname) to queue
        submit_record = self.adapter.submit(step, cmd_script, self.workspace)

        # A conflcit means the job is already running. We don't want to count
        # it as a new submit (it will already be represented in the state)
        if submit_record.submission_code == SubmissionCode.CONFLICT:
            LOGGER.error(
                f"[{self.type}] Found already running {self.type} job (Conflict) for simname = {sim_name}"
            )
            return False

        # Allow it to fail and attempt cleanup
        elif not submit_record or submit_record.submission_code != SubmissionCode.OK:
            LOGGER.error(
                f"[{self.type}] Failed to submit a {self.type} job for simname = {sim_name}"
            )
            self.adapter.cleanup(step.name)
            return False

        # We don't need to save the jobid to running here - we can get
        # them dynamically, and the jobid is stored with the adapter.
        job_id = submit_record.job_identifier
        LOGGER.debug(f"[{self.type}] Started job {job_id} for {sim_name}")
        return True

    # --------------------------------------------------------------------------
    # MuMMI Workflow functionality
    # --------------------------------------------------------------------------
    @staticmethod
    def check_sim_status(
        iointerface, job_type, dir_sim, sim_names
    ) -> List[SimulationStatus]:
        """
        Check the status of a simulation using success flags.
        This previously relied on filesystem indicators. We
        ask the Kubernetes API directly, but keep the same interface
        so it doesn't break something unexpectedly.
        """
        raise ValueError("Check sim status should not be called.")

    def status(self):
        """
        Return status for the workflow manager.
        """
        running = self.running
        return {
            "type": self.type,
            "jobCnt": self.jobCnt,
            "nqueued": len(self.queued),
            "nrunning": len(running),
            "queued": self.queued,
            "running": running,
        }

    def restore(self, state, check_for_running_jobs):
        """Check status of all running jobs, and return list of success and
        failure for the next step. This does not need to update the queue.

        Returns:
            sims_success = []:      simulations that have finished successfully
            sims_failed = []:       simulations that have failed
        """
        assert self.type == state["type"]
        jobs = self.list_jobs_by_status()
        self.jobCnt = jobs["total"]
        nrunning = len(jobs["continue"])
        nqueued = len(jobs["queued"])

        LOGGER.info(
            f"[{self.type}] Restoring KubernetesTracker: running = {nrunning} jobs, queued = {nqueued} sims"
        )
        # Cut out early (don't write history) if nothing restored
        if (nrunning == 0) and (nqueued == 0):
            return [], []

        # This isn't really a restore, it's a discovery
        LOGGER.info(f"[{self.type}] Found {nqueued} queued and {nrunning} running jobs")
        LOGGER.info(self.__str__())

        # return the ones that we did not restore so wf can handle them
        # These ids will be converted back to original sim ids
        return jobs["success"], jobs["failed"]

    def add_to_queue(self, sim_names, prepend=False):
        """
        Add some simulations to the queue. Unlike other Job Trackers, we submit
        all jobs here up to the max allowed. Prepend is not used, and this function
        inherits a lot of the logic of start_jobs.

        Returns:
            sim_names []:       the sims that were actually added
        """
        # These are from rabbitmq, the list of structure_iterXXXX names processed
        # by the ml server
        assert isinstance(sim_names, list)
        assert all([isinstance(s, str) for s in sim_names])

        # nothing to do for empty list
        n = len(sim_names)
        if n == 0:
            return sim_names

        LOGGER.info(f"[{self.type}] Evaluating {n} contender sims: {self.__str__()}")

        # remove any duplicates
        # Python 3.7 and later this will maintain ordering
        sim_names = list(dict.fromkeys(sim_names))
        if len(sim_names) < n:
            LOGGER.warning(f"[{self.type}] Found only {len(sim_names)} unique sims")
            n = len(sim_names)

        # Don't add those that are already accounted for
        jobs = self.list_jobs_by_status()
        active_jobs = len(jobs["queued"]) + len(jobs["continue"])
        sim_names = [x for x in sim_names if x not in jobs["all"]]

        # finally, add these simulations, which is a submit
        if not self.do_scheduling:
            LOGGER.info(f"[{self.type}] Scheduling disabled")
            return []

        # Otherwise, submit. If there is an issue, we'd try again.
        submit_success = []
        for sim_name in sim_names:
            # Have we gone over the allowed active (not completed) jobs?
            current_jobs = len(submit_success) + active_jobs
            if current_jobs >= self.max_jobs_total:
                LOGGER.warning(
                    f"Maximum jobs for {self.name} ({self.max_jobs_total}) reached, will not submit more."
                )
                break

            # Otherwise, submit away!
            if self.submit_job(sim_name):
                submit_success.append(sim_name)

        n = len(submit_success)
        LOGGER.debug(f"[{self.type}]  {n} sims: {self.__str__()}: {submit_success}")
        return submit_success

    # --------------------------------------------------------------------------
    def start_jobs(self, n_jobs):
        """
        This no longer needs to manually start - the add_to_queue that was just
        run has already submit/started jobs. The result returned here does get
        debug printed, but is returned and not used anywhere. Nothing meaningful
        is returned.

        Returns:
            n_jobs:         number of jobs started
            sims_started:   names of the sims started
        """
        return 0, []

    # --------------------------------------------------------------------------
    def write_script(self, sims_chunk: str):
        """
        Create a Maestro study step and cmd_script
        """
        assert self.do_scheduling == True
        LOGGER.debug(f"[{self.type}] Creating step for {sims_chunk}...")
        step = self.create_step([sims_chunk])
        LOGGER.debug(f"[{self.type}] Step created: {step}")
        components = self.adapter.write_script(self.workspace, step)
        return components, step

    # --------------------------------------------------------------------------
    def update(self):
        """Check all running jobs to update the status of the tracker. This has
        been updated so this information is generated dynamically.

        Returns:
            sims_success = []:      simulations that have finished successfully
            sims_failed = []:       simulations that have failed
        """
        # This used to cut out early if no running jobs, but we likely want
        # to return successes / failures regardless.
        LOGGER.info(self.__str__())

        # Get all jobs by status
        jobs = self.list_jobs_by_status()

        # split the simulations of this job based on status
        LOGGER.debug(f"[{self.type}] sims: success = {len(jobs['success'])}")
        LOGGER.debug(f"[{self.type}] sims: failed = {len(jobs['failed'])}")
        LOGGER.debug(f"[{self.type}] sims: continue = {(len(jobs['continue']))}")
        LOGGER.debug(f"[{self.type}] sims: unknown = {len(jobs['unknown'])}")
        LOGGER.debug(f"[{self.type}] sims: queued = {len(jobs['queued'])}")

        # return the successful and failed sims for further processing
        return jobs["success"], jobs["failed"]
