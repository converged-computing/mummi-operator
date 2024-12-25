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

import os
import json
from typing import Tuple, List, ItemsView, Dict
import time, datetime, importlib
from functools import partial
from multiprocessing import Pool
from itertools import count
import uuid
from mummi_core.utils import Naming

from maestrowf.datastructures.core import StudyStep
from maestrowf.abstracts.enums import CancelCode, SubmissionCode, JobStatusCode, State
from maestrowf.abstracts.enums import JobStatusCode, CancelCode
from maestrowf.interfaces.script import CancellationRecord, SubmissionRecord

import mummi_core
from mummi_core import Naming
from mummi_core.utils.utilities import partition_list, sig_ign_and_rename_proc
from mummi_core.workflow.job import Job, JOB_TYPES, SimulationStatus
from mummi_core.workflow.jobTracker import JobTracker

from maestrowf.abstracts.enums import (
    CancelCode,
    JobStatusCode,
    State,
    StepPriority,
    SubmissionCode,
)

# These are added just for maestro and the custom adapter
from maestrowf.abstracts.interfaces import SchedulerScriptAdapter

from logging import getLogger

LOGGER = getLogger(__name__)

from kubernetes import client, config

# This assumes the wfmanager running inside the cluster
config.load_incluster_config()

# This would assume external to it
# config.load_kube_config()


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

        # NOTE: Host doesn"t seem to matter for FLUX. sbatch assumes that the
        # current host is where submission occurs.
        self.add_batch_parameter("nodes", kwargs.pop("nodes", "1"))
        self._addl_args = kwargs.get("args", {})

        # Lookup from integer to actual job name
        self.job_name_lookup = {}
        self.job_counter = count(start=1)

        # TODO - ensure there is documentation, etc. for passing in namespace
        # Header is only for informational purposes.
        # Not sure we need this
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
        Generate the script (but don't write to file) for the specified StudyStep.
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
        return to_be_scheduled, components

    @property
    def _extension(self):
        return "kubernetes-job.sh"

    @property
    def extension(self):
        return self._extension

    # We might want to consider not storing these "hard coded" but deriving from Kubernetes directly instead.
    def nqueued_sims(self):
        return len(self.queued)

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
        walltime = step.run.get("walltime", None)
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
                    raise ValueError(f'Unexpected error with configmap creation: {e.reason}')
                    
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
            response = batch_api.delete_namespaced_job(
                name=name, namespace=self.namespace
            )
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
                LOGGER.warn("Issue deleting configmap {name}: {e}")

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

            LOGGER.warn(
                "'cores per task' set to a non-value. Populating with a "
                "sensible default. (cores per task = %d",
                cores_per_task,
            )
        return cores_per_task

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

    def generate_batch_job(self, step, configmap_name, environment=None):
        """
        Generate the job CRD assuming the config map entrypoitn.
        """
        nodes = step.run.get("nodes", 1)
        nodes = self.set_default_int(nodes, 1)

        processors = step.run.get("procs", 0)
        processors = self.set_default_int(processors, 1)

        walltime = convert_walltime_to_seconds(step.run.get("walltime", 0))
        urgency = step.run.get("priority", "medium")

        # TODO kueue can be added as a label here
        metadata = client.V1ObjectMeta(name=configmap_name)

        # Get variables from job description
        image = self.job_desc.get("image")
        workdir = self.job_desc.get("workdir")
        if not image:
            raise ValueError("A container image is required for the Kubernetes job.")

        # Command should just execute entrypoint - keep it simple for now
        command = ["/bin/bash", "/workdir/entrypoint.sh"]

        # Compute cores per task
        cores_per_task = self.get_cores_per_task(step)

        # Calculate ngpus (note this is not currently mapped to the job)
        ngpus = self.get_gpus(step)

        # Calculate nprocs
        ncores = cores_per_task * nodes

        # Raise an exception if ncores is 0
        if ncores <= 0:
            msg = (
                "Invalid number of cores specified. "
                "Aborting. (ncores = {})".format(ncores)
            )
            LOGGER.error(msg)
            raise ValueError(msg)

        # Unpack waitable flag and pass it along if there: only pass it along if
        # it's in the step maybe, leaving each adapter to retain their defaults?
        waitable = step.run.get("waitable", False)

        # Job container to run the script
        # Do not define working directory assuming container is built with correct one
        container = client.V1Container(
            image=image,
            name=configmap_name,
            command=[command[0]],
            args=command[1:],
            volume_mounts=[
                client.V1VolumeMount(
                    mount_path="/workdir",
                    name="entrypoint-mount",
                ),
            ],
            env=self.extra_environment,
            # Note that I don't see memory in the step.run
            # I do see nodes, procs, gpus, cores per task
            resources={"requests": {"cpu": ncores}},
        )

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

        # Job template
        # TODO: if createsims needs to be on same network, need to derive that here
        # also note - the job name is too long for FQDN and would need to be shorter
        template = {
            "metadata": {
                "labels": {"app": self.job_desc["job_type"]},
            },
            "spec": {
                "containers": [container],
                "restartPolicy": "Never",
                "volumes": volumes,
                "subdomain": "r",
            },
        }
        return client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=metadata,
            spec=client.V1JobSpec(
                parallelism=nodes, completions=nodes, suspend=False, template=template
            ),
        )

    def get_simulation_statuslist(self, sim_names):
        """
        get simulation statuslist is called by check_sim_status
        that previously used filesystem indicators.
        """
        # Create a batch client
        batch_api = client.BatchV1Api()

        # List of simulation statuses
        statuses = []
        for job_name in sim_names:
            try:
                # This requires rbac job/status
                status = batch_api.read_namespaced_job_status(
                    name=job_name, namespace=self.namespace
                )
                # Map the kubernetes status into Maestro status
                statuses.append(self.kubernetes_to_maestro_status(status))
            except Exception as e:
                LOGGER.info(f"Exception for status for job {job_name}: {e.reason}")
                if e.reason == "Not Found":
                    statuses.append(State.NOTFOUND)
                    LOGGER.warning(f"Job name {job_name} is not found, continuing.")
                elif e.reason == "Conflict":
                    print('There is a conflict, line 422 kubernetesTracker.py')
                    import IPython
                    IPython.embed()
                else:
                    continue

                # Not sure if this is the best action to take
                LOGGER.error(e)
                statuses.append(State.UNKNOWN)
        return statuses

    def kubernetes_to_maestro_status(self, status):
        """
        Convert result of read_namespaced_job_status to maestro status.

        How I determined the "mapping" below (needs work upstream)
        https://github.com/kubernetes/kubernetes/issues/68712
        """
        # These are all valid maestro states
        # This means we are completed
        if status.status.completion_time is not None:

            # This isn't the ideal place to put this, but it's the single point when
            # we determine that a job is done and can cleanup the config map.
            print(status.metadata)
            self.delete_configmap(status.metadata.name)
            return State.FINISHING

        # If we have a start time, it's running
        if status.status.start_time is not None:
            return State.RUNNING

        # I don't think we can distinguish waiting for PENDING vs WAITING vs QUEUED
        # PENDING: pending start (in the scheduler)
        # WAITING: waiting for resources (also in the scheduler)
        # QUEUED: queued (in the scheduler)
        return State.PENDING

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

        # TODO what to do with path, cwd? We likely need to pass forward data here
        batch_api = client.BatchV1Api()

        jobid = -1
        retcode = -1
        try:
            result = batch_api.create_namespaced_job(self.namespace, job)
            retcode = 0

            # The calling class (Job) cannot accept a non-integer jobid
            # so instead we keep a lookup here
            jobid = next(self.job_counter)
            self.job_name_lookup[jobid] = result.metadata.name
            submit_status = SubmissionCode.OK

        except Exception as e:
            # Assume script was restarted
            if e.reason == "Conflict":
                LOGGER.warning("Batch job for {step.name} exists, assuming resumed: {e.reason}")
                submit_status = SubmissionCode.OK            
            else:             
                LOGGER.info("There was a create job error: {e.reason}")
                import IPython
                IPython.embed()
                submit_status = SubmissionCode.ERROR

        return SubmissionRecord(submit_status, retcode, jobid)

    def check_jobs(self, joblist):
        """
        For the given job list, query execution status.

        This method uses the scontrol show job <jobid> command and does a
        regex search for job information.

        :param joblist: A list of job identifiers to be queried.
        :returns: The return code of the status query, and a dictionary of job
                  identifiers to their status.
        """
        LOGGER.debug("Joblist type -- %s", type(joblist))
        LOGGER.debug("Joblist contents -- %s", joblist)
        if not joblist:
            LOGGER.debug("Empty job list specified.")
            return JobStatusCode.OK, {}

        if not isinstance(joblist, list):
            LOGGER.debug("Specified parameter is not a list.")
            if isinstance(joblist, int):
                LOGGER.debug("Integer found.")
                joblist = [joblist]
            else:
                LOGGER.debug("Unknown type. Returning an error.")
                return JobStatusCode.ERROR, {}

        # Create a batch client
        batch_api = client.BatchV1Api()

        # We return statuses, looked up by int jobid
        statuses = {}

        # This is the kubernetes status objects
        # We need this to derive failed/success because there is not
        # enough granularity with the above.
        k8s_statuses = {}

        # This only gets set to ERROR if something borks
        chk_status = JobStatusCode.OK

        # Get each job by lookup
        for jobid in joblist:
            job_name = self.job_name_lookup.get(jobid)
            if not job_name:
                statuses[jobid] = State.UNKNOWN
                LOGGER.warning(f"Unknown jobid {jobid}, skipping")
                continue

            try:
                # This requires rbac job/status
                status = batch_api.read_namespaced_job_status(
                    name=job_name, namespace=self.namespace
                )

                # Map the kubernetes status into Maestro status
                statuses[jobid] = self.kubernetes_to_maestro_status(status)

                # And save for kubernetes later
                k8s_statuses[jobid] = status.status

            # TODO - we should have finer grained check here for error type
            except Exception as e:

                # Not found, do not fail, just continue
                print(str(e))
                if str(e) == "Not Found":
                    statuses[jobid] = State.NOTFOUND
                    LOGGER.warning(f"Job name {job_name} is not found, continuing.")
                    continue

                # Otherwise, assume an error we need to know about
                LOGGER.error(e)
                chk_status = JobStatusCode.ERROR

        return chk_status, statuses, k8s_statuses

    def cancel_jobs(self, joblist):
        """
        For the given job list, cancel each job.

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
        # We should look into if there is a parameter like wait or return status
        # to see if we can do better, I'm too tired now.
        for jobid in joblist:
            job_name = self.job_name_lookup.get(jobid)
            if not job_name:
                LOGGER.warning(f"Unknown jobid {jobid} to cancel, skipping")
                continue
            try:
                response = batch_api.delete_namespaced_job(
                    name=job_name, namespace=self.namespace
                )
            except Exception as e:
                LOGGER.warning(f"Issue deleting {job_name}: {e}")

            # Delete the associated config map
            self.delete_configmap(job_name)

        return CancellationRecord(cancel_code, cancel_rcode)

    def _state(self, flux_state):
        raise NotImplementedError(
            "KubernetesScriptAdapter does not use the _state mapping."
        )


def convert_walltime_to_seconds(walltime):
    """
    This is from flux and the function could be shared
    """
    if isinstance(walltime, int) or isinstance(walltime, float):
        LOGGER.debug("Encountered numeric walltime = %s", str(walltime))
        return int(float(walltime) * 60.0)
    elif isinstance(walltime, str) and walltime.isnumeric():
        LOGGER.debug("Encountered numeric walltime = %s", str(walltime))
        return int(float(walltime) * 60.0)
    elif ":" in walltime:
        # Convert walltime to seconds.
        LOGGER.debug("Converting %s to seconds...", walltime)
        seconds = 0.0
        for i, value in enumerate(walltime.split(":")[::-1]):
            seconds += float(value) * (60.0**i)
        return seconds
    elif not walltime or (isinstance(walltime, str) and walltime == "inf"):
        return 0
    else:
        msg = (
            f"Walltime value '{walltime}' is not an integer or colon-"
            f"separated string."
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
        
        # Ensure max jobs total is at least 1
        self.max_jobs_total = max(self.max_jobs_total, 1)

        # Note that the above fails here, and sets self.adapter and self.do_scheduling to false
        # we will eventually need to PR to maestrowlf to add a kubernetes adapter, but for
        # now are just adding our own here.
        if enable_scheduling:
            self.do_scheduling = True
            adapter_type = adapter_batch.get("type")

            # Create the kubernetes adapter. This is an approach to bypass maestro for now
            if adapter_type == "kubernetes":
                self.adapter = KubernetesScriptAdapter(**adapter_batch)

                # There is probably a right way to pass these on, this works for now
                self.adapter.job_desc = job_desc

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
        return self.job_desc['job_type']

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

    @staticmethod
    def unwrap_kubernetes_status(status):
        """
        Convert Kubernetes status to SimulationStatus

        This is returned as a list only because that's how the above was implemented,
        not sure why. It's only for one job status.
        """
        # Assume completed and failed is not None means failure, no oras result
        if status.completion_time is not None and status.failed is not None:
            return [SimulationStatus.Failed]

        # Ditto for succeeded
        if status.completion_time is not None and status.succeeded is not None:
            return [SimulationStatus.Success]

        # TODO I don't know how we'd determine stop or what it means
        # The only other option is unknown, which means keep running!
        return [SimulationStatus.Unknown]

    # --------------------------------------------------------------------------

    def split_sims_on_status(self, sim_names, sim_statuses=None):
        """
        Split a list of sims based on their status.
        Returns:
            sims_success []:       List of sims that finished successfully
            sims_failed []:        List of sims that failed
            sims_unknown []:       List of sims that are neither
        """
        assert isinstance(sim_names, list)
        assert all([isinstance(s, str) for s in sim_names])

        if sim_statuses is not None:
            assert isinstance(sim_statuses, list)
            assert all([isinstance(s, SimulationStatus) for s in sim_statuses])
            assert len(sim_names) == len(sim_statuses)
        else:
            sim_statuses = self.adapter.get_simulation_statuslist(sim_names)

        sims_success = []
        sims_failed = []
        sims_unknown = []
        sims_stop = []

        for i in range(len(sim_names)):
            if sim_statuses[i] == SimulationStatus.Success:
                sims_success.append(sim_names[i])
            elif sim_statuses[i] == SimulationStatus.Failed:
                sims_failed.append(sim_names[i])
            elif sim_statuses[i] == SimulationStatus.Stop:
                sims_stop.append(sim_names[i])
            else:
                sims_unknown.append(sim_names[i])

        LOGGER.debug(
            f"sims_success = {sims_success}, sims_failed = {sims_failed}, sims_unknown = {sims_unknown} and sims_stop = {sims_stop}"
        )
        return sims_success, sims_failed, sims_unknown, sims_stop

    def restore(self, state, check_for_running_jobs):
        """Check status of all running jobs, and add to queue if needed.
        Returns:
            sims_success = []:      simulations that have finished successfully
            sims_failed = []:       simulations that have failed
        """
        assert self.type == state["type"]
        self.jobCnt = state["jobCnt"]  # fake: only needed for no_scheduling mode

        jobs_running = state["running"]
        sims_queued = list(state["queued"])
        nrunning = len(jobs_running)
        nqueued = len(sims_queued)

        LOGGER.info(
            f"[{self.type}] Restoring KubernetesTracker: running = {len(jobs_running)} jobs, queued = {len(sims_queued)} sims"
        )

        if (nrunning == 0) and (nqueued == 0):
            return [], []

        _data = [f"running={nrunning}", f"queued={nqueued}"]
        self.write_history("restore", _data, "restore")

        # ----------------------------------------------------------------------
        # need to check for running jobs
        if check_for_running_jobs:

            jobs_restored = []
            sims_restored = []
            for jobId, sims in jobs_running.items():
                LOGGER.debug(f"[{self.type}] is job {jobId} running? {sims}")
                if self.is_job_running(jobId)[0]:
                    LOGGER.debug(f"[{self.type}] Restoring job {jobId}: sims = {sims}")
                    self.running[jobId] = Job(self.type, jobId, sims)
                    jobs_restored.append(jobId)
                    sims_restored.extend(sims)

                if self.nrunning_jobs() >= self.max_jobs_total:
                    break

            LOGGER.info(f"[{self.type}] Restored {self.nrunning_jobs()} jobs")
            self.write_history("restored", sims_restored, "restore")

            # remove the restored jobs from the list
            for j in jobs_restored:
                jobs_running.pop(j)

        # ----------------------------------------------------------------------
        # now, collect the sims of the jobs that were not restored!
        sims_not_restored = []
        for jobId, sims in jobs_running.items():
            sims_not_restored.extend(sims)

        # ----------------------------------------------------------------------
        # TODO: this fix should not be needed
        # filter the ones that are not correctly setup!
        if True:
            _correct = [self.is_setup(s) for s in sims_not_restored]
            sims_not_restored, _rejected = partition_list(sims_not_restored, _correct)

            if len(_rejected) > 0:
                LOGGER.error(
                    f"[{self.type}] Found some running sims that were not setup correctly. Ignoring those: {_rejected}!"
                )
                self.write_history(
                    "rejected", _rejected, "restore:incorrect_setup/running"
                )
                assert False

            _correct = [self.is_setup(s) for s in sims_queued]
            sims_queued, _rejected = partition_list(sims_queued, _correct)

            if len(_rejected) > 0:
                LOGGER.error(
                    f"[{self.type}] Found some queued sims that were not setup correctly. Ignoring those: {_rejected}!"
                )
                self.write_history(
                    "rejected", _rejected, "restore:incorrect_setup/queued"
                )
                assert False

        if False:
            sims_backup = sims_not_restored
            sims_not_restored = [s for s in sims_not_restored if self.is_setup(s)]

            if len(sims_not_restored) != len(sims_backup):
                LOGGER.error(
                    f"[{self.type}] Found some running sims that were not setup correctly. Ignoring those!"
                )
                LOGGER.error(f"[{self.type}] got from checkpoint file: {sims_backup}")
                LOGGER.error(f"[{self.type}] using only: {sims_not_restored}")

            # filter the ones that are not correctly setup!
            sims_backup_q = sims_queued
            sims_queued = [s for s in sims_queued if self.is_setup(s)]

            if len(sims_queued) != len(sims_backup_q):
                LOGGER.error(
                    f"[{self.type}] Found some queued sims that were not setup correctly. Ignoring those!"
                )
                LOGGER.error(f"[{self.type}] got from checkpoint file: {sims_backup_q}")
                LOGGER.error(f"[{self.type}] using only: {sims_queued}")
        # TODO: above fix should not be needed
        # ----------------------------------------------------------------------

        # check the status of these sims
        sims_success, sims_failed, sims_continue, sims_stop = self.split_sims_on_status(
            sims_not_restored
        )

        self.write_history("found_success", sims_success, "restore")
        self.write_history("found_failed", sims_failed, "restore")
        self.write_history("found_stop", sims_stop, "restore")

        # now, add them to the queue
        LOGGER.info(
            f"[{self.type}] Queuing {len(sims_continue)} previously-running sims"
        )
        _radded = self.add_to_queue(sims_continue, prepend=True)

        # now add the queued jobs
        _qadded = self.add_to_queue(sims_queued, prepend=False)

        # ----------------------------------------------------------------------
        LOGGER.info(
            f"[{self.type}] Restored {nqueued} queued and {nrunning} running jobs"
        )
        LOGGER.info(self.__str__())

        # return the ones that we did not restore so wf can handle them
        return sims_success, sims_failed

    # --------------------------------------------------------------------------
    def add_to_queue(self, sim_names, prepend=False):
        """
        Add some simulations to the queue.
        Returns:
            sim_names []:       the sims that were actually added
        """
        assert isinstance(sim_names, list)
        assert all([isinstance(s, str) for s in sim_names])

        # nothing to do for empty list
        n = len(sim_names)
        if n == 0:
            return sim_names

        LOGGER.info(f"[{self.type}] Adding {n} sims: {self.__str__()}")

        # remove any duplicates
        # TODO: this destroys the order (problem when restoring?)
        sim_names = list(set(sim_names))
        if len(sim_names) < n:
            LOGGER.warning(f"[{self.type}] Found only {len(sim_names)} unique sims")
            n = len(sim_names)

        # don't add those that are already queued
        _is_already_queued = [_ in self.queued for _ in sim_names]
        _rejected, sim_names = partition_list(sim_names, _is_already_queued)
        if len(_rejected) > 0:
            LOGGER.warning(
                f"[{self.type}] "
                f"Rejecting {len(_rejected)} already queued sims: {_rejected}"
            )
            self.write_history("rejected", _rejected, "add_to_queue:already_queued")
            n = len(sim_names)

        # don't add those that are already running
        rsims = self.running_sims()
        _is_already_running = [_ in rsims for _ in sim_names]
        _rejected, sim_names = partition_list(sim_names, _is_already_running)
        if len(_rejected) > 0:
            LOGGER.warning(
                f"[{self.type}] "
                f"Rejecting {len(_rejected)} already running sims: {_rejected}"
            )
            self.write_history("rejected", _rejected, "add_to_queue:already_running")
            n = len(sim_names)

        # finally, add these simulations
        if prepend:
            _tag = "prepended"
            self.queued = sim_names + self.queued
        else:
            _tag = "appended"
            self.queued = self.queued + sim_names

        self.write_history(f"{_tag}_to_queue", sim_names, "add_to_queue")
        LOGGER.debug(f"[{self.type}] {_tag} {n} sims: {self.__str__()}: {sim_names}")
        print(f"Addition to {self.name} queue: {sim_names}")
        return sim_names

    # --------------------------------------------------------------------------
    def start_jobs(self, n_jobs):
        """Start queued simulations in bundles of size self.bundle_size.
        Returns:
            n_jobs:         number of jobs started
            sims_started:   names of the sims started
        """
        print(f'Jobs asked for: {n_jobs}')
        print(f'Max jobs total: {self.max_jobs_total}')
        print(f'Running jobs: {self.nrunning_jobs()}')
        print(f'Queued jobs: {self.nqueued_sims()}')
        print(f'Bundle size: {self.bundle_size}')

        assert 1 == self.bundle_size
        assert isinstance(n_jobs, int)
        assert n_jobs >= 0
        assert self.nrunning_jobs() <= self.max_jobs_total
        assert all([isinstance(q, str) for q in self.queued])

        if n_jobs == 0:
            return 0, []

        LOGGER.info(self.__str__())

        # For kubernetes, we don't need to worry about the resource size -
        # the jobs will be queued. Submit the size of the bundle, up to the max jobs
        # TODO: these numbers for queued / running should be derived directly
        # from the Kubernetes API, and not stored here.
        # ----------------------------------------------------------------------
        mn_jobs = self.nqueued_sims() // self.bundle_size
        if mn_jobs > self.max_jobs_total:
            mn_jobs = self.max_jobs_total
        print(f"mn_jobs is {mn_jobs}")
        if mn_jobs == 0:
            LOGGER.debug(
                f"[{self.type}] Nothing to do! (njobs = {self.max_jobs_total}),"
                f"(max_jobs_total - nrunning_jobs = {self.max_jobs_total}-{self.nrunning_jobs()} = {self.max_jobs_total - self.nrunning_jobs()}), "
                f"(nqueued//bundle = {self.nqueued_sims()}//{self.bundle_size} = {self.nqueued_sims() // self.bundle_size})"
            )
            return 0, []

        n_jobs = mn_jobs
        LOGGER.debug(f"[{self.type}] n_jobs = {n_jobs}")
        assert n_jobs > 0

        # ----------------------------------------------------------------------
        # pick chunks of simulations
        n_sims = n_jobs * self.bundle_size
        sims_started: List[str] = sorted(self.queued[:n_sims])
        self.queued = self.queued[n_sims:]
        LOGGER.debug(f"[{self.type}] sims_to_start = {sims_started}")

        # ----------------------------------------------------------------------
        # Mar 02, 2021. HB commented this piece
        # and replaced with a parallel version
        if self.do_scheduling:
            # We use serial here, no need to submit in parallel (will add error)
            # LP: April 21: the pool version hangs sometimes for ever for unknown reasons (futex_abstimed_wait)
            """
            for i in range(n_jobs):
                sim_names = sims_started[i * self.bundle_size:(i + 1) * self.bundle_size]
                jobid = self.submit_job(sim_names)
                self.running[jobid] = Job(self.type, jobid, sim_names)
                 LOGGER.debug(f'[{self.type}] Started job {} for {}'.format(jobid, sim_names))
            """
            for simname in sims_started:
                # Note that this doesn't actually write the script to the filesystem
                _simname, cmd_script, step = self.write_script(simname)
                LOGGER.debug(f"[{self.type}] submitting script {simname} {cmd_script}")
                # submit cmd_script to adapter and append (jobid, simname) to queue
                submit_record = self.adapter.submit(step, cmd_script, self.workspace)

                # Allow it to fail and attempt cleanup
                if not submit_record or submit_record.submission_code != SubmissionCode.OK:
                    LOGGER.error(
                        f"[{self.type}] Failed to submit a {self.type} job for simname = {simname}"
                    )
                    self.adapter.cleanup(step.name)                    
                    continue

                job_id = submit_record.job_identifier
                self.running[job_id] = Job(self.type, job_id, [simname])
                LOGGER.debug(f"[{self.type}] Started job {job_id} for {simname}")

            LOGGER.info(f"[{self.type}] START_JOB -- Ended Pooled Script Generation")

            # ----------------------------------------------------------------------
            LOGGER.info(f"[{self.type}] Started {n_jobs} jobs: {self.__str__()}")
            self.write_history("started", sims_started, "start_jobs")
            LOGGER.info(f"[{self.type}] Finished writing")

        else:
            LOGGER.info(f"[{self.type}] Scheduling disabled")
            for simname in sims_started:
                job_id = uuid.uuid4().int
                self.running[job_id] = Job(self.type, job_id, [simname])
                self.jobCnt += 1  # Probably not needed.

        assert self.nrunning_jobs() <= self.max_jobs_total
        LOGGER.info(f"[{self.type}] returning {n_jobs} {sims_started}")
        return n_jobs, sims_started

    # --------------------------------------------------------------------------
    def write_script(self, sims_chunk: str):
        """Create a Maestro study step and cmd_script"""

        assert self.do_scheduling == True

        LOGGER.debug(f"[{self.type}] Creating step for {sims_chunk}...")
        step = self.create_step([sims_chunk])
        LOGGER.debug(f"[{self.type}] Step created: {step}")
        _, components = self.adapter.write_script(self.workspace, step)
        return sims_chunk, components, step

    # --------------------------------------------------------------------------
    def update(self):
        """Check all running jobs to update the status of the tracker.
        Returns:
            sims_success = []:      simulations that have finished successfully
            sims_failed = []:       simulations that have failed
        """
        if self.nrunning_jobs() == 0:
            LOGGER.debug(
                f"[{self.type}] Returning because have no running jobs: {self.__str__()}"
            )
            return [], []

        LOGGER.info(self.__str__())

        sims_success = []  # simulations that have finished successfully
        sims_failed = []  # simulations that have failed
        sims_continue = []  # simulations that need to be continued
        sims_stop = []  # Sims that have to stop (not interesting anymore)
        jobs_2_continue = []  # jobs that are still running
        jobs_2_reclaim = []  # jobs that either finished or failed
        jobs_2_cancel = []

        # Initialize termination status set.
        term_set = set([SimulationStatus.Failed, SimulationStatus.Stop])

        # get job statuses
        jobid_jobs: ItemsView[str, Job] = self.running.items()

        LOGGER.debug(f"[{self.type}] Fetching status for {len(self.running)} jobs")

        # Note that statuses is a group of kubernetes objects.
        # We need this to later map the actual status (failed/completed) to a sim status
        # job_statuses is List[Tuple[bool, bool, k8s_status]]
        all_statuses = self.get_jobs_statuses(self.running)

        # This is the original structure the function had, for consistency
        job_statuses = [x[0:2] for x in all_statuses]
        k8s_statuses = [x[2] for x in all_statuses]
        if job_statuses == []:
            LOGGER.error(
                f"[{self.type}] Failed to fetch status. "
                f"Reasons could be: not able to query the scheduler or Job IDs do not exist."
            )
            return [], []

        # NOTE: the flux example had a previous multiprocessing example here
        # We are using serial, it seems less error prone. I don't understand why
        # this is a list of lists.
        sim_statuses = [
            KubernetesTracker.unwrap_kubernetes_status(s) for s in k8s_statuses
        ]

        # look at each running job
        for i, (jobid, job) in enumerate(jobid_jobs):

            nsims = len(job.sims)
            assert nsims == self.bundle_size

            # ------------------------------------------------------------------
            # job status = True:    let it run
            #              False:   kill (if needed) and reclaim resources
            job_is_running, job_is_tout = job_statuses[i]

            if not job_is_running and job_is_tout:
                # TODO: this assumes chunk_size = 1
                sim_status = [SimulationStatus.Failed]
            else:
                # check the status of all sims in the bundle
                #       will continue only if status == unknown (not success/failed)
                sim_status = sim_statuses[i]

            # ------------------------------------------------------------------
            # if the job is still running and at least one sim needs to continue
            sims_continue_any = any([s == SimulationStatus.Unknown for s in sim_status])
            sims_term_cancel = all([s in term_set for s in sim_status])

            LOGGER.debug(
                "[JOBID %s, JOB %s] : Status: %s\t| Running? %s\t| Timedout? %s\t| Continue? %s\t| Cancel? %s\t|",
                jobid,
                job,
                str(sim_status),
                str(job_is_running),
                str(job_is_tout),
                str(sims_continue_any),
                str(sims_term_cancel),
            )

            if job_is_running and sims_continue_any:
                jobs_2_continue.append(jobid)
                continue

            # If the job is running but all underlying simulations have failed
            # reap the job and reclaim the resources.
            # Note from vsoch: I don't understand this case. Does it assume a timeout
            # and we need to continue? I don't think that could happen here, we do
            # not set a timeout on the pod. If we did, we couldn't continue.
            # Question for Loic.
            jobs_2_reclaim.append(jobid)
            if job_is_running and (sims_term_cancel):
                # otherwise, need to reclaim the resources from this job

                # Note from vsoch: we don't need to reclaim anything, but we can
                # explicitly delete the failed job (it will just disappear from the
                # kubectl get jobs interface, but it's basically already gone.
                jobs_2_cancel.append(jobid)

            # split the simulations of this job based on status
            _ss, _sf, _sc, _st = self.split_sims_on_status(job.sims, sim_status)
            LOGGER.debug(f"[{self.type}] sims: success = {_ss}")
            LOGGER.debug(f"[{self.type}] sims: failed = {_sf}")
            LOGGER.debug(f"[{self.type}] sims: continue = {_sc}")
            LOGGER.debug(f"[{self.type}] sims: stop = {_st}")

            sims_success.extend(_ss)
            sims_failed.extend(_sf)
            sims_continue.extend(_sc)
            sims_stop.extend(_st)

        # ----------------------------------------------------------------------
        njobs_continue = len(jobs_2_continue)
        njobs_reclaim = len(jobs_2_reclaim)
        njobs_cancel = len(jobs_2_cancel)
        nsims_success = len(sims_success)
        nsims_failed = len(sims_failed)
        nsims_continue = len(sims_continue)
        nsims_stop = len(sims_stop)

        LOGGER.info(
            f"[{self.type}] processed all jobs. "
            f"(#jobs: continue = {njobs_continue}, reclaim = {njobs_reclaim}, cancel = {njobs_cancel}), "
            f"(#sims: success = {nsims_success}, failed = {nsims_failed}, continue = {nsims_continue}, stop = {nsims_stop})"
        )

        # TODO: where is this written? if to filesystem, probably should nix
        self.write_history("found_success", sims_success, "update")
        self.write_history("found_failed", sims_failed, "update")
        self.write_history("found_stop", sims_stop, "update")

        assert njobs_continue + njobs_reclaim == len(self.running)
        assert (
            nsims_success + nsims_failed + nsims_continue + nsims_stop
            == self.bundle_size * njobs_reclaim
        )

        # when working with a bundle size of 1
        # should not have to cancel a job and find a sim to continue
        if self.bundle_size == 1 and nsims_continue > 0:
            LOGGER.warning(
                f"[{self.type}] Found {nsims_continue} sims to continue for "
                f"bundle_size = {self.bundle_size}: Looks like these sims ended without a flag: {sims_continue}"
            )
            # TODO: We need to check that these jobs have actually ended, they could also be SCHEDULED or PENDING

        # ----------------------------------------------------------------------
        # kill the jobs and remove from the list
        if jobs_2_cancel:
            LOGGER.info(
                f"[{self.type}] Cancelling simulations (njobs_cancel) = {njobs_cancel}"
            )
            self.cancel_jobs(jobs_2_cancel)

        # Note from vsoch: need to talk about this "reclaim" case I don't know what it means
        if njobs_reclaim > 0:
            LOGGER.debug(
                f"[{self.type}] Reclaiming jobs {jobs_2_reclaim} from running {self.running.keys()}"
            )
            for j in jobs_2_reclaim:
                if j in self.running:
                    self.running.pop(j)

        # requeue the sims that need to be continued (their job has ended)
        if nsims_continue > 0:
            self.add_to_queue(sims_continue, prepend=True)
            LOGGER.debug(f"[{job}] requeued nsims={nsims_continue} => {sims_continue}")

        # ----------------------------------------------------------------------
        LOGGER.info(self.__str__())

        # return the successful and failed sims for further pipeline
        return sims_success, sims_failed

    def get_jobs_statuses(self, jobs: Dict[str, Job]) -> List[Tuple[bool, bool]]:
        """
        Check status of this job via Kubernetes
        * False: if job is finished or failed (i.e., workflow can reclaim resources)
        * True:  otherwise (for unknown status, we do not want to reclaim)
        """
        # -- job status code
        # OK                    # could query the job properly
        # NOJOBS                # queried, but job not found
        # ERROR                 # could not query the scheduler

        invalid_codes = {JobStatusCode.NOJOBS, JobStatusCode.ERROR}

        # -- valid states
        # State.INITIALIZED,    # maestro initialized, waiting to submit
        # State.PENDING,        # pending start (in the scheduler)
        # State.WAITING,        # waiting for resources (in the scheduler)
        # State.RUNNING,
        # State.FINISHING,
        # State.QUEUED,         # queued (in the scheduler)

        # -- invalid states
        # State.FINISHED,
        # State.FAILED,
        # State.INCOMPLETE,     # not currently used in maestro
        # State.HWFAILURE,
        # State.TIMEDOUT,
        # State.UNKNOWN,
        # State.CANCELLED

        invalid_states = {
            State.FINISHED,
            State.FAILED,
            State.INCOMPLETE,
            State.HWFAILURE,
            State.TIMEDOUT,
            State.CANCELLED,
            State.UNKNOWN,
            State.NOTFOUND,
        }

        # These ids are associated with our job_name_lookup
        jobIds = list(jobs.keys())

        # Note that despite variable naming, these are groups of statuses
        retcode, job_status, k8s_status = self.adapter.check_jobs(jobIds)
        for jobId in jobIds:

            # We had the job cached, don't know about it anymore
            # Not sure if this is possible in production, triggered in testing
            # a gazillion times. Likely had a stale state
            if jobId not in job_status:
                job_status[jobId] = State.UNKNOWN
            LOGGER.debug(f"{jobs[jobId]} => {job_status[jobId]}")

        if retcode in invalid_codes:
            LOGGER.warning(
                f"[{self.type}] Returning due to invalid code. [Code={retcode} : jobid={jobIds}]"
            )
            return []

        return [
            (
                job_status[jobId] not in invalid_states,
                job_status[jobId] == State.TIMEDOUT,
                k8s_status[jobId],
            )
            for jobId in jobIds
        ]

    # --------------------------------------------------------------------------
    def is_job_running(self, jobId):
        """
        Check status of this job via Maestro
        * False: if job is finished or failed (i.e., workflow can reclaim resources)
        * True:  otherwise (for unknown status, we do not want to reclaim)
        """
        if not self.do_scheduling:
            LOGGER.debug(f"[{self.type}] Returning, adapter is None.")
            return False, False

        # See job_statuses for valid and invalid states
        invalid_codes = {JobStatusCode.NOJOBS, JobStatusCode.ERROR}
        invalid_states = {
            State.FINISHED,
            State.FAILED,
            State.INCOMPLETE,
            State.HWFAILURE,
            State.TIMEDOUT,
            State.CANCELLED,
            State.UNKNOWN,
            State.NOTFOUND,
        }

        # now check the status via maestro
        retcode, job_status, _ = self.adapter.check_jobs([jobId])
        LOGGER.debug(
            f"[{self.type}] Received job status from Maestro: retcode = {retcode}, jobId = {jobId}, state = {job_status}"
        )

        if retcode in invalid_codes:
            LOGGER.warning(
                f"[{self.type}] Returning due to invalid code. [Code={retcode} : jobid={jobId}]"
            )
            return False, False

        return (
            job_status[jobId] not in invalid_states,
            job_status[jobId] == State.TIMEDOUT,
        )
