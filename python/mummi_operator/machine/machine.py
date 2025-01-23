import random

from statemachine import State, StateMachine
from statemachine.factory import StateMachineMetaclass
from statemachine.utils import run_async_from_sync
import mummi_operator.tracker as tracker
import math


def create_mummi_state_machine(definition: dict, **extra_kwargs):
    """
    Create a MummiStateMachine class from a definition.
    """
    states_instances = {
        state_id: State(**state_kwargs) for state_id, state_kwargs in definition["states"].items()
    }

    events = {}
    for event_name, transitions in definition["events"].items():
        for transition_data in transitions:
            source = states_instances[transition_data["from"]]
            target = states_instances[transition_data["to"]]

            transition = source.to(
                target,
                event=event_name,
                cond=transition_data.get("cond"),
                unless=transition_data.get("unless"),
            )

            if event_name in events:
                events[event_name] |= transition
            else:
                events[event_name] = transition

    attrs_mapper = {**extra_kwargs, **states_instances, **events}
    return StateMachineMetaclass("MummiStateMachine", (StateMachine,), attrs_mapper)


def next_step_config(self, current_name):
    """
    Get the config for the next step.
    """
    # If we are completed or at start, go back to first step
    if current_name == "completed" or current_name == "start":
        next_step = self.workflow.first_step
    else:
        next_step = self.workflow.get_next_step(current_name)

    # Otherwise get next step
    return self.workflow.config_for_step(next_step)


def on_enter_start(self):
    """
    On start, prepare to keep track of jobs completed
    """
    if not hasattr(self, "trackers"):
        self.init_trackers()
    if not hasattr(self, "completed"):
        self.completed = {}

    import IPython

    IPython.embed()

    # Simple algorithm to start:
    # 1. Pack the max number of "next step" (first step) into max size
    # This will use the namespace the workflow manager is running in
    jobs = tracker.list_jobs()

    # 2. TODO account for pending / running jobs here...
    # This assumes the max size set by the user accounts for other stuff in cluster
    # If we underestimate, we will just have pending jobs
    
    # TODO need to account for GPU / not GPU, right now we consider just nodes
    next_step = self.next_step_config('start')
    nodes_needed = next_step.get('nnodes', 1)
    
    # pack max into available
    # TODO there are THREE places to get the name now, need to consolidate
    submit_n = math.floor(self.workflow.max_size / nodes_needed)
    for i in range(submit_n):
        self.send(next_step['jobname'])
    

def init_trackers(self):
    """
    Create a job tracker for each job.
    """
    self.trackers = {}
    for state_name, state in self.states_map.items():
        if state_name in ["start", "complete"]:
            continue
        self.trackers[state_name] = tracker.KubernetesTracker(
            state_name, self.workflow
        )


def on_change(self, job):
    """
    On each job finish, re-assess.
    """
    print(job)
    print("ON JOB FINISH")
    import IPython

    IPython.embed()


def is_complete(self, job_name, count) -> bool:
    """
    Return true if we have completed the desired count or more.
    """
    return self.completed.get(job_name, 0) >= count

    # TODO need to define an on start initial function that gets currnet cluster state
    # def before_transition(self, event, state):
    #    print(f"Before '{event}', on the '{state.id}' state.")
    #    return "before_transition_return"

    # def on_transition(self, event, state):
    #    print(f"On '{event}', on the '{state.id}' state.")
    #    return "on_transition_return"

    # def on_exit_state(self, event, state):
    #    print(f"Exiting '{state.id}' state from '{event}' event.")

    # def on_enter_state(self, event, state):
    #    print(f"Entering '{state.id}' state from '{event}' event.")

    # def after_transition(self, event, state):
    #    print(f"After '{event}', on the '{state.id}' state.")


def new_mummi_state_machine(config):
    """
    New mummi state machine creates a new Mummi Workflow state machine.
    """
    states = {"start": {"initial": True, "final": False}}
    events = {"change": []}
    last = None
    for i, job in enumerate(config.jobs):
        states[job] = {"initial": False, "final": False}
        if i != 0:
            # events[f"{last}_finish"] = [{"from": last, "to": job}]
            events["change"].append({"from": last, "to": job})
        else:
            events["change"].append({"from": "start", "to": job})
        last = job

    # Add last state (completed) and transition to it
    states["complete"] = {"initial": False, "final": True}
    events["change"].append({"from": last, "to": "complete"})
    return create_mummi_state_machine(
        {
            "states": states,
            "events": events,
        },
        is_complete=is_complete,
        on_change=on_change,
        on_enter_start=on_enter_start,
        init_trackers=init_trackers,
        workflow=config,
        next_step_config=next_step_config,
    )


"""


    async def is_hot(self, temperature: int):
        return temperature > 25

    async def is_good(self, temperature: int):
        return temperature < 20

    async def is_cool(self, temperature: int):
        return temperature < 18

    async def after_transition(self, event: str, source: State, target: State, event_data):
        print(f"Running {event} from {source!s} to {target!s}: {event_data.trigger_data.kwargs!r}")
"""
