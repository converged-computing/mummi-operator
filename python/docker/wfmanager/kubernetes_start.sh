#!/usr/bin/env bash

# This is derived from:
# https://code.ornl.gov/sochat1/mummi-ras/-/blob/mini-mummi/setup/launch_workflow.sh

# assume one node for now. one node == one pod likely anyway.
mummi_nnodes=${1:-1}

# This is needed in the wfmanager
# I hard coded these for testing on my local machine
export MUMMI_MLSERVER_NNODES=1

# This needs to minimally be the number of jobs.. or you get a negative number (and error)
export MUMMI_NNODES=6
export NCORES_PER_NODE=4

# TODO look up MUMMI_HOST
# rebuild wfmanager ith gridsim2dras
# add logic here

MUMMI_APP=/opt/clones/mummi-ras
export MUMMI_ROOT=$MUMMI_APP
export MUMMI_RESOURCES=/opt/clones/mummi_resources
export MUMMI_APP

echo "(`hostname`: `date`) --> Launching MuMMI workflow ($mummi_nnodes nodes)"
pushd $MUMMI_ROOT/workspace > /dev/null 2>&1

# This normally was a flux submit, but we can just run it natively here.
echo "(`hostname`: `date`) --> Launching workflow"

# ------------------------------------------------------------------------------
# check the paths. these should be pointing to the virtual env
echo "mummi_core:" `python -c "import mummi_core; print (mummi_core.__path__)"`
echo "mummi_ras:"  `python -c "import mummi_ras; print (mummi_ras.__path__)"`

# mummi-manager start /mummi-workflow.yaml --config-dir=/opt/clones/mummi-ras/specs/kubernetes-mini
workflow_config="/opt/mummi-operator/jobs/mummi-workflow.yaml"
config_dir="/opt/mummi-operator/jobs"
cmd="mummi-manager start ${workflow_config} --config-dir=${config_dir} --manager-config=/wfmanager.yaml"
echo $cmd
$cmd
