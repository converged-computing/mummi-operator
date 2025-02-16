#!/usr/bin/env bash

# This could take arguments if needed, hard coding things for now :)

# For now assume we are running on 1 node per ML server (one pod).
# If we expand that, it will still be one pod per node.
mummi_mlserver_nnodes=1

# Note that the pixi container has three places with mummi_ras:
# These should be added to the container
MUMMI_APP=/opt/clones/mummi-ras
export MUMMI_ROOT=$MUMMI_APP
export MUMMI_RESOURCES=/opt/clones/mummi_resources
export MUMMI_APP
mlserver_cmd="mummi-ml start --number-samples 1 --config-dir=/opt/clones/mummi-ras/specs/kubernetes-mini"

# This is where the mini-mummi configs for mummi-ras expect to find the model
mkdir -p /opt/clones/extract
cd /opt/clones/extract
tar -xzvf /opt/clones/model.tar.gz
extracted=$(ls /opt/clones/extract)
cd -
mv /opt/clones/extract/${extracted} /opt/clones/mummi_resources/ml/chonky-model

# ------------------------------------------------------------------------------
NUM_THREADS=$(nproc)
export OMP_NUM_THREADS=$NUM_THREADS

# ------------------------------------------------------------------------------
# check the paths. these should be pointing to the virtual env
echo "mummi_core:" `python -c "import mummi_core; print (mummi_core.__path__)"`
echo "mummi_ras:"  `python -c "import mummi_ras; print (mummi_ras.__path__)"`

export KERAS_BACKEND='theano'
export OMP_NUM_THREADS=4
umask 007


echo
echo "(`hostname`: `date`) --> Launching ML Runner with $mummi_mlserver_nnodes nodes"
echo "   ML Server executable:   $mlserver_cmd"
echo "      Working directory:   `pwd`"
echo "                Threads:   $NUM_THREADS"
echo

python3 $MUMMI_APP/mummi_ras/scripts/create_organization.py
echo "$mlserver_cmd"
sh -c "$mlserver_cmd"
