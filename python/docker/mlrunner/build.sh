#!/bin/bash

export ENV_PREFIX=/pixi-env

# This must be installed before horovod
# NOTE I don't think we need this anymore, but will install anyway
#pip install mxnet-cu101mkl==1.6.*

# I think we want to do this (didn't work easily with pip install)
# https://horovod.readthedocs.io/en/stable/conda.html
# pip install horovod
# This is the recommended one from the article
# pip install horovod==0.19.*
# This is the one in spack.yaml
# pip install horovod==0.24.*
# We might want to start with a base that has it already
# https://horovod.readthedocs.io/en/stable/docker_include.html

cd /opt/clones/mummi-core
pip install -e .
cd /opt/clones/mummi-ras
# This is the correct branch
# git fetch
# git checkout campaign1-gromacs
pip install -e .
