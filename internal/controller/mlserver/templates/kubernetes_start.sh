#!/usr/bin/env bash

# For now assume we are running on 1 node per ML server (one pod).
# The way to scale this is with replicas, but each is still one node
# If we expand that, it will still be one pod per node.
mummi_mlserver_nnodes=${1:-1}

# Use provided container mummi_ras
MUMMI_ROOT={{ if .Spec.Paths.MummiRoot }}{{ .Spec.Paths.MummiRoot }}{{ else }}/opt/clones/mummi-ras{{ end }}
MUMMI_APP={{ if .Spec.Paths.MummiApp }}{{ .Spec.Paths.MummiApp }}{{ else }}/opt/clones/mummi-ras{{ end }}
export MUMMI_RESOURCES={{ if .Spec.Paths.MummiResources }}{{ .Spec.Paths.MummiResources }}{{ else }}/opt/clones/mummi_resources{{ end }}
export MUMMI_APP MUMMI_ROOT

mlserver_exe="python3 $MUMMI_APP/mummi_ras/scripts/run_mlserver.py"
mlserver_cmd=""

# This is where the mini-mummi configs for mummi-ras expect to find the model
# It is currently built into the container, and this should be updated
mkdir -p /opt/clones/extract
cd /opt/clones/extract
tar -xzvf /opt/clones/model.tar.gz
extracted=$(ls /opt/clones/extract)
cd -
mv /opt/clones/extract/${extracted} /opt/clones/mummi_resources/ml/chonky-model

# ------------------------------------------------------------------------------
NUM_THREADS=$(nproc)
export OMP_NUM_THREADS=$NUM_THREADS

# Rabbit mq connection stuff
# rabbitmq.mini-mummi.default.svc.cluster.local
WABBIT_HOST={{ .Mummi.RabbitHost }}

# This gets the rabbitmq script if we need it (I didn't use it yet)
# Note that this is the insecure port, we can test changing this to 15671
curl http://${WABBIT_HOST}:15672/cli/rabbitmqadmin -o /usr/local/bin/rabbitmqadmin
chmod +x /usr/local/bin/rabbitmqadmin

# Create dummy credentials
mkdir -p $MUMMI_ROOT/mlserver

# Tidbits from the setup_env.sh script

# ------------------------------------------------------------------------------
# check the paths. these should be pointing to the virtual env
echo "mummi_core:" `python -c "import mummi_core; print (mummi_core.__path__)"`
echo "mummi_ras:"  `python -c "import mummi_ras; print (mummi_ras.__path__)"`

export KERAS_BACKEND='theano'
export OMP_NUM_THREADS={{ if .Spec.MLServer.Config.Threads }}{{ .Spec.MLServer.Config.Threads }}{{ else }}4{{ end }}
umask 007

# Test if the packages are correctly installed in the container
echo
python3 $MUMMI_APP/tests/test-packaging.py

# Copy the secret generated file to where it needs to be
# Needs to be port 5761 for TLS, 5762 will issue an invalid version error
cp /cert_rabbitmq/rabbitmq-credentials.json $MUMMI_ROOT/mlserver/rabbitmq-credentials.json

echo
echo "(`hostname`: `date`) --> Launching ML Server with $mummi_mlserver_nnodes nodes"
echo "   ML Server executable:   $mlserver_exe"
echo "      Working directory:   `pwd`"
echo "                Threads:   $NUM_THREADS"
echo

python3 $MUMMI_APP/mummi_ras/scripts/create_organization.py

# vim /pixi-env/.pixi/env/lib/python3.11/site-packages/mummi_core/brokers/rabbitmq_rpc.py 107
# One of these is the rabbit client certificate!
# /opt/clones/certs/client_rabbitmq_certificate.pem
# /opt/clones/certs/ca_certificate.pem

# We already have gromacs installed (whereis gmx)
echo "$mlserver_exe $mlserver_cmd"
sh -c "$mlserver_exe $mlserver_cmd"
