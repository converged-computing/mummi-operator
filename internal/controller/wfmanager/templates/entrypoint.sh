#!/usr/bin/env bash

# Modified entrypoint to always run the Kubernetes entrypoint, after sourcing bash profile
. ~/.bash_profile

# Workflow manager also needs the client certificate
cp /mummi_operator/kubernetesTracker.py /opt/kubernetesTracker.py

# Certificate needed for rabbitmq (disabled while we don't use ssl)
# cp /cert_rabbitmq/client_rabbitmq_certificate.pem {{ .Spec.Paths.Certs }}/client_rabbitmq_certificate.pem
rm -rf {{ .Spec.Paths.Certs }}/client_rabbitmq_certificate.pem

# Jobs and wfmanager config
cp /mummi_operator/wfmanager.yaml {{ .Spec.Paths.MummiRoot }}/specs/kubernetes-mini/wfmanager.yaml
cp /mummi_operator/jobs_cg.yaml {{ .Spec.Paths.MummiRoot }}/specs/kubernetes-mini/jobs_cg.yaml
cp /mummi_operator/jobs_createsim.yaml {{ .Spec.Paths.MummiRoot }}/specs/kubernetes-mini/jobs_createsim.yaml

# Give rabbitmq time to boot up if both containers are present
# TODO improve this so we don't need it.
echo "Sleeping 20 seconds anticipating rabbitmq coming up..."
sleep 20

# Trigger interactive mode here so we have files staged above
{{ if .Spec.WorkflowManager.Interactive }}sleep infinity{{ end }}
pixi run /bin/bash /mummi_operator/kubernetes_start.sh
