#!/usr/bin/env bash

# Modified entrypoint to always run the Kubernetes entrypoint, after sourcing bash profile
. ~/.bash_profile

# Workflow manager also needs the client certificate
cp /mummi_operator/kubernetesTracker.py /opt/kubernetesTracker.py

# Certificate needed for rabbitmq
cp /cert_rabbitmq/client_rabbitmq_certificate.pem {{ .Spec.Paths.Certs }}/client_rabbitmq_certificate.pem

# Jobs and wfmanager config
cp /mummi_operator/wfmanager.yaml {{ .Spec.Paths.MummiRoot }}/specs/kubernetes-mini/wfmanager.yaml
cp /mummi_operator/jobs_cg.yaml {{ .Spec.Paths.MummiRoot }}/specs/kubernetes-mini/jobs_cg.yaml
cp /mummi_operator/jobs_createsim.yaml {{ .Spec.Paths.MummiRoot }}/specs/kubernetes-mini/jobs_createsim.yaml

pixi run /bin/bash /mummi_operator/kubernetes_start.sh