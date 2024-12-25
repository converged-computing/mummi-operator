#!/usr/bin/env bash

# Modified entrypoint to always run the Kubernetes entrypoint, after sourcing bash profile
. ~/.bash_profile

# Copy the updated rabbitmq client certificate
cp /cert_rabbitmq/client_rabbitmq_certificate.pem {{ .Spec.Paths.Certs }}/client_rabbitmq_certificate.pem
cp /mummi_operator/mlserver.yaml {{ .Spec.Paths.MummiRoot }}/specs/kubernetes-mini/mlserver.yaml
pixi run /bin/bash /mummi_operator/kubernetes_start.sh