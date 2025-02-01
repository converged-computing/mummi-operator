# mummi operator

> state machine orchestrator for the Mummi workflow

![PyPI - Version](https://img.shields.io/pypi/v/mummi-operator)

An HPC ensemble is an orchestration of jobs that can ideally be controlled by an algorithm. The original Mummi project (now open source) relied on underlying submission tools like Flux and Maestro, with resource needs hard generally hard-coded, and a lot of manual orchestration. This effort aims to design a workflow tool that is more akin to a state machine, and responds to different events. Instead of hard coding specific applications, we allow for them to be defined dynamically. This means that although the tool is designed for Mummi, there is no reason it would not be able to support other analyses. Some assumptions we make:

- An order of steps, A->B, understands how to handle output from the previous step. E.g., if we package up the output of A and give it to B in a known working directory, B knows what to do.
- Similarly, B knows that whatever is placed in that working directory will be provided to the next step.

This project will be intended to run in Kubernetes, because we are developing user-space Kubernetes for our HPC clusters and I want to start simple.

## Prototype Config

Here is an example config to provide to the workflow manager. It should provide the directory of configs, and the names for steps, OR the full paths to each. The steps should be provided in their expected order.

```yaml
# mummi-workflow.yaml
config_dir: /opt/clones/mummi-ras/specs/kubernetes-specs
cluster:
  max_size: 6
  autoscale: False
jobs:
  - name: mlserver
    config: mlserver.yaml
  - name: createsim
    config: jobs_createsim.yaml
  - name: cganalysis
    config: jobs_cganalysis.yaml
```

## Containers

A different design decision is packaging jobs as modular containers ([docker](docker)), which are defined for the workflow manager in [jobs](jobs). We build them from this context to add in the mummi-operator code.

```bash
kind create cluster --config ./kind-config.yaml
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 633731392008.dkr.ecr.us-east-1.amazonaws.com

docker build -f docker/mlrunner/Dockerfile -t 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:mlrunner .
kind load docker-image 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:mlrunner

docker build -f docker/wfmanager/Dockerfile -t 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:manager .
kind load docker-image 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:manager
```

This is how I'm testing. Note that for more customization we likely can use the operator.

```bash
kubectl apply -f ./examples
```

This has an added oras client (in Python) for pushing artifacts.

🚧 Under Construction! 🚧

## License

HPCIC DevTools is distributed under the terms of the MIT license.
All new contributions must be made under this license.

See [LICENSE](https://github.com/converged-computing/cloud-select/blob/main/LICENSE),
[COPYRIGHT](https://github.com/converged-computing/cloud-select/blob/main/COPYRIGHT), and
[NOTICE](https://github.com/converged-computing/cloud-select/blob/main/NOTICE) for details.

SPDX-License-Identifier: (MIT)

LLNL-CODE- 842614
