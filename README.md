# mummi-operator

> Hello! I'm the mini mummi! 🦛

The Mummi Operator is intended to run MiniMummi. 

## Usage

### Prerequisites

- go version v1.22.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### 1. Create Cluster

You can create a cluster locally (if your computer is chonky and can handle it) or use AWS. Here is locally:

```bash
kind create cluster --config ./examples/kind-config.yaml
```

And for AWS (recommended for most cases):

```bash
eksctl create cluster --config-file examples/eks-config-6.yaml
aws eks update-kubeconfig --region us-east-2 --name mini-mummi

# Or with GPUs
eksctl create cluster --config-file examples/eks-config-gpu-6.yaml
aws eks update-kubeconfig --region us-east-1 --name mini-mummi-gpu
```

## 2. Load Images

> Kind Only

If you are using kind, you will want to load your images. If you are using AWS (and on our account with the registry) then you'll be able to pull them to the cluster. Note that we are going to load the images to make our lives easier (otherwise we need to include them with pull secrets). You might need to login and pull these first:

```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 633731392008.dkr.ecr.us-east-1.amazonaws.com
docker pull 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:rabbitmq
docker pull 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:mlserver
docker pull 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:wfmanager
docker pull 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:createsims
docker pull 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:cganalysis
```

And then load from your local machine. When we run this on EKS, we will likely have easy access to our private registry.

```bash
kind load docker-image 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:rabbitmq
kind load docker-image 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:mlserver
kind load docker-image 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:wfmanager
kind load docker-image 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:createsims
kind load docker-image 633731392008.dkr.ecr.us-east-1.amazonaws.com/mini-mummi:cganalysis
```

## 3. Install the Operator

The operator is built via its manifest in dist. For development:

```bash
make test-deploy-recreate
```

For non-development:

```bash
kubectl apply -f examples/dist/mummi-operator.yaml
```

## 4. Deploy an Example Mini Mummi

### a. Without GPU


```bash
# Without GPU
kubectl apply -f examples/test-aws/mummi.yaml 
```

### b. With GPU

Test that you see the GPU devices:

```bash
kubectl get nodes -o json | grep nvidia.com/gpu

# More specific
kubectl get nodes -o json | jq -r .items[].status.capacity | grep nvidia
```

### c. GPU Operator

If you are unfortunate enough to need to use this:

```bash
kubectl create ns gpu-operator
kubectl label --overwrite ns gpu-operator pod-security.kubernetes.io/enforce=privileged

helm repo add nvidia https://helm.ngc.nvidia.com/nvidia
helm repo update
helm install --wait --generate-name -n gpu-operator --create-namespace nvidia/gpu-operator --version=v24.9.1 --set driver.enabled=false

# Check labels and GPUs (you should see nvidia.com/gpu)
kubectl get pods -n gpu-operator
kubectl get nodes -o json | jq '.items[].metadata.labels'
kubectl apply -f examples/test-aws/gpu-mummi.yaml 
```

Here is how to get the charts installed to the namespace and uninstall:

```bash
# Show the name generated in the gpu-operator namesapce
helm list -n gpu-operator

# Uninstall the chart
helm uninstall -n gpu-operator gpu-operator-1736103287
```

Note that if the workflow manager isn't connecting, it's some race condition:

```console
[retry=1/100] No RPC server is listening on queue mummi_queue_mummiusr, retrying in 5 secondes ...
```

You should be able to delete the pod and it will be recreated.

```bash
kubectl delete pod  mummi-sample-wfmanager-64d87ddb87-6w8lb
```

If you want to delete the deployment, and note that jobs are not tied to the Mummi Operator (intentionally) so you can delete them separately:

```bash
kubectl delete -f examples/test-aws/mummi.yaml 

# Or for GPU
kubectl delete -f examples/test-aws/gpu-mummi.yaml 
kubectl delete jobs --all
```

That is done so if the workflow manager or mlserver (or another component) needs to be nuked, we won't lose running jobs.

## 5. Cleanup

```bash
eksctl delete cluster --config-file examples/eks-config-6.yaml --wait
eksctl delete cluster --config-file examples/eks-config-gpu-6.yaml --wait
```


## Design

These are some design decisions I've made (of course open to discussion):

 - state is derived from Kubernetes, and not relying on some filesystem state
 - internal: all of the controller logic, etc. should be internal
 - I'm trying to add kubernetes functionality in a way that doesn't disturb (change) core mummi. E.g., entrypoints and environment variables.
 - If/when the operator is deleted, jobs (createsim and cganalysis) are not. I think this might make sense if the orchestration needs update without destroying the jobs.
   - But discussion is needed, because if the registry is part of the mini mummi setup it will be deleted to. 
   - But the job state can be re-discovered by a newly deployed operator
 - variables and functions to derive customization for Mummi should all derive from the spec (e.g., so the many templates can be populate just using it)
 - instead of all assets for a deployment in one config map or secret, I am separating them out. This will allow more pointed update (if needed) and more transparency to the developer user.

### TODO

- We need to target deployments - e.g., rabbitmq does not need a GPU node. So we need:
  - A cluster config that creates some number of GPUs, and some number of non-GPU nodes.
  - A way to prevent the non gpu apps to be scheduled on GPU nodes.
- We likely want to test with a real registry OR allow a volume bind (existing data) to the registry.
  - Otherwise, artifacts deleted on cleanup. We could also have an option that allows keeping the ephemeral registry.
- Find source of warning `Unidentified hostname: wfmanager.mummi-sample.default.svc.cluster.local` in mummi-core
- Sometimes the wfmanager (starting too quickly after rabbitmq) fails to start, and I added a sleep to fix. A more robust solution is ideal.
- Verify MLServer needs exactly one node, add affinity, what about wfmanager?
- testing is needed for:
  - oras with external artifact registry
  - running jobs with GPU (need to discuss budget)
- wfmanager: when mummi python cloneable, can install (clone) on demand
- mlserver model should eventually be customizable (currently built into container)
- rabbitmq and wfmanager: My certificate generation is off - I am missing the p12 files (need to be generated in go). It generates handshake error. Disabled for now but needs to be reenabled by adding the cert file back.

### Questions

- Is mummi_core imported to init some state or can we remove it?
- Where is job tracker write_history written? If to the filesystem, doesn't make sense to keep (maybe should delete). What is goal?
- What does wfmanager->is_gc mean? Is garbage collecting?
- The wfmanager has a currently empty environment variable section. What is that for?
- I'm still not sure about purpose (and need for) `/opt/clones/mummi-ras/macro/simlist.spec`. It seems like I shouldn't need it? I haven't fully tested without it, I know there is minimally a warning without it. What is it?
- What do each of the following mean (I am guessing th == threshold? I want to have descriptive variables)
  - fbaa_hvr_th
  -	fbaa_crd_th
  - fbaa_frame_increment 
- What is the difference between `MUMMI_ROOT` and `MUMMI_APP`? The second makes sense (e.g., /opt/clones/mummi-ras) but the first is always set to the second. I'd expect it be something like /opt/clones where there are more assets.
  - Note that I am just taking in MUMMI_ROOT as a parameter and setting the app to that
- There is some state of "the job ended but the simuation needs to continue" that I want to avoid for this design. Can we assume a job can be given a long enough timelimit? If not, can we have a special exit code to indicate needs to continue? Or another marker?
  - How would restart happen in an ephemeral job? See kubernetesJobTracker.py when job created - there is restart path I think we can nix.
- Can we have some capture of "no change" for an iteration, and not increase the iteration count until there is?
- Why is the ML server not more tightly controlled as individual jobs?
  - There is a disconnect betweeen using the rabbit data to kick off work vs. always running the ML server first.
  - It takes 5 minutes to get enough samples to start createsims, but they are generated in seconds.
- I don't understand the "jobs to reclaim" use case - if a job times out, it will just fail and we can move on. What is reclaim for? And why/when cancel with timeout?
- I noticed jobs that seem frozen (after ~4 hours) is that expected? The walltime (duration of the job) will eventually fail them (note, we should set what we think are reasonable timeouts)
- Should we put a limit on what the ML server is outputting? I can limit the number that the wfmanager receives, but the MLServer keeps going. This can be problematic if it ovrwhelms the registry.
- What is the affinity of each createsim, etc? (this will help to set an upper limit for what is running)
- Should `OMP_NUM_THREADS` in the job entrypoints coincide with cores_per_task in the config?
- Under what conditions do we cancel / cleanup jobs?
- When should I do a PR to upstream mummi-ras? When everything working as we want?
- When do we cleanup old jobs / config maps? If we need them for state, we have to keep around.

## Debugging

### RabbitMQ

You can shell into the rabbitmq pod to test the connection:

```console
root@rabbitmq:/# openssl s_client -connect rabbitmq.mummi-sample.default.svc.cluster.local:5671 -servername rabbitmq.mummi-sample.default.svc.cluster.local
Connecting to 10.244.0.7
CONNECTED(00000003)
write:errno=104
---
no peer certificate available
---
No client certificate CA names sent
---
SSL handshake has read 0 bytes and written 355 bytes
Verification: OK
---
New, (NONE), Cipher is (NONE)
This TLS version forbids renegotiation.
Compression: NONE
Expansion: NONE
No ALPN negotiated
Early data was not sent
Verify return code: 0 (ok)
---
```

## License

HPCIC DevTools is distributed under the terms of the MIT license.
All new contributions must be made under this license.

See [LICENSE](https://github.com/converged-computing/cloud-select/blob/main/LICENSE),
[COPYRIGHT](https://github.com/converged-computing/cloud-select/blob/main/COPYRIGHT), and
[NOTICE](https://github.com/converged-computing/cloud-select/blob/main/NOTICE) for details.

SPDX-License-Identifier: (MIT)

LLNL-CODE- 842614
