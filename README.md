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
aws eks update-kubeconfig --region us-east-2 --name topology-study
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

For non-development

```bash
kubectl apply -f examples/dist/mummi-operator.yaml
```

## 4. Deploy an Example Mini Mummi

```bash
kubectl apply -f examples/test-aws/mummi.yaml 
```

## Design

These are some design decisions I've made:

 - internal: all of the controller logic, etc. should be internal
 - variables and functions to derive customization for Mummi should all derive from the spec (e.g., so the many templates can be populate just using it)
 - instead of all assets for a deployment in one config map or secret, I am separating them out. This will allow more pointed update (if needed) and more transparency to the developer user.

### TODO

- Verify MLServer needs exactly one node, add affinity, what about wfmanager?
- oras:
  - should be setup to handle with https / ssl
  - allow for customize of port
  - allow for using external artifact registry
- wfmanager
 - ensure we export envars 
 - when mummi python cloneable, can install (clone) on demand
- jobs:
 - if these are volume mounts into the wfmanager container, they should be moved
- mlserver model should eventually be customizable (currently built into container)
- rabbitmq and wfmanager: My certificate generation is off - I am missing the p12 files (need to be generated in go). It generates handshake error. Disabled for now but needs to be reenabled by adding the cert file back.

### Questions

- Logging: I think we might want to double check if it's doing anything. I've changed levels and I don't see much difference.
- What is the difference between `MUMMI_ROOT` and `MUMMI_APP`? The second makes sense (e.g., /opt/clones/mummi-ras) but the first is always set to the second. I'd expect it be something like /opt/clones where there are more assets.
- Should `OMP_NUM_THREADS` in the job entrypoints coincide with cores_per_task in the config?
- The wfmanager has a currently empty environment variable section. What is that for?
- What does wfmanager->is_gc mean? Is garbage collecting?
- What do each of the following mean (I am guessing th == threshold? I want to have descriptive variables)
  - fbaa_hvr_th
  -	fbaa_crd_th
  - fbaa_frame_increment 
- I'm still not sure about purpose (and need for) `/opt/clones/mummi-ras/macro/simlist.spec`. It seems like I shouldn't need it? I haven't fully tested without it, I know there is minimally a warning without it. What is it?

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
