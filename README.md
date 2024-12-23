# mummi-operator

> Hello! I'm the mini mummi! 🦛

The Mummi Operator is intended to run MiniMummi. 

## Usage

### Prerequisites

- go version v1.22.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### TODO

- Add debug mode for each, meaning we can start with a sleep
- rabbitmq should come from an operator or similar?
  - maybe not - a simple certificate generation and deployment is a safe way to start
  - likely we want to simply generate our own certs, and having a rabbitmq deployment that scales. [generate](https://go.dev/src/crypto/tls/generate_cert.go) and [deploy](https://github.com/rabbitmq/cluster-operator/tree/eda79247f6c30d98681706aa63213c8b32152e62/docs/examples/tls)
- oras:
  - should be setup to handle with https / ssl
  - allow for customize of port
  - allow for using external artifact registry
  - mlserver->workspace->path default (`mummi_ras.Naming.dir_root('mlserver')`)
- wfmanager
 - ensure we export envars 
 - when mummi python cloneable, can install (clone) on demand
 
### Questions

- What is the difference between `MUMMI_ROOT` and `MUMMI_APP`? The second makes sense (e.g., /opt/clones/mummi-ras) but the first is always set to the second. I'd expect it be something like /opt/clones where there are more assets.
- Should `OMP_NUM_THREADS` in the job entrypoints coincide with cores_per_task in the config?
- The wfmanager has a currently empty environment variable section. What is that for?
- What does wfmanager->is_gc mean? Is garbage collecting?
- What do each of the following mean (I am guessing th == threshold? I want to have descriptive variables)
  - fbaa_hvr_th
  -	fbaa_crd_th        
  - fbaa_frame_increment 

## License

HPCIC DevTools is distributed under the terms of the MIT license.
All new contributions must be made under this license.

See [LICENSE](https://github.com/converged-computing/cloud-select/blob/main/LICENSE),
[COPYRIGHT](https://github.com/converged-computing/cloud-select/blob/main/COPYRIGHT), and
[NOTICE](https://github.com/converged-computing/cloud-select/blob/main/NOTICE) for details.

SPDX-License-Identifier: (MIT)

LLNL-CODE- 842614
