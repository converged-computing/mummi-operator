/*
Copyright 2025 Lawrence Livermore National Security, LLC
 (c.f. AUTHORS, NOTICE.LLNS, COPYING)

This is part of the Flux resource manager framework.
For details, see https://github.com/flux-framework.

SPDX-License-Identifier: Apache-2.0
*/

package controller

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
)

// This is a MiniCluster! A MiniCluster is associated with a running MiniCluster and include:
// 1. An indexed job with some number of pods
// 2. Config maps for secrets and other things.
// 3. We "launch" a job by starting the Indexed job on the connected nodes
// ensureMiniCluster creates a new MiniCluster, a stateful set for running flux!
func (r *MiniMummiReconciler) ensureMiniMummi(
	ctx context.Context,
	spec *api.MiniMummi,
) (ctrl.Result, error) {

	// Create headless service for the MiniCluster OR single service for the broker
	// The selector is how different objects (e.g,. deployment are added to the service)
	selector := map[string]string{"app": spec.Name}
	result, err := r.exposeServices(ctx, spec, selector)
	if err != nil {
		return result, err
	}

	// Create RBAC that will give wfmanager permission to create jobs
	// This means Role and RoleBinding that provide permission on the level of a namespace
	result, err = r.createRBAC(ctx, spec)
	if err != nil {
		return result, err
	}

	// Create the registry, only if we need to!
	// The selector is needed to add it to the headless service
	if spec.HasInClusterRegistry() {
		result, err := r.createRegistry(ctx, spec, selector)
		if err != nil {
			return result, err
		}
	}

	// TODO: rabbitmq and certs
	// TODO: mlserver (can be started first) - data to start built into image
	// TODO: wfmanager
	// TODO: look into labels for autoscaler for jobs that wfmanager creates
	return ctrl.Result{}, nil
}
