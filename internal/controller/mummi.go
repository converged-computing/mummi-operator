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

// ensureMiniMummi creates a new MiniMummi
func (r *MiniMummiReconciler) ensureMiniMummi(
	ctx context.Context,
	spec *api.MiniMummi,
) (ctrl.Result, error) {

	// Create headless service for the MiniCluster OR single service for the broker
	result, err := r.exposeServices(ctx, spec)
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
		result, err := r.createRegistry(ctx, spec)
		if err != nil {
			return result, err
		}
	}

	// Create certificates as secrets to mount (not mounted yet)
	result, err = r.ensureRabbitMQCerts(ctx, spec)
	if err != nil {
		return result, err
	}

	// Create rabbitmq deployment
	result, err = r.ensureRabbitMQ(ctx, spec)
	if err != nil {
		return result, err
	}

	// Create MLServer deployment
	// The data is currently built into container - maybe should be volume instead
	result, err = r.ensureMLServer(ctx, spec)
	if err != nil {
		return result, err
	}

	// Create the wfmanager deployment
	result, err = r.ensureWorkflowManager(ctx, spec)
	if err != nil {
		return result, err
	}

	// TODO: look into labels for autoscaler for jobs that wfmanager creates
	return ctrl.Result{}, nil
}
