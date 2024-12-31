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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
	"github.com/converged-computing/mummi-operator/internal/controller/wfmanager"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// ensure the wfmaanger deployment is created
func (r *MiniMummiReconciler) ensureWorkflowManager(
	ctx context.Context,
	spec *api.MiniMummi,
) (ctrl.Result, error) {

	// Create the config map entrypoint first
	cm := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: spec.WFManagerName(), Namespace: spec.Namespace}, cm)
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = r.createWFManagerEntrypoint(ctx, spec)
		}
		return ctrl.Result{}, err
	}

	// Create the MLServer deployment
	existing := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: spec.WFManagerName(), Namespace: spec.Namespace}, existing)
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = r.createWFManager(ctx, spec)
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, err
}

// createMLServer creates the MLserver
func (r *MiniMummiReconciler) createWFManager(
	ctx context.Context,
	spec *api.MiniMummi,
) (*appsv1.Deployment, error) {

	deployment := wfmanager.NewWorkflowManagerDeployment(spec)
	ctrl.SetControllerReference(spec, deployment, r.Scheme)
	err := r.Create(ctx, deployment)
	return deployment, err
}

// createMLServerEntrypoint creates the entrypoint configmap
func (r *MiniMummiReconciler) createWFManagerEntrypoint(
	ctx context.Context,
	spec *api.MiniMummi,
) (*corev1.ConfigMap, error) {

	// Return data for the workflow manager entrypoint, along with all of the job specs
	// entrypoint.sh
	// kubernetes_start.sh
	// wfmanager.yaml
	// jobs_cg.yaml
	// jobs_createsim.yaml

	data, err := wfmanager.NewEntrypoint(spec)
	if err != nil {
		return nil, err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.WFManagerName(),
			Namespace: spec.Namespace,
		},
		Data: data,
	}

	ctrl.SetControllerReference(spec, cm, r.Scheme)
	err = r.Create(ctx, cm)
	return cm, err
}
