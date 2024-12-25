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
	"github.com/converged-computing/mummi-operator/internal/controller/mlserver"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// createCerts creates a secret with cert.pem and key.pem
func (r *MiniMummiReconciler) ensureMLServer(
	ctx context.Context,
	spec *api.MiniMummi,
) (ctrl.Result, error) {

	// Create the config map entrypoint first
	cm := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: spec.MLServerName(), Namespace: spec.Namespace}, cm)
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = r.createMLServerEntrypoint(ctx, spec)
		}
		return ctrl.Result{}, err
	}

	// Create the MLServer deployment
	existing := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: spec.MLServerName(), Namespace: spec.Namespace}, existing)
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = r.createMLServer(ctx, spec)
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, err
}

// createMLServer creates the MLserver
func (r *MiniMummiReconciler) createMLServer(
	ctx context.Context,
	spec *api.MiniMummi,
) (*appsv1.Deployment, error) {

	deployment := mlserver.NewMLServerDeployment(spec)
	ctrl.SetControllerReference(spec, deployment, r.Scheme)
	err := r.Create(ctx, deployment)
	if err != nil {
		mLog.Error(err, "🔴 Create mlserver deployment", "Name", spec.MLServerName())
	}
	return deployment, err
}

// createMLServerEntrypoint creates the entrypoint configmap
func (r *MiniMummiReconciler) createMLServerEntrypoint(
	ctx context.Context,
	spec *api.MiniMummi,
) (*corev1.ConfigMap, error) {

	// Return data for the MLServer entrypoint:
	// entrypoint.sh
	// kubernetes_start.sh
	data, err := mlserver.NewEntrypoint(spec)
	if err != nil {
		return nil, err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.MLServerName(),
			Namespace: spec.Namespace,
		},
		Data: data,
	}

	ctrl.SetControllerReference(spec, cm, r.Scheme)
	err = r.Create(ctx, cm)
	if err != nil {
		mLog.Error(err, "🔴 Create mlserver entrypoint configmap", "Name", spec.MLServerName())
	}
	return cm, err
}
