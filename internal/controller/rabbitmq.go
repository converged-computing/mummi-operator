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
	"github.com/converged-computing/mummi-operator/internal/controller/certs"
	"github.com/converged-computing/mummi-operator/internal/controller/rabbitmq"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// createCerts creates a secret with cert.pem and key.pem
func (r *MiniMummiReconciler) createRabbitMQCerts(
	ctx context.Context,
	spec *api.MiniMummi,
) (ctrl.Result, error) {

	// Create either the headless service or broker service
	existing := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: spec.RabbitSecretName(), Namespace: spec.Namespace}, existing)
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = r.createRabbitMQSecret(ctx, spec)
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, err
}

// createSecret creates the certificates and secret for rabbitmq
func (r *MiniMummiReconciler) createRabbitMQSecret(
	ctx context.Context,
	spec *api.MiniMummi,
) (*corev1.Secret, error) {

	// Generate certificates for the rabbitmq host
	cert, err := certs.GenerateCertificates(spec.RabbitHost())
	if err != nil {
		return nil, err
	}

	// Create the rabbitMQ secret with certificates
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: spec.RabbitSecretName(), Namespace: spec.Namespace},
		Data: map[string][]byte{
			"server_rabbitmq_key.pem":         cert.Certificate,
			"server_rabbitmq_certificate.pem": cert.Key,
			"ca_certificate.pem":              cert.CA,
		},
	}
	ctrl.SetControllerReference(spec, secret, r.Scheme)
	err = r.Create(ctx, secret)
	if err != nil {
		mLog.Error(err, "🔴 Create rabbitmq certificate secret", "Name", spec.Name)
	}
	return secret, err
}

// createRabbitMQ creates the rabbitMQ deployment, if it doesn't exist
// This takes into account:
// 1. Secrets for the server certificate, etc.
// 2. Configmaps for the entrypoint, customized for it
func (r *MiniMummiReconciler) createRabbitMQ(
	ctx context.Context,
	spec *api.MiniMummi,
) (ctrl.Result, error) {

	// TODO need to generate config maps / secrets here for:
	// - entrypoint (matched to the entrypoint here)
	// - rabbitmq.conf (that takes the username / password)
	//     maybe we should generate it on the fly? Take from user yaml? something else?

	// Check for an existing rabbitmq deployment
	existing := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: spec.RabbitDeploymentName(), Namespace: spec.Namespace}, existing)
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = r.createRabbitDeployment(ctx, spec)
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, err
}

// createStatefulSet creates the actual registry stateful set
func (r *MiniMummiReconciler) createRabbitDeployment(
	ctx context.Context,
	spec *api.MiniMummi,
) (*appsv1.Deployment, error) {

	deployment := rabbitmq.NewRabbitDeployment(spec)
	ctrl.SetControllerReference(spec, deployment, r.Scheme)
	err := r.Create(ctx, deployment)
	if err != nil {
		mLog.Error(err, "🔴 Create rabbitmq deployment", "Name", spec.RabbitDeploymentName())
	}
	return deployment, err
}
