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
func (r *MiniMummiReconciler) ensureRabbitMQCerts(
	ctx context.Context,
	spec *api.MiniMummi,
) (ctrl.Result, error) {

	// Create the secret that will hold certs and the rabbitmq.conf
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

	// Generate the rabbitmq.conf
	conf, err := rabbitmq.NewRabbitConfig(spec)
	if err != nil {
		return nil, err
	}

	// Generate rabbitmq-credentials.json
	creds, err := rabbitmq.NewRabbitCredentials(spec)
	if err != nil {
		return nil, err
	}

	// Create the rabbitMQ secret with certificates
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: spec.RabbitSecretName(), Namespace: spec.Namespace},
		Data: map[string][]byte{
			"client_rabbitmq_certificate.pem": cert.Client,
			"server_rabbitmq_key.pem":         cert.Certificate,
			"server_rabbitmq_certificate.pem": cert.Key,
			"ca_certificate.pem":              cert.CA,
			"rabbitmq.conf":                   []byte(conf),
			"rabbitmq-credentials.json":       []byte(creds),
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
func (r *MiniMummiReconciler) ensureRabbitMQ(
	ctx context.Context,
	spec *api.MiniMummi,
) (ctrl.Result, error) {

	// Check for an existing rabbitmq configmap (contains entrypoint)
	existing := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: spec.RabbitName(), Namespace: spec.Namespace}, existing)
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = r.createRabbitEntrypoint(ctx, spec)
		}
		return ctrl.Result{}, err
	}

	// Check for an existing rabbitmq deployment
	deployment := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: spec.RabbitName(), Namespace: spec.Namespace}, deployment)
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = r.createRabbitDeployment(ctx, spec)
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, err
}

// createRabbitDeployment creates rabbitmq deployment
func (r *MiniMummiReconciler) createRabbitDeployment(
	ctx context.Context,
	spec *api.MiniMummi,
) (*appsv1.Deployment, error) {

	deployment := rabbitmq.NewRabbitDeployment(spec)
	ctrl.SetControllerReference(spec, deployment, r.Scheme)
	err := r.Create(ctx, deployment)
	if err != nil {
		mLog.Error(err, "🔴 Create rabbitmq deployment", "Name", spec.RabbitName())
	}
	return deployment, err
}

// createStatefulSet creates the actual registry stateful set
func (r *MiniMummiReconciler) createRabbitEntrypoint(
	ctx context.Context,
	spec *api.MiniMummi,
) (*corev1.ConfigMap, error) {

	// This entrypoint does not have customization with variables
	// It respects the development build (not breaking it) but
	// replacing it with updated files.
	entrypoint := rabbitmq.NewRabbitEntrypoint()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.RabbitName(),
			Namespace: spec.Namespace,
		},
		Data: map[string]string{
			"entrypoint.sh": entrypoint,
		},
	}

	ctrl.SetControllerReference(spec, cm, r.Scheme)
	err := r.Create(ctx, cm)
	if err != nil {
		mLog.Error(err, "🔴 Create rabbitmq entrypoint configmap", "Name", spec.RabbitName())
	}
	return cm, err
}
