/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	mummi "github.com/converged-computing/mummi-operator/api/v1alpha1"
)

// MiniMummiReconciler reconciles a MiniMummi object
type MiniMummiReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	RESTClient rest.Interface
	RESTConfig *rest.Config
}

// +kubebuilder:rbac:groups=mummi.converged-computing.org,resources=minimummis,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mummi.converged-computing.org,resources=minimummis/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mummi.converged-computing.org,resources=minimummis/finalizers,verbs=update

//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods/log,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods/exec,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources="",verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources="services",verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=networking.k8s.io,resources="ingresses",verbs=get;list;watch;create;update;patch;delete

// These are added anticipating the operator could support the controller (wfmanager)
//+kubebuilder:rbac:groups=core,resources=batch,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=core,resources=networks,verbs=create;patch

//+kubebuilder:rbac:groups="",resources=events,verbs=create;watch;update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete;exec
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get;list;watch;create;update;patch;delete;exec

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MiniMummi object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/reconcile
func (r *MiniMummiReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Create a new MetricSet
	var spec mummi.MiniMummi

	// Keep developer informed what is going on.
	logger.Info("🦛 Event received by MiniMummi controller!")
	logger.Info("Request: ", "req", req)

	// Does the metric exist yet (based on name and namespace)
	err := r.Get(ctx, req.NamespacedName, &spec)
	if err != nil {

		// Create it, doesn't exist yet
		if errors.IsNotFound(err) {
			logger.Info("🟥️ MiniMummi not found. Ignoring since object must be deleted.")

			// This should not be necessary, but the config map isn't owned by the operator
			return ctrl.Result{}, nil
		}
		logger.Info("🟥️ Failed to get MiniMummi. Re-running reconcile.")
		return ctrl.Result{Requeue: true}, err
	}

	// Show parameters provided and validate one flux runner
	if !spec.Validate() {
		logger.Info("🟥️ Your MiniMummi config did not validate.")
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MiniMummiReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mummi.MiniMummi{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}
