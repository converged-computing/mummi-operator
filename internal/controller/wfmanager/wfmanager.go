package wfmanager

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	mLog = ctrl.Log.WithName("mlserver")
)

// NewWorkflowManagerDeployment returns a new MLServer deployment based on the spec
func NewWorkflowManagerDeployment(spec *api.MiniMummi) *appsv1.Deployment {

	mLog.Info("Creating workflow manager deployment for: ", spec.WFManagerName(), spec.Namespace)

	// Prepare pull policy and selector. Use "Never" for pre-loaded image
	// Note that interactive mode is added in entrypoint after staging
	pullPolicy := corev1.PullPolicy(spec.Spec.WorkflowManager.ImagePullPolicy)
	command := []string{"/bin/bash", "/mummi_operator/entrypoint.sh"}
	selector := spec.Selector()

	// Environment needs to have the rabbit username and password
	env := []corev1.EnvFromSource{
		{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: spec.RabbitSecretName(),
				},
			},
		},
	}

	// MLServer container
	container := corev1.Container{
		Name:            "wfmanager",
		Image:           spec.Spec.WorkflowManager.Image,
		ImagePullPolicy: pullPolicy,
		Command:         command,
		EnvFrom:         env,

		// Entrypoint with entrypoint.sh and kubernetes_start.sh
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "wfmanager-entrypoint",
				MountPath: "/mummi_operator/",
			},
			{
				Name:      "rabbitmq",
				MountPath: "/cert_rabbitmq/",
			},
		},
	}

	// This configmap has entrypoint.sh and kubernetes_start.sh
	// It should be created before the ML Server deployment
	volumes := []corev1.Volume{
		{
			Name: "wfmanager-entrypoint",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: spec.WFManagerName(),
					},
				},
			},
		},
		{
			Name: "rabbitmq",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: spec.RabbitSecretName(),
					Items: []corev1.KeyToPath{
						// TODO: add this back when generated correctly
						//{
						//	Key:  "client_rabbitmq_certificate.pem",
						//	Path: "client_rabbitmq_certificate.pem",
						//},
						{
							Key:  "rabbitmq-credentials.json",
							Path: "rabbitmq-credentials.json",
						},
					},
				},
			},
		},
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.WFManagerName(),
			Namespace: spec.Namespace,
			Labels:    selector,
		},
		Spec: appsv1.DeploymentSpec{
			// Note that if we increase replicas, need to check if entrypoint needs to change
			Replicas: &spec.Spec.WorkflowManager.Replicas,

			// Match labels say which deployment a set of pods apply to
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: selector,
				},
				Spec: corev1.PodSpec{
					// The service account allows the pod to interact with the API
					ServiceAccountName: spec.Name,
					Subdomain:          spec.Name,
					Hostname:           "wfmanager",
					Containers:         []corev1.Container{container},
					Volumes:            volumes,
				},
			},
		},
	}
	if spec.Spec.WorkflowManager.NodeSelector != "" {
		nodeSelector := map[string]string{"node.kubernetes.io/instance-type": spec.Spec.WorkflowManager.NodeSelector}
		deployment.Spec.Template.Spec.NodeSelector = nodeSelector
	}
	return deployment
}
