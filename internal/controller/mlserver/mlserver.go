package mlserver

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	mLog = ctrl.Log.WithName("mlserver")
)

// NewMLServerDeployment returns a new MLServer deployment based on the spec
func NewMLServerDeployment(spec *api.MiniMummi) *appsv1.Deployment {

	mLog.Info("Creating mlserver deployment for: ", spec.MLServerName(), spec.Namespace)

	// Prepare pull policy and selector. Use "Never" for pre-loaded image
	// Note that interactive mode is added in entrypoint after staging
	pullPolicy := corev1.PullPolicy(spec.Spec.MLServer.ImagePullPolicy)
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
		Name:            "mlserver",
		Image:           spec.Spec.MLServer.Image,
		ImagePullPolicy: pullPolicy,
		Command:         command,
		EnvFrom:         env,

		// Entrypoint with entrypoint.sh and kubernetes_start.sh
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "mlserver-entrypoint",
				MountPath: "/mummi_operator/",
			},
			{
				Name:      "rabbitmq",
				MountPath: "/cert_rabbitmq/",
			},
		},
	}

	// Custom working directory
	if spec.Spec.MLServer.Workdir != "" {
		container.WorkingDir = spec.Spec.MLServer.Workdir
	}

	// Are we asking for GPU?
	if spec.Spec.MLServer.Config.Gpus > 0 {
		labelName, labelValue := spec.GetGPULabel(spec.Spec.MLServer.Config.Gpus)
		gpuResource := corev1.ResourceList{
			corev1.ResourceName(labelName): resource.MustParse(labelValue),
		}
		container.Resources = corev1.ResourceRequirements{
			Limits:   gpuResource,
			Requests: gpuResource,
		}
	}

	// This configmap has entrypoint.sh and kubernetes_start.sh
	// It should be created before the ML Server deployment
	volumes := []corev1.Volume{
		{
			Name: "mlserver-entrypoint",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: spec.MLServerName(),
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
						// IMPORTANT: this needs to be re-enabled, it was giving a handshake error (likely an issue
						// with the generation) and I didn't feel like debugging, so running with TLS mode for now)
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
			Name:      spec.MLServerName(),
			Namespace: spec.Namespace,
			Labels:    selector,
		},
		Spec: appsv1.DeploymentSpec{
			// Note that if we increase replicas, need to check if entrypoint needs to change
			Replicas: &spec.Spec.MLServer.Replicas,

			// Match labels say which deployment a set of pods apply to
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: selector,
				},
				Spec: corev1.PodSpec{
					Subdomain:  spec.Name,
					Hostname:   "mlserver",
					Containers: []corev1.Container{container},
					Volumes:    volumes,
				},
			},
		},
	}
	if spec.Spec.MLServer.NodeSelector != "" {
		nodeSelector := map[string]string{"node.kubernetes.io/instance-type": spec.Spec.MLServer.NodeSelector}
		deployment.Spec.Template.Spec.NodeSelector = nodeSelector
	}
	return deployment
}
