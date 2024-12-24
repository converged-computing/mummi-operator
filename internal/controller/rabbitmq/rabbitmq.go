package rabbitmq

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	rLog = ctrl.Log.WithName("rabbitmq")
)

// NewRabbitDeployment returns a new rabbit deployment based on the spec
func NewRabbitDeployment(spec *api.MiniMummi) *appsv1.Deployment {

	rLog.Info("Creating rabbitmq deployment for: ", spec.RabbitDeploymentName(), spec.Namespace)

	// Prepare pull policy and selector. Use "Never" for pre-loaded image
	pullPolicy := corev1.PullPolicy(spec.Spec.RabbitMQ.ImagePullPolicy)
	selector := spec.Selector()

	// RabbitMQ needs lots of ports :)
	ports := []corev1.ContainerPort{
		{ContainerPort: 15672}, // admin without TLS (3.x and up)
		{ContainerPort: 15671}, // admin with TLS
		{ContainerPort: 5672},  // API (AMQP) without TLS
		{ContainerPort: 5671},  // API with TLS encryption
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.RabbitDeploymentName(),
			Namespace: spec.Namespace,
			Labels:    selector,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &spec.Spec.RabbitMQ.Replicas,

			// Match labels say which deployment a set of pods apply to
			Selector: &metav1.LabelSelector{
				MatchLabels: selector,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: selector,
				},
				Spec: corev1.PodSpec{
					Subdomain: spec.Name,
					Hostname:  spec.Spec.RabbitMQ.Name,
					Containers: []corev1.Container{
						{
							Name:            "rabbitmq",
							Image:           spec.Spec.RabbitMQ.Image,
							ImagePullPolicy: pullPolicy,
							TTY:             true,
							Ports:           ports,

							// Certificates mounted from secret volumes
							// These need to be moved and chowned for redis user
							// So we mount elsewhere and will do that! We also do not
							// want to break the development / testing container
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "rabbitmq",
									MountPath: "/mummi_operator/",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "rabbitmq",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: spec.RabbitSecretName(),
								},
							},
						},
					},
				},
			},
		},
	}
}
