package rabbitmq

import (
	_ "embed"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
	"github.com/converged-computing/mummi-operator/internal/controller/utils"
)

//go:embed templates/entrypoint.sh
var entrypointTemplate string

//go:embed templates/rabbitmq.conf
var confTemplate string

//go:embed templates/rabbitmq-credentials.json
var credsTemplate string

// NewRabbitConfig writes the rabbitmq.conf, which is saved as a secret
func NewRabbitConfig(spec *api.MiniMummi) (string, error) {
	return utils.PopulateTemplate(spec, confTemplate)
}

// NewRabbitEntrypoint generates the entrypoint based on the spec
// We don't need to template anything - so we just return the string
func NewRabbitEntrypoint() string {
	return entrypointTemplate
}

// NewRabbitCredentials generates the rabbitmq-credentials.json for the MLServer
func NewRabbitCredentials(spec *api.MiniMummi) (string, error) {
	return utils.PopulateTemplate(spec, credsTemplate)
}
