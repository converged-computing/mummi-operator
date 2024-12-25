package rabbitmq

import (
	"bytes"
	_ "embed"
	"text/template"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
)

//go:embed templates/entrypoint.sh
var entrypointTemplate string

//go:embed templates/rabbitmq.conf
var confTemplate string

// ServiceTemplate is for a separate service container
type ConfigSubs struct {
	Spec *api.MiniMummi
}

// NewRabbitConfig writes the rabbitmq.conf, which is saved as a secret
func NewRabbitConfig(spec *api.MiniMummi) (string, error) {

	// Parse the entrypoint into a template
	tmpl, err := template.New("conf").Parse(confTemplate)
	if err != nil {
		return "", err
	}

	// We can write into a bytes buffer (and return as string)
	var out bytes.Buffer

	// Data for the template
	subs := ConfigSubs{Spec: spec}

	// Execute the template and write output to stdout
	err = tmpl.Execute(&out, subs)
	if err != nil {
		return "", err
	}
	return out.String(), nil
}

// NewRabbitEntrypoint generates the entrypoint based on the spec
// We don't need to template anything - so we just return the string
func NewRabbitEntrypoint() string {
	return entrypointTemplate
}
