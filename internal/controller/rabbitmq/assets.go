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

//go:embed templates/rabbitmq-credentials.json
var credsTemplate string

// RabbitSubs are currently just the spec
type RabbitSubs struct {
	Spec *api.MiniMummi
}

// populateTemplate is a generic template to provide the spec to populate a template
func populateTemplate(spec *api.MiniMummi, templateString string) (string, error) {

	// Parse the entrypoint into a template
	tmpl, err := template.New("script").Parse(templateString)
	if err != nil {
		return "", err
	}

	// We can write into a bytes buffer (and return as string)
	var out bytes.Buffer

	// Data for the template
	subs := RabbitSubs{Spec: spec}

	// Execute the template and write output to stdout
	err = tmpl.Execute(&out, subs)
	if err != nil {
		return "", err
	}
	return out.String(), nil

}

// NewRabbitConfig writes the rabbitmq.conf, which is saved as a secret
func NewRabbitConfig(spec *api.MiniMummi) (string, error) {
	return populateTemplate(spec, confTemplate)
}

// NewRabbitEntrypoint generates the entrypoint based on the spec
// We don't need to template anything - so we just return the string
func NewRabbitEntrypoint() string {
	return entrypointTemplate
}

// NewRabbitCredentials generates the rabbitmq-credentials.json for the MLServer
func NewRabbitCredentials(spec *api.MiniMummi) (string, error) {
	return populateTemplate(spec, credsTemplate)
}
