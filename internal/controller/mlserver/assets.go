package mlserver

import (
	"bytes"
	_ "embed"
	"text/template"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
)

//go:embed templates/entrypoint.sh
var entrypointTemplate string

//go:embed templates/kubernetes_start.sh
var startTemplate string

// StartSubs populate the Kubernetes start script
type StartSubs struct {
	Spec *api.MiniMummi
}

// newStartScript writes kubernetes_start.sh
func newStartScript(spec *api.MiniMummi) (string, error) {

	// Parse the entrypoint into a template
	tmpl, err := template.New("start").Parse(startTemplate)
	if err != nil {
		return "", err
	}

	// We can write into a bytes buffer (and return as string)
	var out bytes.Buffer

	// Data for the template
	subs := StartSubs{Spec: spec}

	// Execute the template and write output to stdout
	err = tmpl.Execute(&out, subs)
	if err != nil {
		return "", err
	}
	return out.String(), nil
}

// NewEntrypoint generates the entrypoint.sh and kubernetes_start.sh
// as data for a config map
func NewEntrypoint(spec *api.MiniMummi) (map[string]string, error) {
	data := map[string]string{
		"entrypoint.sh": entrypointTemplate,
	}
	script, err := newStartScript(spec)
	if err != nil {
		return data, err
	}
	return map[string]string{
		"entrypoint.sh":       entrypointTemplate,
		"kubernetes_start.sh": script,
	}, nil
}
