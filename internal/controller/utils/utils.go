package utils

import (
	"bytes"
	"html/template"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
)

// TemplateSubs are currently just the spec
// This is consistent across all deployments, etc.
type TemplateSubs struct {
	Spec  *api.MiniMummiSpec
	Mummi *api.MiniMummi
}

// PopulateTemplate is a generic template to provide the spec to populate a template
func PopulateTemplate(spec *api.MiniMummi, templateString string) (string, error) {

	// Parse the entrypoint into a template
	tmpl, err := template.New("script").Parse(templateString)
	if err != nil {
		return "", err
	}

	// We can write into a bytes buffer (and return as string)
	var out bytes.Buffer

	// Data for the template (redundant, but provided for convenience)
	subs := TemplateSubs{Spec: &spec.Spec, Mummi: spec}

	// Execute the template and write output to stdout
	err = tmpl.Execute(&out, subs)
	if err != nil {
		return "", err
	}
	return out.String(), nil

}
