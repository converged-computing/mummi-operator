package mlserver

import (
	_ "embed"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
	"github.com/converged-computing/mummi-operator/internal/controller/utils"
)

//go:embed templates/entrypoint.sh
var entrypointTemplate string

//go:embed templates/kubernetes_start.sh
var startTemplate string

//go:embed templates/mlserver.yaml
var mlserverTemplate string

// NewEntrypoint generates the entrypoint.sh and kubernetes_start.sh,
// and the mlserver.yaml that defines the machine learning server
// as data for a config map
func NewEntrypoint(spec *api.MiniMummi) (map[string]string, error) {
	data := map[string]string{
		"entrypoint.sh": entrypointTemplate,
	}
	script, err := utils.PopulateTemplate(spec, startTemplate)
	if err != nil {
		return data, err
	}
	mlserverYAML, err := utils.PopulateTemplate(spec, mlserverTemplate)
	if err != nil {
		return data, err
	}
	return map[string]string{
		"entrypoint.sh":       entrypointTemplate,
		"kubernetes_start.sh": script,
		// Copied to /opt/clones/mummi-ras/specs/kubernetes-mini
		"mlserver.yaml": mlserverYAML,
	}, nil
}
