package wfmanager

import (
	_ "embed"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
	"github.com/converged-computing/mummi-operator/internal/controller/utils"
	"github.com/converged-computing/mummi-operator/internal/controller/wfmanager/jobs"
)

//go:embed templates/entrypoint.sh
var entrypointTemplate string

//go:embed templates/kubernetes_start.sh
var startTemplate string

//go:embed templates/wfmanager.yaml
var managerTemplate string

//go:embed templates/kubernetesTracker.py
var trackerTemplate string

// NewEntrypoint generates the entrypoint.sh and kubernetes_start.sh
// as data for a config map
func NewEntrypoint(spec *api.MiniMummi) (map[string]string, error) {
	data := map[string]string{}
	script, err := utils.PopulateTemplate(spec, startTemplate)
	if err != nil {
		return data, err
	}
	entrypoint, err := utils.PopulateTemplate(spec, entrypointTemplate)
	if err != nil {
		return data, err
	}
	// Generate wfmanager.yaml
	wfmanagerYAML, err := utils.PopulateTemplate(spec, managerTemplate)
	if err != nil {
		return data, err
	}

	// These are jobs added to $MUMMI_ROOT/specs/kubernetes-mini
	cgJob, err := jobs.TemplateCganalysis(spec, &spec.Spec.Jobs.CgAnalysis)
	if err != nil {
		return data, err
	}
	createsimJob, err := jobs.TemplateCreateSim(spec, &spec.Spec.Jobs.CreateSim)
	if err != nil {
		return data, err
	}

	return map[string]string{
		// We add the tracker template for quick development here
		// Otherwise we would need to rebuild the container.
		"kubernetesTracker.py": trackerTemplate,

		// Entrypoint scripts
		"entrypoint.sh":       entrypoint,
		"kubernetes_start.sh": script,

		// Workflow manager configuration
		"wfmanager.yaml": wfmanagerYAML,

		// Jobs that workflow manager orchestrates
		"jobs_cg.yaml":        cgJob,
		"jobs_createsim.yaml": createsimJob,
	}, nil
}
