package jobs

import (
	"bytes"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
)

// PopulateCreateSim populates a newly provided MummiJob with defaults, etc.
func populateCreateSim(job *api.MummiJob) {

	// We don't allow nodes, procs, or cores per task to be 0
	if job.Config.Nodes == 0 {
		job.Config.Nodes = defaultNodes
	}
	// Createsim defaults to 6 cores per task
	if job.Config.CoresPerTask == 0 {
		job.Config.CoresPerTask = 6
	}
	// Number of processes, nproc
	if job.Config.Nproc == 0 {
		job.Config.Nproc = defaultNumberProcs
	}

	// Createsim default wall time (also in the template, but this is backup)
	if job.Config.Walltime == "" {
		job.Config.Walltime = "0:45:00"
	}
}

// TemplateCreateSim prepares the template for the config file mount
func TemplateCreateSim(spec *api.MiniMummi, job *api.MummiJob) (string, error) {

	// populate defaults for createsim
	populateCreateSim(job)

	subs := JobTemplate{
		JobName:        "createsim",
		JobDescription: "CreateSim ({})",
		Spec:           &spec.Spec,
		Mummi:          spec,
		Job:            job,
	}

	// Wrap the named template to identify it later
	startTemplate := `{{define "start"}}` + createsimTemplate + "{{end}}"

	// We assemble different strings (including the components) into one!
	template, err := combineTemplates(components, startTemplate)
	if err != nil {
		return "", err
	}
	var output bytes.Buffer
	if err := template.ExecuteTemplate(&output, "start", subs); err != nil {
		return "", err
	}
	return output.String(), nil
}
