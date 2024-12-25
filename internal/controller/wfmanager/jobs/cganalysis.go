package jobs

import (
	"bytes"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
)

// PopulateCganalysis populates a newly provided MummiJob with defaults, etc.
func populateCganalysis(job *api.MummiJob) {

	// CGanalysis is always nested
	job.Config.Nested = true

	// We don't allow nodes, procs, or cores per task to be 0
	if job.Config.Nodes == 0 {
		job.Config.Nodes = defaultNodes
	}
	// Cganalysis defautls to 3 cores per task
	// 6 frontier / 3 summit / 5 on lassen (vsoch: this used to be 6 default)
	if job.Config.CoresPerTask == 0 {
		job.Config.CoresPerTask = 3
	}
	if job.Config.NumberProcs == 0 {
		job.Config.NumberProcs = defaultNumberProcs
	}

	// We need to have a minimum bundle size here of 1
	if job.Config.BundleSize == 0 {
		job.Config.BundleSize = 1
	}
}

// TemplateCganalysis prepares the template for the config file mount
func TemplateCganalysis(spec *api.MiniMummi, job *api.MummiJob) (string, error) {

	// Populate the job with defaults
	populateCganalysis(job)

	subs := JobTemplate{
		JobName:        "cganalysis",
		JobDescription: "CGAnalysis ({})",
		Spec:           *spec,
		Job:            *job,
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
