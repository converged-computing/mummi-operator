package jobs

import (
	_ "embed"
	"fmt"
	"text/template"

	api "github.com/converged-computing/mummi-operator/api/v1alpha1"
)

//go:embed templates/createsim.yaml
var createsimTemplate string

//go:embed templates/cganalysis.yaml
var cganalysisTemplate string

//go:embed templates/components.sh
var components string

// ServiceTemplate is for a separate service container
type JobTemplate struct {
	JobName        string
	JobDescription string
	Spec           api.MiniMummi
	Job            api.MummiJob
}

// combineTemplates into one common start
func combineTemplates(listing ...string) (t *template.Template, err error) {
	t = template.New("start")

	for i, templ := range listing {
		_, err = t.New(fmt.Sprint("_", i)).Parse(templ)
		if err != nil {
			return t, err
		}
	}
	return t, nil
}
