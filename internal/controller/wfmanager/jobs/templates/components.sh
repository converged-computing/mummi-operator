{{ define "job-config" }}
config:
  {{ if .Job.Config.BundleSize }}bundle_size: {{ .Job.Config.BundleSize }}{{ end }}
  jobname:        {{ .JobName }}
  jobdesc:        {{ .JobDescription }}
  nnodes:         {{ if .Job.Config.Nodes }}{{ .Job.Config.Nodes }}{{ else }}1{{ end }}
  nprocs:         {{ if .Job.Config.Nproc }}{{ .Job.Config.Nproc }}{{ else }}1{{ end }}
  cores per task: {{ if .Job.Config.CoresPerTask }}{{ .Job.Config.CoresPerTask }}{{ else }}6{{ end }}
  ngpus:          {{ if .Job.Config.Gpus }}{{ .Job.Config.Gpus }}{{ else }}1{{ end }}
  walltime:       {{ if .Job.Config.Walltime }}'{{ .Job.Config.Walltime }}'{{ else }}'0:45:00'{{ end }}
  # If this job is nested (run inside batch ob - leave out entirely if not)
  {{ if .Job.Config.Nested }}nested: True{{ end }}
{{end}}

{{ define "mummi-vars" }}
  MUMMI_APP={{ if .Spec.Paths.MummiRoot }}{{ .Spec.Paths.MummiRoot }}{{ else }}/opt/clones/mummi-ras{{ end }}
  export MUMMI_ROOT=$MUMMI_APP
  export MUMMI_RESOURCES={{ if .Spec.Paths.MummiResources }}{{ .Spec.Paths.MummiResources }}{{ else }}/opt/clones/mummi_resources{{ end }}
  export MUMMI_APP
{{ end }}