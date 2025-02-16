{{ define "job-config" }}
config:
  {{ if .Job.Config.BundleSize }}bundle_size: {{ .Job.Config.BundleSize }}{{ end }}
  jobname:          {{ .JobName }}
  jobdesc:          {{ .JobDescription }}
  nnodes:           {{ if .Job.Config.Nodes }}{{ .Job.Config.Nodes }}{{ else }}1{{ end }}
  nprocs:           {{ if .Job.Config.Nproc }}{{ .Job.Config.Nproc }}{{ else }}1{{ end }}
  cores per task:   {{ if .Job.Config.CoresPerTask }}{{ .Job.Config.CoresPerTask }}{{ else }}6{{ end }}
  ngpus:            {{ .Job.Config.Gpus }}
  walltime:         {{ if .Job.Config.Walltime }}'{{ .Job.Config.Walltime }}'{{ else }}'0:45:00'{{ end }}
  max_active_jobs:  {{ if .Job.Config.MaxActive }}{{ .Job.Config.MaxActive }}{{ else }}2{{ end }}
  # Kubernetes specific settings
  gpulabel:         {{ if .Spec.Labels.GPU }}{{ .Spec.Labels.GPU }}{{ else }}nvidia.com/gpu{{ end }}
  pull_policy:      {{ .Job.ImagePullPolicy }}
  retry_failure:    {{ if .Job.Config.RetryFailure }}true{{ else }}false{{ end }}

  # If this job is nested (run inside batch ob - leave out entirely if not)
  {{ if .Job.Config.Nested }}nested: True{{ end }}
{{end}}

{{ define "oras-pull" }}
  echo "Looking for $simname with oras repo list"
  # Check 1: The repo (sim name) must exist in the registry
  oras repo list $registry {{ if .Spec.Registry.PlainHttp }}--plain-http{{ end }} {{ if .Spec.Registry.TLSVerify }}{{ else }}--insecure{{ end }} | grep $simname
  if [ $? -ne 0 ];
    then
      echo "Cannot find $simname in $registry listing"
      exit 1
  fi
  # Check 2: The tag for the previous step must exist
  oras repo tags $registry/$simname {{ if .Spec.Registry.PlainHttp }}--plain-http{{ end }} {{ if .Spec.Registry.TLSVerify }}{{ else }}--insecure{{ end }} | grep $tag
  if [ $? -ne 0 ];
    then
      echo "Cannot find $tag in $registry/$simname"
      exit 1
  fi

  # Check 3: The pull must succeed
  echo "Pulling oras artifact to $locpath"
  oras pull $uri {{ if .Spec.Registry.PlainHttp }}--plain-http{{ end }} {{ if .Spec.Registry.TLSVerify }}{{ else }}--insecure{{ end }}
  if [ $? -ne 0 ];
    then
      echo "Cannot pull $uri"
      exit 1
  fi
{{ end }}

{{ define "mummi-vars" }}
  MUMMI_APP={{ if .Spec.Paths.MummiRoot }}{{ .Spec.Paths.MummiRoot }}{{ else }}/opt/clones/mummi-ras{{ end }}
  export MUMMI_ROOT=$MUMMI_APP
  export MUMMI_RESOURCES={{ if .Spec.Paths.MummiResources }}{{ .Spec.Paths.MummiResources }}{{ else }}/opt/clones/mummi_resources{{ end }}
  export MUMMI_APP
{{ end }}
