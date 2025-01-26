default_scheduler = "kubernetes"

# Operator label for the jobid
operator_label = "jobid"

# Completions (1 completion is an entire sequence) needed
default_completions = 4

# Prefix for job names
# Do not change for now, relied on by createsim
default_prefix = "structure_"
supported_schedulers = [default_scheduler]
