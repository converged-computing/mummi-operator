default_scheduler = "kubernetes"

# TODO this should match to the name of the TBA operator
operator_label = "jobid"

# Prefix for job names
# Do not change for now, relied on by createsim
default_prefix = "structure_"
supported_schedulers = [default_scheduler]
