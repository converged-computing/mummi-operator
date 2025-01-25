mummi_workflow_config_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://github.com/converged-computing/mummi_operator/tree/main/python/mummi_operator/schema.py",
    "title": "mummi-workflow-01",
    "description": "Mummi Workflow Config",
    "type": "object",
    # The only required thing is jobs
    "required": ["jobs"],
    "properties": {
        "jobs": {"$ref": "#/definitions/jobs"},
        "cluster": {"$ref": "#/definitions/cluster"},
        "logging": {"$ref": "#/definitions/logging"},
        "config_dir": {"type": "string"},
        "additionalProperties": False,
    },
    "definitions": {
        "cluster": {
            "type": "object",
            "properties": {
                "max_size": {"type": "number", "default": 6},
                "autoscale": {"type": "boolean", "default": False},
            },
            "additionalProperties": False,
        },
        "logging": {
            "type": "object",
            "properties": {
                "debug": {"type": "boolean", "default": False},
            },
            "additionalProperties": False,
        },
        "jobs": {
            "type": ["array"],
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                },
                "required": ["config"],
            },
        },
    },
}

mummi_job_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://github.com/converged-computing/mummi_operator/tree/main/python/mummi_operator/schema.py",
    "title": "mummi-job-01",
    "description": "Mummi Job Config",
    "type": "object",
    "required": ["name", "config", "script", "image"],
    "properties": {
        "name": {"type": "string"},
        "config": {"$ref": "#/definitions/config"},
        "script": {"type": "string"},
        "image": {"type": "string"},
        "additionalProperties": False,
    },
    "definitions": {
        "config": {
            "type": "object",
            "properties": {
                "nnodes": {"type": "number", "default": 1},
            },
        }
    },
}
