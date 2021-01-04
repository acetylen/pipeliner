from .pipeliner import Pipeline

_default = Pipeline()

step = _default.step
add_environment_resources = _default.add_environment_resources
resource_ready = _default.resource_ready
resource = _default.resource
