from .pipeliner import Pipeline

_default = Pipeline()

step = _default.step
add_resources = _default.add_resources
resource_ready = _default.resource_ready
resource = _default.resource
