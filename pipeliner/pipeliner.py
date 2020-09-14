"""Pipeliner is a simple orchestrator.


How to use:
1. register all steps using the @step decorator.
2. call the step you want the result from, providing any missing prerequisites as arguments.

How it works:
When a step is added, its arguments and return values are redirected to an internal
value store holding Futures. Calling a step function creates a dependency tree of
steps that provide prerequisites, and then queues them up to run whenever their
own prerequisites become available. Steps without prerequisites will run first,
then the steps that depend on those steps, and so on down to the root of the tree.
If whatever resource a function provides is already present in the store when
it is about to run, the function is not run and dependecy resolution continues
as normal. This allows the user to skip parts of the tree by providing values.

The return values of the executed step are returned.
"""

import asyncio
import logging
from collections import defaultdict
from functools import wraps
from inspect import signature
from typing import Any, Callable, Dict, Sequence

_ENV = "!environment!"


class PipelineError(Exception):
    pass


class Pipeline:
    """A pipeline is a series of steps that provide resources to one another.

    Steps are defined using the step decorator. The provides argument to the decorator
    define the resource(s) that the step creates, and the function parameters define
    what resources need to be available for the step to be able to run."""

    def __init__(self, initial_resources: Dict[str, Any] = None):
        self.log = logging.getLogger(__name__)
        self._store: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._steps: Dict[str, Callable] = {}
        self._provider: Dict[str, str] = {}
        if initial_resources:
            self.add_resources(**initial_resources)

    def step(self, provides: Sequence[str] = None):
        """Designate a function as a pipeline step.

        {provides} is a list of resource names which will be available when this
        step finishes."""

        if provides is None:
            provides = ()
        elif isinstance(provides, str):
            provides = (provides,)

        def decorator(func):
            func._provides = provides
            func._wants = signature(func).parameters

            for resource in provides:
                # TODO: if multiple steps provide a resource, what's the resolution order?
                # For now, the first one to register gets dibs.
                if resource not in self._provider:
                    self._provider[resource] = func.__name__

            @wraps(func)
            async def step_wrapper(**resources):
                self.add_resources(**resources)
                if any(self.resource_ready(r) for r in func._provides):
                    log.debug("Resource cached, skipping %s", func.__name__)
                    return

                args, kwargs = [], {}
                for name, param in func._wants.items():
                    value = await self.resource(name)
                    if param.kind is param.VAR_POSITIONAL:
                        args.append(value)
                    else:
                        kwargs[name] = value

                log.debug("calling %s", func.__name__)
                results = await func(*args, **kwargs)
                if not func._provides:
                    return results

                if len(func._provides) == 1:
                    results = (results,)

                if len(results) != len(func._provides):
                    # TODO: support functions that want to add_resources during runtime
                    raise PipelineError(
                        f"Expected {func.__name__} to return "
                        f"{len(func._provides)} value(s), but got {len(results)}"
                    )
                resources = dict(zip(func._provides, results))
                self.add_resources(__provider=func.__name__, **resources)

                return results

            self._steps[func.__name__] = step_wrapper
            return step_wrapper

        return decorator

    def add_resources(self, __provider=_ENV, **kwargs):
        """Add one or more named resources to the pipeline datastore."""
        for k, v in kwargs.items():
            self.log.debug("adding resource %s", k)
            self._store[k].set_result(v)
            self._provider[k] = __provider

    def resource_ready(self, name):
        """Check if the named resource is ready for use."""
        return self._store[name].done()

    async def resource(self, name):
        """Get a resource from the store, blocking until it is ready to use."""
        if not self._store[name].done() and self._provider[name] != _ENV:
            self.log.debug("waiting for %s to become available", name)
            await self._steps[self._provider[name]]()
        return await self._store[name]

    def clear(self):
        """Remove all runtime-provided resources from the store."""
        for resource, provider in self._provider.items():
            if provider == _ENV:
                continue
            del self._store[resource]
