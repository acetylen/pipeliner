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

log = logging.getLogger(__name__)


class PipelineError(Exception):
    pass


class Pipeline:
    def __init__(self, initial_resources: Dict[str, Any] = None):
        self._store: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._steps: Dict[str, Callable] = {}
        self._provider: Dict[str, str] = {}
        if initial_resources:
            self.add_resources(**initial_resources)

    def step(self, provides: Sequence[str] = None):
        """Decorator; designates a function as a step in a pipeline."""
        if not provides:
            provides = ()
        elif isinstance(provides, str):
            provides = (provides,)

        def decorator(func):
            return wraps(func)(Step(pipe=self, func=func, provides=provides))

        return decorator

    def add_resources(self, **kwargs):
        """Add resources to store."""
        for k, v in kwargs.items():
            log.debug("adding resource %s", k)
            self._store[k].set_result(v)

    def resource_ready(self, name):
        """Check if the named resource is available for use."""
        return self._store[name].done()

    async def resource(self, name):
        """Block until named resource is available, then return it."""
        return await self._store[name]

    def _add_call_graph(self, func):
        """Add a tree of prerequisites to the given step function."""
        log.debug("building call graph for %s", func.__name__)
        parents = set()
        for want in func.wants:
            if self._store[want].done():  # already provided
                continue
            parents.add(self._provider[want])
        func.prerequisites = (self._add_call_graph(self._steps[p]) for p in parents)
        return func


class Step:
    def __init__(self, pipe, func, provides):
        self.pipe = pipe
        self.func = func
        self.fname = func.__name__
        self.sig = signature(func)
        self.wants = tuple(self.sig.parameters.keys())
        self.provides = provides
        self.prerequisites: Sequence[Callable] = []

        pipe._steps[self.fname] = self
        for resource in provides:
            # TODO: if multiple steps provide a resource, what's the resolution order?
            if resource not in pipe._provider:
                pipe._provider[resource] = self.fname

    async def __call__(self, **resources):
        self.pipe.add_resources(**resources)

        # Build prerequisites tree
        if not self.prerequisites:
            self.pipe._add_call_graph(self)

        # run prerequisites
        await asyncio.gather(*[f() for f in self.prerequisites])

        # check for pre-existing resources
        for resource in self.provides:
            if self.pipe.resource_ready(resource):
                log.debug(f"{resource} is already cached, skipping call to {self.fname}")
                return

        # Pull arguments from store
        args, kwargs = [], {}
        for resource in self.wants:
            value = await self.pipe.resource(resource)
            param = self.sig.parameters[resource]
            if param.kind is param.VAR_POSITIONAL:
                args.append(value)
            else:
                kwargs[resource] = value

        results = await self.func(*args, **kwargs)

        # put results into store
        if not self.provides:
            return results

        if len(self.provides) == 1:
            results = (results,)

        if len(results) != len(self.provides):
            raise PipelineError(
                f"Output mismatched in step {self.fname}:"
                f" expected {len(self.provides)}"
                f" return value(s), got {len(results)}"
            )
        self.pipe.add_resources(**dict(zip(self.provides, results)))

        return results
