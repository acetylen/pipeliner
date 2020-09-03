
Pipeliner - async python pipeline orchestrator
==============================================

Pipeliner is a simple-to-use asynchronous pipeline orchestrator for python functions.

**How to use:**

1. register all steps using the @step decorator.
2. call the step you want the result from, providing any missing prerequisites as arguments.

**How it works:**

When a step is added, its arguments and return values are redirected to an internal
value store holding Futures. Calling a step function creates a dependency tree of
steps that provide prerequisites, and then queues them up to run whenever their
own prerequisites become available. Steps without prerequisites will run first,
then the steps that depend on those steps, and so on down to the root of the tree.
If whatever resource a function provides is already present in the store when
it is about to run, the function is not run and dependecy resolution continues
as normal. This allows the user to skip parts of the tree by providing values.

The return values of the executed step are returned.

**Example:**

```python
import pipeliner

@pipeliner.step(provides="base")
async def base_provider():
    print("base_provider called")
    return 23

@pipeliner.step(provides=("result", "modulus"))
async def divide(base, divisor):
    print("divide called")
    return base // divisor, base % divisor

@pipeliner.step()
async def get_division(base, result, modulus):
    print("get_division called")
    return base, result, modulus

async def main():
    base, result, mod = await get_division(divisor=3)
    print(base, "divided by 3 is", result, "with",mod,"left")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```
