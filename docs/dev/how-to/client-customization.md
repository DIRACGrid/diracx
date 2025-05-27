### Customising the Generated Client

TODO: update

Modifications to the generated client should be made in the `patches` files to ensure maintainability, and possibly imported in the `_patch.py` files if needed. Detailed guidance can be found in [Python Autorest documentation](https://github.com/Azure/autorest.python/blob/main/docs/customizations.md).

Note: any modification in the synchronous client should also be performed in the asynchronous client (**aio**), and vice-versa.

#### Example Usage

Operations are accessible via the `AsyncDiracClient`, which manages token refreshment:

```python
from diracx.client.aio import AsyncDiracClient

async with AsyncDiracClient() as client:
    jobs = await client.jobs.submit_jobs([x.read() for x in jdl])
```
