# Part 8: Running locally

## Starting the full stack

Start the complete DiracX stack with:

```bash
pixi run local-start
```

Once everything is ready you'll see:

```
✅ DiracX is running on http://localhost:8000
📋 To interact with DiracX you can:
  1️⃣  Open a configured shell:  pixi run local-shell
  2️⃣  Submit a task:  pixi run local-tasks submit <entry_point> [--args JSON]
  3️⃣  Swagger UI: http://localhost:8000/api/docs

📊 Services: ✅ seaweedfs ✅ redis ✅ uvicorn ✅ scheduler ✅ worker-sm ✅ worker-md ✅ worker-lg
```

This launches:

| Component           | Description                                         |
| ------------------- | --------------------------------------------------- |
| **seaweedfs**       | S3-compatible object storage (sandboxes)            |
| **Redis**           | Message broker for the task system                  |
| **uvicorn**         | DiracX API server                                   |
| **scheduler**       | Periodic task scheduling and delayed-task promotion |
| **worker-sm/md/lg** | One worker per size (small, medium, large)          |

Press ++ctrl+c++ to stop all services.

## Interacting with the running instance

Open a shell that is pre-configured to talk to the local instance:

```bash
pixi run local-shell
```

From this shell you can use the `dirac` CLI as normal, for example:

```bash
dirac jobs submit ...
```

## Submitting tasks

With the full stack running, tasks can be submitted via the helper command:

```bash
pixi run local-tasks submit <entry_point> [--args JSON]
```

The scheduler handles periodic task scheduling, and workers pick up
tasks from the Redis streams based on their size and priority.

## Interactive task execution

You can also run a task directly, bypassing the broker — useful for
development and debugging:

```bash
pixi run local-tasks call <entry_point> [args...]
```

## What's next

- Read the [Tasks explanation](../../explanations/tasks/index.md) for
    deeper understanding of the broker lifecycle
- Check the [admin tasks guide](../../../admin/how-to/tasks/index.md)
    for operational guidance
