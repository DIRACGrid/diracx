# DiracX tasks

This document describes the architecture of the DiracX tasks system that is used to asynchronously perform work.

## Scope

In DiracX there are three different forms of task:

### Cron-triggered tasks

Tasks which are performed periodically based on a time-based schedule. Simple examples would be:

- Generating weekly accounting reports
- Synchronising IAM to the DiracX configuration
- Cleaning the sandbox store
- Polling resource status

### Reactive tasks

Some tasks are triggered in response to external input, for example:

- Job submission triggers it to be optimised
- Performing heavy tasks for the webapp, e.g. preparing a dump of the file catalog in response to user input in DiracX web

### Scheduled tasks

Occasionally tasks must be scheduled to run at some point in the future, most commonly to retry a task after a delay. For example if a resource is banned corresponding tasks can use an exponential backoff strategy to avoid running excessively.

## What is a task?

Tasks are async Python functions which have extremely low overhead, allowing for many tasks to be spawned for even cheap operations.
Tasks can be executed in four different ways:

- **Standalone tasks:** The task performs its work and then returns. For example synchronising the IAM to DiracX configuration queries IAM and then updates the DiracX CS.
- **Spawning tasks:** The task creates additional tasks. For example, many cron-triggered tasks for transformations will spawn a task per transformation.
- **Batching tasks:** This task groups together the work of many smaller tasks. For example, it is more efficient to clean many jobs at once so many individual cleaning tasks are batched together for execution.
- **Callback tasks:** These tasks run in response to several parent tasks finishing successfully. Removing a user from the configuration store after deleting all of the objects owned by them.

### Reentrancy

Tasks are designed to be re-entrant and DiracX tasks provides the infrastructure to limit parallel execution if required. This is necessary due to the asynchronous and distributed nature of their execution. Locks can also be used to prevent two different tasks from operating on the same underlying object, for example a transformation cannot be cleaned while jobs are being submitted for it.

Not all tasks are equal in their importance. Most tasks have "soft failure" modes and will be naturally retried, typically by cron-style schedules re-spawning them if required. If these tasks fail repeatedly that must be easily detectable so it can be reported for review. For tasks that must be executed, such as pending request for a job, a dead-letter queue is used in the event of repeated failures to persist them for manual recovery.

All tasks can easily be executed interactively if desired to assist with debugging or to perform manual recovery actions.

### Sizing

Currently we foresee the need for three sizes of tasks:

- **Small:** Uses little CPU and memory, often IO bound. Many can be ran asynchronously in a single thread. For example, optimising a job.
- **Medium:** Medium memory, medium CPU. Single task per thread. For example, bulk submitting jobs for a transformation.
- **Large:** Large amounts of memory, multiple CPU cores. Single task per thread. For example, generating reporting data.

Tasks should be designed to be lightweight, especially in terms of memory usage, using techniques such as streaming database responses. The vast majority of tasks should be in the **Small** category. The **Large** tasks should be reserved for infrequent, time insensitive activity (e.g. reporting).

### Priorities

In general tasks are executed on a first-in-first-out basis depending on three levels of priority:

- **Realtime:** Tasks which are expected to be executed immediately. If there is ever a backlog of tasks in this queue the available worker pool is likely undersized for the installation. Examples of realtime tasks would be user input in DiracX Web or running the job optimizers.
- **Normal:** Tasks which should generally run immediately but for which there is a less strict latency requirement. If there is regularly a backlog in this queue the available worker pool is likely undersized for the installation. Examples include submitting jobs for transformations or periodically polling external services.
- **Background:** Tasks which have no specific latency requirements. Having a backlog of tasks in response to operational activity is expected and not directly indicative of an issue provided the backlog is not growing without bound. Examples include pending requests for jobs or data management activities from the transformation system.

It is currently expected that these three queues will form a strict ordering for each pool of workers, i.e. no **Normal** priority tasks are executed unless the **Realtime** queue is empty. At a future date this policy could be relaxed to probabilistically execute tasks from all three queues if that is found to be operationally necessary.

### Locking

Several levels of locking can be applied to support the reentrancy requirements of tasks:

- **Task level locking:** Tasks can be configured to prevent simultaneous execution. For example, most cron-style tasks are configured to ensure only a single instance is running at any given time.
- **Object level locking:** Tasks can also optionally claim ownership on a DiracX resource (e.g. job or transformation). Several types of lock are available: mutex, semaphore and readers–writer. Additionally, the failure mode of locks can be configured to either reset with a time-to-live or require manual recovery.

### Retries

Two forms of retry are available to tasks:

- **Reschedule retries:** Pre-requisites are not satisfied, these retries can be configured to repeat without hard limits.
- **Failure retries:** The task failed for an unknown reason that may be ephemeral. A configurable hard upper limit is placed on the number of failure retries.

When retrying each type of failure has a configurable policy of when the next attempt is applied ranging from immediate retries to various backoff strategies.

## Broker

The state of the broker should be ephemeral and recreated with each update. Any persistent state should be stored in the standard MySQL database's used by DiracX. This requirement is imposed to:

- Simplify recovery from unexpected outages.
- Reduce the complexity of reasoning about updates which may change details of the broker's internal state.
- Improve performance by removing the need to ensure every action is flushed to persistent storage.

Upon first start, the broker is populated with the cron-style tasks as well as any pending reactive tasks that have been persisted in MySQL. The tasks which were persisted are those eligible for the dead letter queue.

## Operational considerations

### Configuration

The configuration of tasks can be done through a dedicated DiracX configuration file. This file includes per-task overrides for:

- Cron-style schedules
- Rate limits
- Number of retry attempts
- Backoff behaviour

Default values for the above are included in the task definitions in the DiracX source code.

### Monitoring

All tasks should keep track of:

- When they were first submitted.
- How many times they have failed.
- How many times they have attempted to be executed. This may be different from the number of failures if the task is configured to defer itself based on rate limits or the status of an external resource.
- The parent task which caused them to be called (if any).
- The resource consumption of their execution.
- The number of failed attempts and debugging information about each failed attempt.

The broker should provide functionality to:

- Understand the size and composition of pending and deferred tasks.
- Observe the currently running tasks.

### Resource utilisation

To ensure the stability of the system, all workers should be configured to enforce memory and CPU limits. If these limits are exceeded the DiracX task system must be able to detect and report the issue, however in the case of small workers such detection may be unreliable due to technical constraints in async environments.
