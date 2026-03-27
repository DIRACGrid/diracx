# Tasks configuration reference

The task system's runtime behaviour can be overridden via the DiracX configuration system without code changes. This page documents the YAML structure.

## Structure

```yaml
tasks:
  common:
    TaskClassName:
      enabled: true
      rate_limit: 100
      rate_window_seconds: 60
      concurrency_limit: 50
      periodic:
        enabled: true
        schedule: "0 0 * * *"
        interval_seconds: 3600
  vo-overrides:
    lhcb:
      TaskClassName:
        rate_limit: 200
```

## Fields

### TaskOverride

Per-task configuration overrides applied in the `common` section or under a specific VO in `vo-overrides`.

| Field                 | Type                         | Default | Description                                                    |
| --------------------- | ---------------------------- | ------- | -------------------------------------------------------------- |
| `enabled`             | `bool`                       | `true`  | Whether the task is enabled                                    |
| `rate_limit`          | `int \| null`                | `null`  | Maximum operations per rate window (`null` = use code default) |
| `rate_window_seconds` | `int \| null`                | `null`  | Rate limit window duration in seconds                          |
| `concurrency_limit`   | `int \| null`                | `null`  | Maximum concurrent executions                                  |
| `periodic`            | `PeriodicTaskConfig \| null` | `null`  | Periodic schedule overrides (only for periodic tasks)          |

### PeriodicTaskConfig

Overrides for periodic task scheduling.

| Field              | Type          | Default | Description                             |
| ------------------ | ------------- | ------- | --------------------------------------- |
| `enabled`          | `bool`        | `true`  | Whether the periodic schedule is active |
| `schedule`         | `str \| null` | `null`  | Cron expression override                |
| `interval_seconds` | `int \| null` | `null`  | Fixed-interval override in seconds      |

If both `schedule` and `interval_seconds` are set, `schedule` takes precedence.

## Override resolution

When determining the effective configuration for a task:

1. Start with the code defaults (class variables on the task class)
2. Apply the `common` entry for this task name (if present)
3. If a VO is specified, merge the `vo-overrides` entry on top — VO values take precedence over common values

## Example

This configuration shows per-VO overrides for pilot submission and global limits on the transformation agent:

```yaml
tasks:
  common:
    periodic-tasks:
      SubmitPilots:
        - args:
            ce_regex: .*
          enabled: true

    limits:
      Task:
        SubmitPilots:
          ConcurrencyLimiter:
            limit: 2
          RateLimiter:
            limit: 10
            window_seconds: 3600

      Transformation:
        default:
          ConcurrencyLimiter:
            limit: 10

  vo-overrides:
    lhcb:
      periodic-tasks:
        SubmitPilots:
          - name: SubmitPilotsCERN
            schedule:
              class: RRuleSchedule
              arg: "FREQ=HOURLY;INTERVAL=2"
            args:
              ce_regex: .*.cern.ch
          - name: SubmitPilotsRAL
            schedule:
              class: CronSchedule
              arg: "0 */6 * * *"
            args:
              ce_regex: .*.ral.ac.uk
```

!!! note

    The example above shows the full configuration schema from the [ADR](../../adr/DX-ADR-001_tasks.md#configuration). The `TaskOverride` model in the current implementation supports the `common` and `vo_overrides` sections with the fields documented above. The nested `limits` and `periodic-tasks` structures shown here reflect the target configuration format.
