# Advanced Tutorial: Toy Pilot Submission System

This tutorial walks through building a complete system in DiracX, from
database design through tasks to testing. We use a simplified "site
director" as the domain — a system that submits pilots to compute
elements and tracks their lifecycle.

## What we're building

A DiracX installation has **compute elements** (CEs) — sites where
pilots can be submitted. Each CE has a name, a capacity, and a
reliability (`success_rate` from 0.0 to 1.0). The system periodically
submits pilots to available CEs, and pilots transition through states:

```
SUBMITTED → RUNNING → DONE
                    ↘ FAILED
```

We set up two CEs for testing:

- **`reliable-ce.example.org`** — `success_rate=1.0`, always succeeds
- **`flaky-ce.example.org`** — `success_rate=0.3`, frequently fails

## Prerequisites

- Completed the [Getting started](../getting-started.md) tutorial
- Familiarity with the [Tasks documentation](../../explanations/tasks/index.md)
- Development environment set up with `pixi install`

## Setup

If you want to follow along by writing the code yourself:

```bash
git checkout -b my-tutorial # clean starting point
pixi run tutorial-reset    # strips tutorial code from gubbins
pixi run pre-commit run --all-files
git add extensions/gubbins/
git commit -m "docs: Reset tutorial code"
```

Then follow each part, adding code step by step. Run the tutorial
tests at any point to check your progress:

```bash
pixi run test-tutorial
```

## Parts

1. [**Design**](design.md) — Think through the domain before writing code
2. [**Database**](database.md) — Implement MyPilotDB with SQLAlchemy
3. [**Logic**](logic.md) — Extract business logic into the logic layer
4. [**Tasks**](tasks.md) — Implement the four task types
5. [**Router**](router.md) — Add a minimal HTTP API
6. [**Testing the database**](testing-database.md) — Verify schema and queries
7. [**Testing the tasks**](testing-tasks.md) — Mock, lock, and execute
8. [**Testing the router**](testing-router.md) — Test client infrastructure
9. [**Running locally**](running-locally.md) — Run the system end-to-end
