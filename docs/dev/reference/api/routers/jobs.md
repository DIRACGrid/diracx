# Jobs Router

Job management API endpoints including submission, querying, and status updates.

The Jobs router is composed of multiple sub-routers:

- **Submission**: Job submission endpoints
- **Query**: Job search and filtering
- **Status**: Job status management
- **Sandboxes**: Sandbox upload/download

## Router

::: diracx.routers.jobs
options:
show_root_heading: true
show_source: true
members_order: source
group_by_category: true
show_if_no_docstring: true
filters:
\- "!^\_"
\- "!^logger"

## Sub-Routers

### Submission

::: diracx.routers.jobs.submission
options:
show_root_heading: false
show_source: true
members_order: source
group_by_category: true

### Query

::: diracx.routers.jobs.query
options:
show_root_heading: false
show_source: true
members_order: source
group_by_category: true

### Status

::: diracx.routers.jobs.status
options:
show_root_heading: false
show_source: true
members_order: source
group_by_category: true

### Sandboxes

::: diracx.routers.jobs.sandboxes
options:
show_root_heading: false
show_source: true
members_order: source
group_by_category: true

### Access Policies

::: diracx.routers.jobs.access_policies
options:
show_root_heading: false
show_source: true
members_order: source
group_by_category: true

### Legacy

::: diracx.routers.jobs.legacy
options:
show_root_heading: false
show_source: true
members_order: source
group_by_category: true
