# Pilots from the operator's perspective

This page is for operators running a DiracX instance. For the developer view (identity model, lifecycle, cross-table search, extension points) see the [developer explanation](../../dev/explanations/pilots.md).

## VO scoping

Pilots are partitioned by VO. By default a user only sees and acts on pilots belonging to their own VO. Users holding the
`SERVICE_ADMINISTRATOR` security property bypass that filter and can read pilots across every VO through the same search and summary endpoints.

Management actions (register, delete, patch metadata) are restricted to `SERVICE_ADMINISTRATOR`. Legacy X.509 pilot identities (`GENERIC_PILOT` property) may be permitted to self-register or self-modify on a per-route basis; those routes opt in via `allow_legacy_pilots=True` in the access policy and cap each call to a single pilot stamp as a containment measure against stolen credentials.
See [authentication with DiracX](auth-with-diracx.md) for the full pilot auth story (X.509, DiracX pilot tokens).
