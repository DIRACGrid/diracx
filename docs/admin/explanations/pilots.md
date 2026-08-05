# Pilots from the operator's perspective

This page is for operators running a DiracX instance. For the developer view (identity model, lifecycle, cross-table search, extension points) see the [developer explanation](../../dev/explanations/pilots.md).

## VO scoping

Pilots are partitioned by VO. By default a user only sees and acts on pilots belonging to their own VO. Users holding the
`SERVICE_ADMINISTRATOR` security property bypass that filter and can read pilots across every VO through the same search and summary endpoints.

Management actions (register, patch metadata) are restricted to `SERVICE_ADMINISTRATOR`, who — as for reads — may act across VOs. Legacy X.509 pilot identities (`GENERIC_PILOT` property) may additionally self-register and self-update within their own VO — pilots started in the vacuum have no SiteDirector to register them, mirroring `dirac-admin-add-pilot` in legacy DIRAC. Those routes opt in via `allow_legacy_pilots=True` in the access policy, which caps each call to a single pilot stamp to limit the blast radius of a stolen credential (note this bounds the rate of abuse, not its scope: a legacy pilot identity is not bound to its own stamp and can act on any pilot in its VO).
See [authentication with DiracX](auth-with-diracx.md) for the full pilot auth story (X.509, DiracX pilot tokens).
