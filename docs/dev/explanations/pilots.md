## Presentation

Pilots are a piece of software that is running on *worker nodes*. There are two types of pilots: "DIRAC pilots", and "DiracX pilots". The first type corresponds to pilots with proxies, sent by DIRAC; and the second type corresponds to pilots with secrets. Both kinds will eventually interact with DiracX using tokens (DIRAC pilots by exchanging their proxies for tokens, DiracX by exchanging their secrets for tokens).

## Management

Their management is adapted in DiracX, and each feature has its own route in DiracX. We will split the `/pilots` route into two parts:

1. `/api/pilots/*` to allow administrators and users to access and modify pilots
2. `/api/pilots/internal/*` is allocated for pilots resources: only DiracX pilots will have access to these resources

Each part has its own security policy: we want to prevent pilots to access users resources and vice-versa. To differentiate DIRAC pilots from users, we can get their token and compare their properties: `GENERIC_PILOT` is the property that defines a pilot. For DiracX pilots, we can differentiate them by looking at the token structure: they don't have properties, but a "stamp" (their identifier).

## Endpoints

We ordered our endpoints like so:

1. Creation: `POST /api/pilots/`
2. Deletion: `DELETE /api/pilots/`
3. Modification: `PATCH /api/pilots/metadata`
