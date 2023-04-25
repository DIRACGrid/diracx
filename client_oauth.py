from pprint import pprint

import requests

DIRAC_CLIENT_ID = "myDIRACClientID"
DIRAC_TOKEN_FILE = "/tmp/dirac_token.json"

try:
    with open(DIRAC_TOKEN_FILE, "rt") as f:
        diracToken = f.read()
except Exception:
    r = requests.post(
        "http://localhost:8000/auth/lhcb/device",
        params={
            "client_id": DIRAC_CLIENT_ID,
            "audience": "Dirac server",
            "scope": "group:lhcb_user property:FileCatalogManagement property:NormalUser",
        },
    )
    r.raise_for_status()

    pprint(r.json())

    while True:
        r2 = requests.post(
            "http://localhost:8000/auth/lhcb/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": r.json()["device_code"],
                "client_id": DIRAC_CLIENT_ID,
            },
        )
        print(r2.text)
        if r2.status_code == 200:
            break
        if r2.status_code != 400 or r2.json()["error"] != "authorization_pending":
            pprint(r2.text)
            r2.raise_for_status()

    tokenResponse = r2.json()

    diracToken = tokenResponse["access_token"]

    with open(DIRAC_TOKEN_FILE, "wt") as f:
        f.write(diracToken)

r = requests.get(
    "http://localhost:8000/jobs/", headers={"authorization": f"Bearer {diracToken}"}
)
r.raise_for_status()
pprint(r.json())


job_definitions = [
    {"owner": "owner1", "group": "group1", "vo": "vo1", "jdl": "jdl1"},
    {"owner": "owner2", "group": "group2", "vo": "vo2", "jdl": "jdl2"},
]

r = requests.post(
    "http://localhost:8000/jobs/",
    headers={"authorization": f"Bearer {diracToken}"},
    json=job_definitions,
)


assert r.ok, r.json()

r = requests.get(
    "http://localhost:8000/jobs/", headers={"authorization": f"Bearer {diracToken}"}
)
assert r.ok, r.json()


# test CS
r = requests.get(
    "http://localhost:8000/config/lhcb",
    headers={"authorization": f"Bearer {diracToken}"},
)

assert r.ok, r.json()
last_modified = r.headers["Last-Modified"]
etag = r.headers["ETag"]

print(f"{r.status_code=} {len(r.text)=}")


r = requests.get(
    "http://localhost:8000/config/lhcb",
    headers={
        "authorization": f"Bearer {diracToken}",
        "If-None-Match": etag,
        "If-Modified-Since": last_modified,
    },
)

assert r.ok, r.json()
last_modified = r.headers["Last-Modified"]
etag = r.headers["ETag"]

print(f"{r.status_code=} {len(r.text)=}")

r = requests.get(
    "http://localhost:8000/config/lhcb",
    headers={"authorization": f"Bearer {diracToken}", "If-None-Match": "123"},
)

assert r.ok, r.json()
last_modified = r.headers["Last-Modified"]
etag = r.headers["ETag"]

print(f"{r.status_code=} {len(r.text)=}")


# pprint(r.json())
