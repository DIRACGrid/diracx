import time
import json
import base64
from pprint import pprint

import requests

# TODO: use pkci

r = requests.get("http://localhost:8000/auth/login", params={"vo": "lhcb"})
r.raise_for_status()


device_auth_endpoint = r.json()["device_auth_endpoint"]
client_id = r.json()["client_id"]
token_endpoint = r.json()["token_endpoint"]


data = {
    "client_id": client_id,
    "scope": "openid profile email",
    "audience": "DIRAC servers",
}


r = requests.post(device_auth_endpoint, data=data)
r.raise_for_status()
# {'user_code': '2QRKPY',
#  'device_code': 'b5dfda24-7dc1-498a-9409-82f1c72e6656',
#  'verification_uri_complete': 'https://wlcg.cloud.cnaf.infn.it/device?user_code=2QRKPY',
#  'verification_uri': 'https://wlcg.cloud.cnaf.infn.it/device',
#  'expires_in': 600}

pprint(r.json())
while True:
    r2 = requests.post(
        token_endpoint,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "device_code": r.json()["device_code"],
            "client_id": client_id,
        },
    )
    if r2.status_code == 200:
        break
    if r2.status_code != 400 or r2.json()["error"] != "authorization_pending":
        r2.raise_for_status()
    time.sleep(1)


idToken = r2.json()["id_token"]



pprint(idToken)

x= idToken.split('.')[1]

pprint(json.loads(base64.b64decode(x + (4 - len(x) % 4 if x != 0 else 0) * "=")))


# {'sub': '6ebbcc29-8680-4347-92fb-12c3d37d1e4b',
#  'kid': 'rsa1',
#  'iss': 'https://wlcg.cloud.cnaf.infn.it/',
#  'preferred_username': 'cburr',
#  'organisation_name': 'wlcg',
#  'wlcg.ver': '1.0',
#  'aud': '85618558-f7ce-429c-a372-51d7b252cbd8',
#  'name': 'Chris Burr',
#  'exp': 1680602629,
#  'iat': 1680602029,
#  'jti': 'fe13041c-6d8d-4ff5-8a1b-6c7046d72585',
#  'email': 'christopher.burr@cern.ch'}


# chrisToken = {
#     "aud": "5c0541bf-85c8-4d7f-b1df-beaeea19ff5b",
#     "cern_person_id": "705305",
#     "email": "christophe.haen@cern.ch",
#     "exp": 1680613292,
#     "iat": 1680612692,
#     "iss": "https://lhcb-auth.web.cern.ch/",
#     "jti": "38dbb060-19ad-4a77-9c54-15901b96e286",
#     "kid": "rsa1",
#     "name": "CHRISTOPHE DENIS HAEN",
#     "organisation_name": "lhcb",
#     "preferred_username": "chaen",
#     "sub": "b824d4dc-1f9d-4ee8-8df5-c0ae55d46041",
#     "wlcg.ver": "1.0",
# }


r = requests.post(
    "http://localhost:8000/auth/login",
    headers={"authorization": f"Bearer {idToken}"},
    params={"diracGroup": "lhcb_user"},
)
r.raise_for_status()
pprint(r.json())
tokenResponse = r.json()

diracToken = tokenResponse["access_token"]

r = requests.get('http://localhost:8000/jobs/', headers={"authorization":f"Bearer {diracToken}"})
r.raise_for_status()
pprint(r.json())
