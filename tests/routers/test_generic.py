def test_openapi(test_client):
    r = test_client.get("/api/openapi.json")
    assert r.status_code == 200
    assert r.json()


def test_oidc_configuration(test_client):
    r = test_client.get("/.well-known/openid-configuration")
    assert r.status_code == 200
    assert r.json()


def test_dirac_configuration(test_client):
    r = test_client.get("/.well-known/dirac")

    assert r.status_code == 200
    assert r.json()
