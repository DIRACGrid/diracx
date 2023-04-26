from fastapi.testclient import TestClient

from chrishackaton import app


def test_read_main():
    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200


def test_insert_and_list_jobs(normal_user_client):
    job_definitions = [
        {"owner": "owner1", "group": "group1", "vo": "vo1", "jdl": "jdl1"},
        {"owner": "owner2", "group": "group2", "vo": "vo2", "jdl": "jdl2"},
    ]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    r = normal_user_client.get("/jobs")
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)
    print(r.json())
