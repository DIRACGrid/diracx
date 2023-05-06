TEST_JDL = """
    Arguments = "jobDescription.xml -o LogLevel=INFO";
    Executable = "dirac-jobexec";
    JobGroup = jobGroup;
    JobName = jobName;
    JobType = User;
    LogLevel = INFO;
    OutputSandbox =
        {
            Script1_CodeOutput.log,
            std.err,
            std.out
        };
    Priority = 1;
    Site = ANY;
    StdError = std.err;
    StdOutput = std.out;
"""

TEST_PARAMETRIC_JDL = """
Arguments = "jobDescription.xml -o LogLevel=DEBUG  -p JOB_ID=%(JOB_ID)s  -p InputData=%(InputData)s";
    Executable = "dirac-jobexec";
    InputData = %(InputData)s;
    InputSandbox = jobDescription.xml;
    JOB_ID = %(JOB_ID)s;
    JobName = Name;
    JobType = User;
    LogLevel = DEBUG;
    OutputSandbox =
        {
            Script1_CodeOutput.log,
            std.err,
            std.out
        };
    Parameters = 3;
    Parameters.InputData =
        {
            {/lhcb/data/data1,
            /lhcb/data/data2},
            {/lhcb/data/data3,
            /lhcb/data/data4},
            {/lhcb/data/data5,
            /lhcb/data/data6}
        };
    Parameters.JOB_ID =
        {
            1,
            2,
            3
        };
    Priority = 1;
    StdError = std.err;
    StdOutput = std.out;
"""


def test_insert_and_list_parametric_jobs(normal_user_client):
    job_definitions = [TEST_PARAMETRIC_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == 3  # Parameters.JOB_ID is 3

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    r = normal_user_client.get("/jobs")
    assert r.status_code == 200, r.json()

    listed_jobs = r.json()

    assert len(listed_jobs) == 3  # Parameters.JOB_ID is 3

    assert submitted_job_ids == sorted([job_dict["JobID"] for job_dict in listed_jobs])


def test_insert_and_list_jobs(normal_user_client):
    # job_definitions = [TEST_JDL%(normal_user_client.dirac_token_payload)]
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    r = normal_user_client.get("/jobs")
    assert r.status_code == 200, r.json()

    listed_jobs = r.json()

    assert len(listed_jobs) == len(job_definitions)

    assert submitted_job_ids == sorted([job_dict["JobID"] for job_dict in listed_jobs])


def test_insert_and_search(normal_user_client):
    # job_definitions = [TEST_JDL%(normal_user_client.dirac_token_payload)]
    job_definitions = [TEST_JDL]
    r = normal_user_client.post("/jobs/", json=job_definitions)
    assert r.status_code == 200, r.json()
    assert len(r.json()) == len(job_definitions)

    submitted_job_ids = sorted([job_dict["JobID"] for job_dict in r.json()])

    r = normal_user_client.post("/jobs/search")
    assert r.status_code == 200, r.json()
    assert [x["JobID"] for x in r.json()] == submitted_job_ids

    r = normal_user_client.post(
        "/jobs/search", json={"search": [["Status", "eq", "NEW"]]}
    )
    assert r.status_code == 200, r.json()
    assert r.json() == []

    r = normal_user_client.post(
        "/jobs/search", json={"search": [["Status", "eq", "RECEIVED"]]}
    )
    assert r.status_code == 200, r.json()
    assert [x["JobID"] for x in r.json()] == submitted_job_ids

    r = normal_user_client.post(
        "/jobs/search", json={"parameters": ["JobID", "Status"]}
    )
    assert r.status_code == 200, r.json()
    assert r.json() == [
        {"JobID": jid, "Status": "RECEIVED"} for jid in submitted_job_ids
    ]

    r = normal_user_client.post(
        "/jobs/summary", json={"grouping": ["Status", "OwnerDN"]}
    )
    assert r.status_code == 200, r.json()
    assert r.json() == [{"Status": "RECEIVED", "OwnerDN": "ownerDN", "count": 1}]

    r = normal_user_client.post(
        "/jobs/summary",
        json={"grouping": ["Status"], "search": [["Status", "eq", "RECEIVED"]]},
    )
    assert r.status_code == 200, r.json()
    assert r.json() == [{"Status": "RECEIVED", "count": 1}]

    r = normal_user_client.post(
        "/jobs/summary",
        json={"grouping": ["Status"], "search": [["Status", "eq", "NEW"]]},
    )
    assert r.status_code == 200, r.json()
    assert r.json() == []
