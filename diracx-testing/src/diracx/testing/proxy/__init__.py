from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path

from DIRAC.Core.Security.VOMS import voms_init_cmd
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
from sqlalchemy import insert

from diracx.db.sql.proxy.schema import CleanProxies

TEST_NAME = "testuser"
TEST_DN = "/O=Dirac Computing/O=CERN/CN=MrUser"
TEST_DATA_DIR = Path(__file__).parent / "data"
TEST_PEM_PATH = TEST_DATA_DIR / "proxy.pem"


@wraps(voms_init_cmd)
def voms_init_cmd_fake(vo, *args, **kwargs):
    cmd = voms_init_cmd(*args, **kwargs)

    new_cmd = ["voms-proxy-fake"]
    i = 1
    while i < len(cmd):
        # Some options are not supported by voms-proxy-fake
        if cmd[i] in {"-valid", "-vomses", "-timeout"}:
            i += 2
            continue
        new_cmd.append(cmd[i])
        i += 1
    new_cmd.extend(
        [
            "-hostcert",
            f"{TEST_DATA_DIR}/certs/host/hostcert.pem",
            "-hostkey",
            f"{TEST_DATA_DIR}/certs/host/hostkey.pem",
            "-fqan",
            f"/{vo}/Role=NULL/Capability=NULL",
        ]
    )
    return new_cmd


async def insert_proxy(conn):
    await conn.execute(
        insert(CleanProxies).values(
            UserName=TEST_NAME,
            UserDN=TEST_DN,
            ProxyProvider="Certificate",
            Pem=TEST_PEM_PATH.read_bytes(),
            ExpirationTime=datetime(2033, 11, 25, 21, 25, 23, tzinfo=timezone.utc),
        )
    )


def check_proxy_string(vo, pem_data):
    proxy_chain = X509Chain()
    returnValueOrRaise(proxy_chain.loadProxyFromString(pem_data))

    # Check validity
    not_after = returnValueOrRaise(proxy_chain.getNotAfterDate()).replace(
        tzinfo=timezone.utc
    )
    # The proxy should currently be valid
    assert datetime.now(timezone.utc) < not_after
    # The proxy should be invalid in less than 3601 seconds
    time_left = not_after - datetime.now(timezone.utc)
    assert time_left < timedelta(hours=1, seconds=1)

    # Check VOMS data
    voms_data = returnValueOrRaise(proxy_chain.getVOMSData())
    assert voms_data["vo"] == vo
    assert voms_data["fqan"] == [f"/{vo}/Role=NULL/Capability=NULL"]
