from __future__ import annotations

import asyncio
import os
import stat
from datetime import datetime, timezone
from pathlib import Path
from subprocess import DEVNULL, PIPE, STDOUT
from tempfile import TemporaryDirectory

from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.VOMS import voms_init_cmd
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
from sqlalchemy import select

from diracx.core.exceptions import ProxyNotFoundError, VOMSInitError
from diracx.db.sql.utils import BaseSQLDB, utcnow

from .schema import Base as ProxyDBBase
from .schema import CleanProxies

PROXY_PROVIDER = "Certificate"


class ProxyDB(BaseSQLDB):
    metadata = ProxyDBBase.metadata

    async def get_proxy(
        self,
        dn: str,
        vo: str,
        dirac_group: str,
        voms_attr: str | None,
        lifetime_seconds: int,
    ) -> str:
        """Generate a new proxy for the given DN as PEM with the given VOMS extension"""
        original_chain = await self.get_stored_proxy(
            dn, min_lifetime_seconds=lifetime_seconds
        )

        proxy_string = returnValueOrRaise(
            original_chain.generateProxyToString(
                lifetime_seconds,
                diracGroup=dirac_group,
                strength=returnValueOrRaise(original_chain.getStrength()),
            )
        )
        proxy_chain = X509Chain()
        proxy_chain.loadProxyFromString(proxy_string)

        with TemporaryDirectory() as tmpdir:
            in_fn = Path(tmpdir) / "in.pem"
            in_fn.touch(stat.S_IRUSR | stat.S_IWUSR)
            in_fn.write_text(proxy_string)
            out_fn = Path(tmpdir) / "out.pem"

            cmd = voms_init_cmd(
                vo,
                voms_attr,
                proxy_chain,
                str(in_fn),
                str(out_fn),
                Locations.getVomsesLocation(),
            )
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=DEVNULL,
                stdout=PIPE,
                stderr=STDOUT,
                env=os.environ
                | {
                    "X509_CERT_DIR": Locations.getCAsLocationNoConfig(),
                    "X509_VOMS_DIR": Locations.getVomsdirLocation(),
                },
            )
            await proc.wait()
            if proc.returncode != 0:
                assert proc.stdout
                message = (await proc.stdout.read()).decode("utf-8", "backslashreplace")
                raise VOMSInitError(
                    f"voms-proxy-init failed with return code {proc.returncode}: {message}"
                )

            voms_string = out_fn.read_text()

        return voms_string

    async def get_stored_proxy(
        self, dn: str, *, min_lifetime_seconds: int
    ) -> X509Chain:
        """Get the X509 proxy that is stored in the DB for the given DN

        NOTE: This is the original long-lived proxy and should only be used to
        generate short-lived proxies!!!
        """
        stmt = select(CleanProxies.Pem, CleanProxies.ExpirationTime)
        stmt = stmt.where(
            CleanProxies.UserDN == dn,
            CleanProxies.ExpirationTime > utcnow(),
            CleanProxies.ProxyProvider == PROXY_PROVIDER,
        )

        for pem_data, expiration_time in (await self.conn.execute(stmt)).all():
            seconds_remaining = (
                expiration_time.replace(tzinfo=timezone.utc)
                - datetime.now(timezone.utc)
            ).total_seconds()
            if seconds_remaining <= min_lifetime_seconds:
                continue

            pem_data = pem_data.decode("ascii")
            if not pem_data:
                continue
            chain = X509Chain()
            returnValueOrRaise(chain.loadProxyFromString(pem_data))
            return chain
        raise ProxyNotFoundError(
            f"No proxy found for {dn} with over {min_lifetime_seconds} seconds of life"
        )
