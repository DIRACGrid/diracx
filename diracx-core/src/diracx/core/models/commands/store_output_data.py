"""Post-processing command that stores output files to grid storage."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Sequence

from diracx.core.models.commands.core import PostProcessCommand

logger = logging.getLogger(__name__)


class StoreOutputDataCommand(PostProcessCommand):
    """Store output files to grid storage elements via DataManager.

    Replaces the output storage logic previously in ExecutionHooksBasePlugin.
    """

    def __init__(
        self,
        output_paths: dict[str, str],
        output_se: list[str],
    ) -> None:
        self._output_paths = output_paths
        self._output_se = output_se
        self._datamanager = None

    def _get_datamanager(self):
        if self._datamanager is None:
            from DIRAC.DataManagementSystem.Client.DataManager import (
                DataManager,  # type: ignore[import-untyped]
            )

            self._datamanager = DataManager()
        return self._datamanager

    async def execute(self, job_path: Path, **kwargs: Any) -> None:
        """Store output files to grid storage.

        :param job_path: Path to the job working directory.
        :param kwargs: Must include 'outputs' dict mapping output names to file paths.
        """
        outputs: dict[str, str | Path | Sequence[str | Path]] = kwargs.get(
            "outputs", {}
        )

        for output_name, src_path in outputs.items():
            if not src_path:
                raise RuntimeError(
                    f"src_path parameter required for filesystem storage of {output_name}"
                )

            lfn = self._output_paths.get(output_name, None)
            if not lfn:
                continue

            logger.info("Storing output %s, with source %s", output_name, src_path)
            if isinstance(src_path, (str, Path)):
                src_path = [src_path]

            for src in src_path:
                # Resolve relative paths against the job working directory
                local_path = Path(src)
                if not local_path.is_absolute():
                    local_path = (job_path / local_path).resolve()

                if not local_path.exists():
                    raise RuntimeError(
                        f"Output file {local_path} does not exist for output '{output_name}'"
                    )

                file_lfn = str(Path(lfn) / local_path.name)

                # TODO: Compute Adler32 checksum before upload
                # TODO: Extract POOL/ROOT GUID if applicable
                # TODO: Prefer local SEs (getSEsForSite) before remote ones
                # TODO: Implement retry with exponential backoff on transient failures
                # TODO: On complete failure, create a failover Request (RMS)
                #       for async recovery instead of raising immediately
                # TODO: Report upload progress via job status updates

                uploaded = False
                last_error = ""
                for se in self._output_se:
                    result = self._get_datamanager().putAndRegister(
                        file_lfn, str(local_path), se
                    )
                    if not result["OK"]:
                        last_error = result["Message"]
                        logger.warning(
                            "Failed to upload %s to %s: %s", local_path, se, last_error
                        )
                        continue

                    # putAndRegister returns {lfn: {'put': ..., 'register': ...}}
                    lfn_result = result["Value"].get("Successful", {}).get(file_lfn)
                    if lfn_result:
                        logger.info(
                            "Successfully stored %s with LFN %s on %s",
                            local_path,
                            file_lfn,
                            se,
                        )
                        uploaded = True
                        break

                    failed = result["Value"].get("Failed", {}).get(file_lfn, "")
                    last_error = str(failed)
                    logger.warning(
                        "putAndRegister reported failure for %s on %s: %s",
                        file_lfn,
                        se,
                        last_error,
                    )

                if not uploaded:
                    # TODO: Instead of raising, create a failover Request
                    raise RuntimeError(
                        f"Could not store {local_path} as {file_lfn} "
                        f"on any SE {self._output_se}: {last_error}"
                    )
