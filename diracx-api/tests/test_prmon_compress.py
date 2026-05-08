"""Tests for OnlineCompressor."""

from __future__ import annotations


def test_linear_segment_compressed():
    """Linear growth should compress to just start and end points."""
    from diracx.api.prmon_compress import OnlineCompressor

    compressor = OnlineCompressor(
        changing_metrics=["pss"],
        steady_metrics=[],
        precision=0.05,
    )
    for i in range(10):
        compressor.add_row({"Time": i, "pss": 1000 * (i + 1)})
    compressor.flush()

    assert len(compressor.compressed) == 2
    assert compressor.compressed[0]["Time"] == 0
    assert compressor.compressed[-1]["Time"] == 9


def test_spike_preserved():
    """A memory spike should not be dropped."""
    from diracx.api.prmon_compress import OnlineCompressor

    compressor = OnlineCompressor(
        changing_metrics=["pss"],
        steady_metrics=[],
        precision=0.05,
    )
    for t in range(5):
        compressor.add_row({"Time": t, "pss": 1000})
    compressor.add_row({"Time": 5, "pss": 50000})
    for t in range(6, 11):
        compressor.add_row({"Time": t, "pss": 1000})
    compressor.flush()

    pss_values = [r["pss"] for r in compressor.compressed]
    assert 50000 in pss_values


def test_steady_metric_change_kept():
    """A change in nprocs should force the row to be kept."""
    from diracx.api.prmon_compress import OnlineCompressor

    compressor = OnlineCompressor(
        changing_metrics=["pss"],
        steady_metrics=["nprocs"],
        precision=0.05,
    )
    for t in range(10):
        nprocs = 1 if t < 5 else 2
        compressor.add_row({"Time": t, "pss": 1000 * (t + 1), "nprocs": nprocs})
    compressor.flush()

    nprocs_at_change = [r for r in compressor.compressed if r["Time"] == 4]
    assert len(nprocs_at_change) == 1
    assert nprocs_at_change[0]["nprocs"] == 1
    nprocs_after = [r for r in compressor.compressed if r["Time"] == 5]
    assert len(nprocs_after) == 1
    assert nprocs_after[0]["nprocs"] == 2


def test_flush_commits_pending():
    """flush() must commit the last pending row."""
    from diracx.api.prmon_compress import OnlineCompressor

    compressor = OnlineCompressor(
        changing_metrics=["pss"],
        steady_metrics=[],
        precision=0.05,
    )
    compressor.add_row({"Time": 0, "pss": 100})
    compressor.add_row({"Time": 1, "pss": 200})
    assert len(compressor.compressed) == 1
    compressor.flush()
    assert len(compressor.compressed) == 2
    assert compressor.compressed[-1]["Time"] == 1


def test_single_row():
    """A single row should be committed immediately."""
    from diracx.api.prmon_compress import OnlineCompressor

    compressor = OnlineCompressor(
        changing_metrics=["pss"],
        steady_metrics=[],
    )
    compressor.add_row({"Time": 0, "pss": 100})
    compressor.flush()
    assert len(compressor.compressed) == 1
