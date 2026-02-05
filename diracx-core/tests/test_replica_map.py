from __future__ import annotations

import pytest

from diracx.core.models.replica_map import (
    ReplicaMap,
    _validate_adler32,
    _validate_guid,
    _validate_lfn,
    _validate_pfn,
)


class TestValidateLFN:
    """Tests for _validate_lfn function."""

    def test_valid_lfn(self):
        """Test that valid LFN paths are accepted."""
        assert _validate_lfn("/lhcb/MC/2024/file.dst") == "/lhcb/MC/2024/file.dst"

    def test_lfn_with_prefix(self):
        """Test that LFN: prefix is stripped."""
        assert _validate_lfn("LFN:/lhcb/MC/2024/file.dst") == "/lhcb/MC/2024/file.dst"

    def test_empty_lfn_raises_error(self):
        """Test that empty LFN raises ValueError."""
        with pytest.raises(ValueError, match="LFN cannot be empty"):
            _validate_lfn("")

    def test_lfn_prefix_only_raises_error(self):
        """Test that LFN: prefix alone raises ValueError."""
        with pytest.raises(ValueError, match="LFN cannot be empty"):
            _validate_lfn("LFN:")


class TestValidatePFN:
    """Tests for _validate_pfn function."""

    def test_valid_pfn(self):
        """Test that valid PFN URLs are accepted."""
        assert (
            _validate_pfn("https://example.com/path/file.dst")
            == "https://example.com/path/file.dst"
        )

    def test_pfn_with_prefix(self):
        """Test that PFN: prefix is stripped."""
        assert (
            _validate_pfn("PFN:https://example.com/path/file.dst")
            == "https://example.com/path/file.dst"
        )

    def test_empty_pfn_raises_error(self):
        """Test that empty PFN raises ValueError."""
        with pytest.raises(ValueError, match="PFN cannot be empty"):
            _validate_pfn("")

    def test_pfn_prefix_only_raises_error(self):
        """Test that PFN: prefix alone raises ValueError."""
        with pytest.raises(ValueError, match="PFN cannot be empty"):
            _validate_pfn("PFN:")


class TestValidateAdler32:
    """Tests for _validate_adler32 function."""

    def test_valid_adler32_lowercase(self):
        """Test that valid lowercase adler32 is accepted."""
        assert _validate_adler32("788c5caa") == "788c5caa"

    def test_valid_adler32_uppercase(self):
        """Test that valid uppercase adler32 is normalized to lowercase."""
        assert _validate_adler32("788C5CAA") == "788c5caa"

    def test_valid_adler32_mixed_case(self):
        """Test that valid mixed case adler32 is normalized to lowercase."""
        assert _validate_adler32("788c5CAA") == "788c5caa"

    def test_adler32_too_short_raises_error(self):
        """Test that too short adler32 raises ValueError."""
        with pytest.raises(ValueError, match="must be 8 characters long"):
            _validate_adler32("788c5ca")

    def test_adler32_too_long_raises_error(self):
        """Test that too long adler32 raises ValueError."""
        with pytest.raises(ValueError, match="must be 8 characters long"):
            _validate_adler32("788c5caaa")

    def test_adler32_invalid_characters_raises_error(self):
        """Test that non-hex characters in adler32 raise ValueError."""
        with pytest.raises(
            ValueError, match="must contain only hexadecimal characters"
        ):
            _validate_adler32("788c5cag")

    def test_adler32_with_spaces_raises_error(self):
        """Test that adler32 with spaces raises ValueError."""
        with pytest.raises(
            ValueError, match="must contain only hexadecimal characters"
        ):
            _validate_adler32("788c5ca ")


class TestValidateGUID:
    """Tests for _validate_guid function."""

    def test_valid_guid_uppercase(self):
        """Test that valid uppercase GUID is accepted."""
        assert (
            _validate_guid("6032CB7C-32DC-EC11-9A66-D85ED3091D71")
            == "6032CB7C-32DC-EC11-9A66-D85ED3091D71"
        )

    def test_valid_guid_lowercase(self):
        """Test that valid lowercase GUID is normalized to uppercase."""
        assert (
            _validate_guid("6032cb7c-32dc-ec11-9a66-d85ed3091d71")
            == "6032CB7C-32DC-EC11-9A66-D85ED3091D71"
        )

    def test_valid_guid_mixed_case(self):
        """Test that valid mixed case GUID is normalized to uppercase."""
        assert (
            _validate_guid("6032cb7C-32DC-ec11-9A66-d85eD3091d71")
            == "6032CB7C-32DC-EC11-9A66-D85ED3091D71"
        )

    def test_guid_wrong_length_raises_error(self):
        """Test that GUID with wrong length raises ValueError."""
        with pytest.raises(ValueError, match="must be 36 characters long"):
            _validate_guid("6032CB7C-32DC-EC11-9A66-D85ED3091D7")

    def test_guid_without_hyphens_raises_error(self):
        """Test that GUID without hyphens raises ValueError."""
        with pytest.raises(ValueError, match="must be 36 characters long"):
            _validate_guid("6032CB7C32DCEC119A66D85ED3091D71")

    def test_guid_wrong_format_raises_error(self):
        """Test that GUID with wrong format raises ValueError."""
        with pytest.raises(ValueError, match="must follow format 8-4-4-4-12"):
            _validate_guid("6032CB7C-32DC-EC11-9A66D-85ED3091D71")

    def test_guid_invalid_characters_raises_error(self):
        """Test that GUID with invalid characters raises ValueError."""
        with pytest.raises(ValueError, match="must follow format 8-4-4-4-12"):
            _validate_guid("6032CB7G-32DC-EC11-9A66-D85ED3091D71")


class TestReplicaMapEntry:
    """Tests for ReplicaMap.MapEntry model."""

    def test_valid_map_entry(self):
        """Test that valid map entry is created successfully."""
        entry = ReplicaMap.MapEntry(
            replicas=[
                ReplicaMap.MapEntry.Replica(
                    url="https://example.com/file.dst", se="SE1"
                )
            ],
            size_bytes=1024,
            checksum=ReplicaMap.MapEntry.Checksum(adler32="788c5caa"),
        )
        assert len(entry.replicas) == 1
        assert entry.size_bytes == 1024
        assert entry.checksum.adler32 == "788c5caa"

    def test_entry_without_optional_fields(self):
        """Test that map entry without optional fields is valid."""
        entry = ReplicaMap.MapEntry(
            replicas=[
                ReplicaMap.MapEntry.Replica(
                    url="https://example.com/file.dst", se="SE1"
                )
            ]
        )
        assert len(entry.replicas) == 1
        assert entry.size_bytes is None
        assert entry.checksum is None

    def test_entry_empty_replicas_raises_error(self):
        """Test that empty replicas list raises ValueError."""
        with pytest.raises(ValueError, match="At least one replica is required"):
            ReplicaMap.MapEntry(replicas=[])

    def test_entry_negative_size_raises_error(self):
        """Test that negative size_bytes raises ValueError."""
        with pytest.raises(
            ValueError, match="Size in bytes cannot be zero or negative"
        ):
            ReplicaMap.MapEntry(
                replicas=[
                    ReplicaMap.MapEntry.Replica(
                        url="https://example.com/file.dst", se="SE1"
                    )
                ],
                size_bytes=-1,
            )

    def test_entry_zero_size_is_invalid(self):
        """Test that zero size_bytes is invalid."""
        with pytest.raises(
            ValueError, match="Size in bytes cannot be zero or negative"
        ):
            ReplicaMap.MapEntry(
                replicas=[
                    ReplicaMap.MapEntry.Replica(
                        url="https://example.com/file.dst", se="SE1"
                    )
                ],
                size_bytes=0,
            )

    """Tests for ReplicaMap.MapEntry.Replica model."""

    def test_valid_replica(self):
        """Test that valid replica is created successfully."""
        replica = ReplicaMap.MapEntry.Replica(
            url="https://example.com/file.dst", se="SE1"
        )
        assert str(replica.url) == "https://example.com/file.dst"
        assert replica.se == "SE1"

    def test_replica_with_pfn_prefix(self):
        """Test that PFN: prefix is stripped from URL."""
        replica = ReplicaMap.MapEntry.Replica(
            url="PFN:https://example.com/file.dst", se="SE1"
        )
        assert str(replica.url) == "https://example.com/file.dst"

    def test_replica_empty_se_raises_error(self):
        """Test that empty storage element raises ValueError."""
        with pytest.raises(ValueError, match="Storage Element ID cannot be empty"):
            ReplicaMap.MapEntry.Replica(url="https://example.com/file.dst", se="")

    def test_replica_whitespace_se_raises_error(self):
        """Test that whitespace-only storage element raises ValueError."""
        with pytest.raises(ValueError, match="Storage Element ID cannot be empty"):
            ReplicaMap.MapEntry.Replica(url="https://example.com/file.dst", se="   ")

    def test_replica_se_is_stripped(self):
        """Test that storage element is stripped of whitespace."""
        replica = ReplicaMap.MapEntry.Replica(
            url="https://example.com/file.dst", se="  SE1  "
        )
        assert replica.se == "SE1"


class TestReplicaMapChecksum:
    """Tests for ReplicaMap.MapEntry.Checksum model."""

    def test_checksum_with_adler32(self):
        """Test checksum with only adler32."""
        checksum = ReplicaMap.MapEntry.Checksum(adler32="788c5caa")
        assert checksum.adler32 == "788c5caa"
        assert checksum.guid is None

    def test_checksum_with_guid(self):
        """Test checksum with only GUID."""
        checksum = ReplicaMap.MapEntry.Checksum(
            guid="6032CB7C-32DC-EC11-9A66-D85ED3091D71"
        )
        assert checksum.adler32 is None
        assert checksum.guid == "6032CB7C-32DC-EC11-9A66-D85ED3091D71"

    def test_checksum_with_both(self):
        """Test checksum with both adler32 and GUID."""
        checksum = ReplicaMap.MapEntry.Checksum(
            adler32="788c5caa", guid="6032CB7C-32DC-EC11-9A66-D85ED3091D71"
        )
        assert checksum.adler32 == "788c5caa"
        assert checksum.guid == "6032CB7C-32DC-EC11-9A66-D85ED3091D71"

    def test_checksum_empty_is_valid(self):
        """Test that checksum with no values is valid."""
        checksum = ReplicaMap.MapEntry.Checksum()
        assert checksum.adler32 is None
        assert checksum.guid is None


class TestReplicaMap:
    """Tests for ReplicaMap model."""

    def test_valid_map(self):
        """Test that valid map is created successfully."""
        replica_map = ReplicaMap(
            root={
                "/lhcb/MC/2024/file.dst": ReplicaMap.MapEntry(
                    replicas=[
                        ReplicaMap.MapEntry.Replica(
                            url="https://example.com/file.dst", se="SE1"
                        )
                    ]
                )
            }
        )
        assert "/lhcb/MC/2024/file.dst" in replica_map

    def test_map_with_lfn_prefix(self):
        """Test that LFN: prefix is stripped from keys."""
        replica_map = ReplicaMap(
            root={
                "LFN:/lhcb/MC/2024/file.dst": ReplicaMap.MapEntry(
                    replicas=[
                        ReplicaMap.MapEntry.Replica(
                            url="https://example.com/file.dst", se="SE1"
                        )
                    ]
                )
            }
        )
        assert "/lhcb/MC/2024/file.dst" in replica_map

    def test_map_iteration(self):
        """Test that map can be iterated."""
        replica_map = ReplicaMap(
            root={
                "/lhcb/MC/2024/file1.dst": ReplicaMap.MapEntry(
                    replicas=[
                        ReplicaMap.MapEntry.Replica(
                            url="https://example.com/file1.dst", se="SE1"
                        )
                    ]
                ),
                "/lhcb/MC/2024/file2.dst": ReplicaMap.MapEntry(
                    replicas=[
                        ReplicaMap.MapEntry.Replica(
                            url="https://example.com/file2.dst", se="SE2"
                        )
                    ]
                ),
            }
        )
        lfns = list(replica_map)
        assert len(lfns) == 2
        assert "/lhcb/MC/2024/file1.dst" in lfns
        assert "/lhcb/MC/2024/file2.dst" in lfns

    def test_map_getitem(self):
        """Test that map entries can be accessed by LFN."""
        replica_map = ReplicaMap(
            root={
                "/lhcb/MC/2024/file.dst": ReplicaMap.MapEntry(
                    replicas=[
                        ReplicaMap.MapEntry.Replica(
                            url="https://example.com/file.dst", se="SE1"
                        )
                    ],
                    size_bytes=1024,
                )
            }
        )
        entry = replica_map["/lhcb/MC/2024/file.dst"]
        assert entry.size_bytes == 1024

    def test_map_getitem_missing_key(self):
        """Test that accessing missing key raises KeyError."""
        replica_map = ReplicaMap(root={})
        with pytest.raises(KeyError):
            _ = replica_map["/nonexistent/file.dst"]

    def test_map_multiple_replicas(self):
        """Test map entry with multiple replicas."""
        replica_map = ReplicaMap(
            root={
                "/lhcb/MC/2024/file.dst": ReplicaMap.MapEntry(
                    replicas=[
                        ReplicaMap.MapEntry.Replica(
                            url="https://site1.com/file.dst", se="SE1"
                        ),
                        ReplicaMap.MapEntry.Replica(
                            url="https://site2.com/file.dst", se="SE2"
                        ),
                    ]
                )
            }
        )
        entry = replica_map["/lhcb/MC/2024/file.dst"]
        assert len(entry.replicas) == 2

    def test_map_from_dict(self):
        """Test creating map from plain dict (JSON-like structure)."""
        data = {
            "/lhcb/MC/2024/file.dst": {
                "replicas": [{"url": "https://example.com/file.dst", "se": "SE1"}],
                "size_bytes": 2048,
                "checksum": {"adler32": "788c5caa"},
            }
        }
        replica_map = ReplicaMap(root=data)
        entry = replica_map["/lhcb/MC/2024/file.dst"]
        assert entry.size_bytes == 2048
        assert entry.checksum.adler32 == "788c5caa"
