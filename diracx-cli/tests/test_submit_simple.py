from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from typer.testing import CliRunner

from diracx.cli._submission.simple import detect_sandbox_files, generate_cwl
from diracx.cli.job import app as job_app

runner = CliRunner()


class TestDetectSandboxFiles:
    def test_existing_relative_file(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "my_script.py").write_text("print('hello')")
        result = detect_sandbox_files("python my_script.py")
        assert Path("my_script.py") in result

    def test_ignores_nonexistent(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = detect_sandbox_files("python nonexistent.py")
        assert result == []

    def test_ignores_absolute_paths(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        abs_file = tmp_path / "script.py"
        abs_file.write_text("print('hello')")
        result = detect_sandbox_files(f"python {abs_file}")
        assert result == []

    def test_ignores_directories(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "mydir").mkdir()
        result = detect_sandbox_files("ls mydir")
        assert result == []

    def test_ignores_symlinks(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        real = tmp_path / "real.py"
        real.write_text("print('hello')")
        link = tmp_path / "link.py"
        link.symlink_to(real)
        result = detect_sandbox_files("python link.py")
        assert result == []

    def test_ignores_commands(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = detect_sandbox_files("echo hello world")
        assert result == []

    def test_dedup_with_explicit(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "script.py").write_text("print('hello')")
        auto = detect_sandbox_files("python script.py")
        explicit = [Path("script.py")]
        combined = list(set(auto + explicit))
        assert len(combined) == 1


class TestGenerateCWL:
    def test_basic_generation(self):
        cwl = generate_cwl(
            command="python my_script.py",
            sandbox_files=[Path("my_script.py")],
        )
        assert cwl["cwlVersion"] == "v1.2"
        assert cwl["class"] == "CommandLineTool"
        assert cwl["baseCommand"] == ["bash", "-c", "python my_script.py"]
        assert "dirac:Job" in str(cwl["hints"])

    def test_no_sandbox(self):
        cwl = generate_cwl(command="echo hello", sandbox_files=[])
        assert cwl["baseCommand"] == ["bash", "-c", "echo hello"]

    def test_captures_stdout_stderr(self):
        """Generated CWL should capture tool stdout/stderr to log files."""
        cwl = generate_cwl(command="echo hello", sandbox_files=[])
        assert cwl["stdout"] == "stdout.log"
        assert cwl["stderr"] == "stderr.log"
        output_ids = {o["id"] for o in cwl["outputs"]}
        assert "stdout_log" in output_ids
        assert "stderr_log" in output_ids

    def test_label_derived_from_command(self):
        cwl = generate_cwl(command="python my_script.py", sandbox_files=[])
        assert "my_script" in cwl.get("label", "") or "python" in cwl.get("label", "")


class TestSubmitCommand:
    def test_basic_command(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "script.py").write_text("print('hello')")

        with patch(
            "diracx.cli.job.submit.cmd.submit_cwl", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = [MagicMock(job_id=1001, status="Submitting")]
            result = runner.invoke(
                job_app,
                ["submit", "cmd", "python script.py", "-y"],
            )

        assert result.exit_code == 0, result.output
        assert "1001" in result.output
        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs["workflow"].suffix == ".cwl"

    def test_explicit_sandbox(self, tmp_path: Path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "config.json").write_text("{}")

        with patch(
            "diracx.cli.job.submit.cmd.submit_cwl", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = [MagicMock(job_id=1001, status="Submitting")]
            result = runner.invoke(
                job_app,
                [
                    "submit",
                    "cmd",
                    "echo hello",
                    "--sandbox",
                    str(tmp_path / "config.json"),
                    "-y",
                ],
            )

        assert result.exit_code == 0, result.output
