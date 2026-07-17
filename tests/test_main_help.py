# tests/test_main_help.py
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_help_exits_zero_without_db():
    # --help must parse before any DB/network work.
    result = subprocess.run(
        [sys.executable, str(ROOT / "main.py"), "--help"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert result.returncode == 0
    assert "ingestion" in result.stdout.lower()
