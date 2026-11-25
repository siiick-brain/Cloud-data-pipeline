"""Fill an empty AIRFLOW_FERNET_KEY line in an env file with a fresh key. Stdlib only.

Run: python3 infra/fernet_key.py .env
"""

from __future__ import annotations

import base64
import os
import sys
from pathlib import Path


def fill(path: Path) -> None:
    key = base64.urlsafe_b64encode(os.urandom(32)).decode()
    lines = path.read_text().splitlines()
    lines = [f"AIRFLOW_FERNET_KEY={key}" if line == "AIRFLOW_FERNET_KEY=" else line for line in lines]
    path.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    fill(Path(sys.argv[1]))
