"""Remove only project-generated data directories."""

from __future__ import annotations

import shutil
from pathlib import Path


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    targets = (
        project_root / "data" / "raw",
        project_root / "data" / "phase1",
        project_root / "data" / "lake",
    )
    for target in targets:
        if target.exists():
            shutil.rmtree(target)
            print(f"Removed {target.relative_to(project_root)}")


if __name__ == "__main__":
    main()
