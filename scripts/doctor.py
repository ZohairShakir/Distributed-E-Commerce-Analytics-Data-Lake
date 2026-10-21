"""Print the exact local execution environment for reproducibility."""

from __future__ import annotations

import os
import platform
import subprocess
import sys

import boto3
import pyspark


def main() -> None:
    java_version = subprocess.run(
        ["java", "-version"], check=True, capture_output=True, text=True
    ).stderr.splitlines()[0]
    print(f"Python: {sys.version.split()[0]}")
    print(f"Java: {java_version}")
    print(f"PySpark: {pyspark.__version__}")
    print(f"Boto3: {boto3.__version__}")
    print(f"Platform: {platform.platform()}")
    print(f"Spark master: {os.getenv('SPARK_MASTER', 'local[2]')}")
    print(f"Spark driver memory: {os.getenv('SPARK_DRIVER_MEMORY', '1g')}")
    print(f"Storage mode: {os.getenv('STORAGE_MODE', 'local')}")


if __name__ == "__main__":
    main()
