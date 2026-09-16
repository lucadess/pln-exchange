import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# PySpark needs a JVM. If JAVA_HOME isn't set, fall back to a Homebrew-installed
# JDK known to work with Spark (very new JDKs, e.g. 24+, break Spark's Hadoop
# shim). This only affects local test runs, not the shipped pipeline.
if "JAVA_HOME" not in os.environ:
    for formula in ("openjdk@17", "openjdk@21"):
        try:
            prefix = subprocess.run(
                ["brew", "--prefix", formula], capture_output=True, text=True, check=True
            ).stdout.strip()
            os.environ["JAVA_HOME"] = prefix
            break
        except (FileNotFoundError, subprocess.CalledProcessError):
            continue
