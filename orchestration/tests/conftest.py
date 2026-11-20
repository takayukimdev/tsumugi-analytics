"""Isolate Airflow from ~/airflow before anything imports it."""

import os
import sys
import tempfile
from pathlib import Path

os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-home-"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))
