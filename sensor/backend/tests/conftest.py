from pathlib import Path

import pytest

from engine.data import load_dataset

DATA_DIR = Path(__file__).resolve().parents[3] / "data" / "raw" / "cmapss"


@pytest.fixture(scope="session")
def ds():
    return load_dataset(DATA_DIR)
