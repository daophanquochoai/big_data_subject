from pathlib import Path
import os

import pytest

from engine.data import load_dataset

ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = Path(os.environ.get('CMAPSS_DATA_DIR', str(ROOT / 'data/raw/cmapss')))
if not DATA_DIR.is_absolute():
    DATA_DIR = ROOT / DATA_DIR


@pytest.fixture(scope="session")
def ds():
    return load_dataset(DATA_DIR)
