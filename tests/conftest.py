import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES = REPO_ROOT / "tests" / "fixtures"

# scripts/ is not an installed package, so make it importable for tests.
sys.path.insert(0, str(REPO_ROOT / "scripts"))


@pytest.fixture
def fixtures() -> Path:
    return FIXTURES


@pytest.fixture
def sample_csv(fixtures: Path) -> str:
    return (fixtures / "sample_dispatchscada.csv").read_text()
