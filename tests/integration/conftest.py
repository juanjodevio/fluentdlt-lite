"""Pytest fixtures for integration tests."""

import pytest
from pathlib import Path


@pytest.fixture
def postgres_credentials():
    """Provide test PostgreSQL credentials."""
    return "postgresql://test:test@localhost:5432/testdb"


@pytest.fixture
def duckdb_path(tmp_path):
    """Provide temporary DuckDB path."""
    return tmp_path / "test.duckdb"


@pytest.fixture
def tmp_duckdb_database(tmp_path):
    """Provide temporary DuckDB database path for integration tests."""
    db_path = tmp_path / "integration_test.duckdb"
    return str(db_path)

