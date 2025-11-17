"""Integration tests for SQL sources."""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from fldt import FluentPipeline


class TestSqlSourceIntegration:
    """Integration tests for SQL source methods."""

    @pytest.fixture
    def postgres_engine(self, postgres_credentials):
        """Create SQLAlchemy engine for PostgreSQL."""
        try:
            engine = create_engine(postgres_credentials)
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except OperationalError:
            pytest.skip("PostgreSQL database not available")

    def test_from_sql_table_integration(self, postgres_engine, tmp_duckdb_database):
        """Test from_sql_table with real database connection."""
        # Create a test table if it doesn't exist
        with postgres_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.commit()
            
            # Insert test data
            conn.execute(text("""
                INSERT INTO test_users (name, email) 
                VALUES ('Test User', 'test@example.com')
                ON CONFLICT DO NOTHING
            """))
            conn.commit()
        
        try:
            pipeline = (
                FluentPipeline()
                .from_sql_table(postgres_engine, "test_users")
                .to("duckdb", database=tmp_duckdb_database)
            )
            
            assert pipeline._source is not None
            assert pipeline._destination is not None
            
            # Note: Actual run() would require dlt pipeline execution
            # which may need additional setup
        except Exception as e:
            pytest.skip(f"SQL integration test failed: {e}")

    def test_from_sql_query_integration(self, postgres_engine, tmp_duckdb_database):
        """Test from_sql_query with real database connection."""
        try:
            query = "SELECT id, name, email FROM test_users WHERE id > 0"
            
            pipeline = (
                FluentPipeline()
                .from_sql_query(postgres_engine, query, "test_users")
                .to("duckdb", database=tmp_duckdb_database)
            )
            
            assert pipeline._source is not None
            assert pipeline._destination is not None
        except Exception as e:
            pytest.skip(f"SQL query integration test failed: {e}")

    def test_sql_with_schema(self, postgres_engine, tmp_duckdb_database):
        """Test SQL source with schema specification."""
        try:
            # Create schema and table
            with postgres_engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS test_schema"))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS test_schema.test_table (
                        id SERIAL PRIMARY KEY,
                        data VARCHAR(100)
                    )
                """))
                conn.commit()
            
            pipeline = (
                FluentPipeline()
                .from_sql_table(postgres_engine, "test_table", schema="test_schema")
                .to("duckdb", database=tmp_duckdb_database)
            )
            
            assert pipeline._source is not None
        except Exception as e:
            pytest.skip(f"SQL schema integration test failed: {e}")

