"""Integration tests for S3 sources."""

import pytest

from fldt import FluentPipeline


class TestS3SourceIntegration:
    """Integration tests for S3 source methods."""

    def test_from_s3_basic_configuration(self, tmp_duckdb_database):
        """Test from_s3 basic configuration without actual S3 connection."""
        # This test verifies configuration without requiring actual S3 access
        pipeline = (
            FluentPipeline()
            .from_s3("s3://test-bucket", file_glob="*.csv")
            .to("duckdb", database=tmp_duckdb_database)
        )
        
        assert pipeline._source is not None
        assert pipeline._destination is not None

    def test_from_s3_with_file_glob(self, tmp_duckdb_database):
        """Test from_s3 with different file glob patterns."""
        pipeline = (
            FluentPipeline()
            .from_s3("s3://test-bucket", file_glob="data/*.parquet")
            .to("duckdb", database=tmp_duckdb_database)
        )
        
        assert pipeline._source is not None

    def test_from_s3_with_extract_content(self, tmp_duckdb_database):
        """Test from_s3 with extract_content option."""
        pipeline = (
            FluentPipeline()
            .from_s3("s3://test-bucket", extract_content=True)
            .to("duckdb", database=tmp_duckdb_database)
        )
        
        assert pipeline._source is not None

    def test_from_s3_with_custom_files_per_page(self, tmp_duckdb_database):
        """Test from_s3 with custom files_per_page."""
        pipeline = (
            FluentPipeline()
            .from_s3("s3://test-bucket", files_per_page=5000)
            .to("duckdb", database=tmp_duckdb_database)
        )
        
        assert pipeline._source is not None

    def test_s3_to_s3_pipeline(self):
        """Test S3 to S3 pipeline configuration."""
        # This test verifies configuration without requiring actual S3 access
        pipeline = (
            FluentPipeline()
            .from_s3("s3://source-bucket", file_glob="*.csv")
            .to_s3("s3://dest-bucket", format="parquet")
        )
        
        assert pipeline._source is not None
        assert pipeline._destination is not None
        assert pipeline._destination["destination"] == "filesystem"
        assert pipeline._destination["format"] == "parquet"

    # Note: Actual S3 integration tests would require:
    # - AWS credentials configured
    # - Test S3 buckets
    # - Mock S3 service or localstack
    # These are left as configuration tests for now

