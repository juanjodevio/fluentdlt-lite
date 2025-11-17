"""Unit tests for destination methods."""

import pytest

from fldt import FluentPipeline, ValidationError


class TestTo:
    """Test to method."""

    def test_to_basic(self):
        """Test to with basic destination."""
        pipeline = FluentPipeline()
        result = pipeline.to("duckdb")
        
        assert result is pipeline
        assert pipeline._destination["destination"] == "duckdb"

    def test_to_with_kwargs(self):
        """Test to with additional kwargs."""
        pipeline = FluentPipeline()
        pipeline.to("duckdb", database="test.db")
        
        assert pipeline._destination["destination"] == "duckdb"
        assert pipeline._destination["database"] == "test.db"

    def test_to_none_destination(self):
        """Test to with None destination raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.to(None)

    def test_to_empty_destination(self):
        """Test to with empty destination raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.to("")

    def test_to_whitespace_destination(self):
        """Test to with whitespace-only destination raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.to("   ")


class TestToRedshift:
    """Test to_redshift method."""

    def test_to_redshift_basic(self):
        """Test to_redshift with basic parameters."""
        pipeline = FluentPipeline()
        result = pipeline.to_redshift("postgresql://test", "mydb")
        
        assert result is pipeline
        assert pipeline._destination["destination"] == "redshift"
        assert pipeline._destination["database"] == "mydb"
        assert pipeline._destination["credentials"] == "postgresql://test"

    def test_to_redshift_with_schema(self):
        """Test to_redshift with schema."""
        pipeline = FluentPipeline()
        pipeline.to_redshift("postgresql://test", "mydb", schema="public")
        
        assert pipeline._destination["schema"] == "public"

    def test_to_redshift_none_credentials(self):
        """Test to_redshift with None credentials raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.to_redshift(None, "mydb")

    def test_to_redshift_empty_database(self):
        """Test to_redshift with empty database raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.to_redshift("postgresql://test", "")

    def test_to_redshift_invalid_schema(self):
        """Test to_redshift with invalid schema raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.to_redshift("postgresql://test", "mydb", schema="   ")


class TestToS3:
    """Test to_s3 method."""

    def test_to_s3_basic(self):
        """Test to_s3 with basic parameters."""
        pipeline = FluentPipeline()
        result = pipeline.to_s3("s3://bucket")
        
        assert result is pipeline
        assert pipeline._destination["destination"] == "filesystem"
        assert pipeline._destination["bucket_url"] == "s3://bucket"
        assert pipeline._destination["format"] == "parquet"

    def test_to_s3_with_format(self):
        """Test to_s3 with different formats."""
        pipeline = FluentPipeline()
        pipeline.to_s3("s3://bucket", format="jsonl")
        
        assert pipeline._destination["format"] == "jsonl"
        
        pipeline = FluentPipeline()
        pipeline.to_s3("s3://bucket", format="csv")
        
        assert pipeline._destination["format"] == "csv"

    def test_to_s3_with_credentials(self):
        """Test to_s3 with credentials."""
        pipeline = FluentPipeline()
        mock_credentials = {"aws_access_key_id": "test"}
        pipeline.to_s3("s3://bucket", credentials=mock_credentials)
        
        assert pipeline._destination["credentials"] is mock_credentials

    def test_to_s3_empty_bucket_url(self):
        """Test to_s3 with empty bucket_url raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.to_s3("")

    def test_to_s3_invalid_format(self):
        """Test to_s3 with invalid format raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError) as exc_info:
            pipeline.to_s3("s3://bucket", format="invalid")
        
        assert "parquet, jsonl, csv" in str(exc_info.value)

