"""Unit tests for validation logic."""

import pytest

from fldt import FluentPipeline, ValidationError


class TestValidateCredentials:
    """Test _validate_credentials method."""

    def test_validate_credentials_none(self):
        """Test validation fails with None credentials."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError) as exc_info:
            pipeline._validate_credentials(None, "SQL")
        
        assert "credentials required" in str(exc_info.value).lower()

    def test_validate_credentials_empty_string(self):
        """Test validation fails with empty string credentials."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError) as exc_info:
            pipeline._validate_credentials("   ", "SQL")
        
        assert "cannot be empty" in str(exc_info.value).lower()

    def test_validate_credentials_valid_string(self):
        """Test validation passes with valid string credentials."""
        pipeline = FluentPipeline()
        
        # Should not raise
        pipeline._validate_credentials("postgresql://test", "SQL")

    def test_validate_credentials_valid_object(self):
        """Test validation passes with valid object credentials."""
        pipeline = FluentPipeline()
        credentials = object()
        
        # Should not raise
        pipeline._validate_credentials(credentials, "SQL")


class TestValidateTableName:
    """Test _validate_table_name method."""

    def test_validate_table_name_none(self):
        """Test validation fails with None table name."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError) as exc_info:
            pipeline._validate_table_name(None)
        
        assert "non-empty string" in str(exc_info.value).lower()

    def test_validate_table_name_empty_string(self):
        """Test validation fails with empty string."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError) as exc_info:
            pipeline._validate_table_name("")
        
        assert "non-empty string" in str(exc_info.value).lower()

    def test_validate_table_name_whitespace(self):
        """Test validation fails with whitespace-only string."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError) as exc_info:
            pipeline._validate_table_name("   ")
        
        assert "whitespace" in str(exc_info.value).lower()

    def test_validate_table_name_non_string(self):
        """Test validation fails with non-string type."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline._validate_table_name(123)

    def test_validate_table_name_valid(self):
        """Test validation passes with valid table name."""
        pipeline = FluentPipeline()
        
        # Should not raise
        pipeline._validate_table_name("users")
        pipeline._validate_table_name("my_table")


class TestValidateQuery:
    """Test _validate_query method."""

    def test_validate_query_none(self):
        """Test validation fails with None query."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError) as exc_info:
            pipeline._validate_query(None)
        
        assert "non-empty string" in str(exc_info.value).lower()

    def test_validate_query_empty_string(self):
        """Test validation fails with empty string."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline._validate_query("")

    def test_validate_query_whitespace(self):
        """Test validation fails with whitespace-only string."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline._validate_query("   ")

    def test_validate_query_non_string(self):
        """Test validation fails with non-string type."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline._validate_query(123)

    def test_validate_query_valid(self):
        """Test validation passes with valid query."""
        pipeline = FluentPipeline()
        
        # Should not raise
        pipeline._validate_query("SELECT * FROM users")
        pipeline._validate_query("SELECT id, name FROM users WHERE id > 100")


class TestValidateDestinationString:
    """Test _validate_destination_string method."""

    def test_validate_destination_none(self):
        """Test validation fails with None destination."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError) as exc_info:
            pipeline._validate_destination_string(None)
        
        assert "non-empty string" in str(exc_info.value).lower()

    def test_validate_destination_empty_string(self):
        """Test validation fails with empty string."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline._validate_destination_string("")

    def test_validate_destination_whitespace(self):
        """Test validation fails with whitespace-only string."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline._validate_destination_string("   ")

    def test_validate_destination_non_string(self):
        """Test validation fails with non-string type."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline._validate_destination_string(123)

    def test_validate_destination_valid(self):
        """Test validation passes with valid destination."""
        pipeline = FluentPipeline()
        
        # Should not raise
        pipeline._validate_destination_string("duckdb")
        pipeline._validate_destination_string("postgres")
        pipeline._validate_destination_string("bigquery")

