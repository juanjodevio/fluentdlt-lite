"""Unit tests for source methods."""

import sys
import pytest
from unittest.mock import MagicMock, Mock

from fldt import FluentPipeline, ValidationError


class TestFromSqlTable:
    """Test from_sql_table method."""

    def test_from_sql_table_string_credentials(self):
        """Test from_sql_table with string credentials."""
        # Mock the sql_database module in sys.modules before import
        mock_sql_db_module = Mock()
        mock_source = MagicMock()
        mock_sql_table = MagicMock(return_value=mock_source)
        mock_sql_db_module.sql_table = mock_sql_table
        
        # Insert mock into sys.modules before import
        original_modules = sys.modules.copy()
        sys.modules["dlt.sources.sql_database"] = mock_sql_db_module
        
        try:
            pipeline = FluentPipeline()
            result = pipeline.from_sql_table("postgresql://test", "users")
            
            assert result is pipeline
            assert pipeline._source is mock_source
            mock_sql_table.assert_called_once()
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

    def test_from_sql_table_with_schema(self):
        """Test from_sql_table with schema."""
        # Mock the sql_database module in sys.modules before import
        mock_sql_db_module = Mock()
        mock_source = MagicMock()
        mock_sql_table = MagicMock(return_value=mock_source)
        mock_sql_db_module.sql_table = mock_sql_table
        
        # Insert mock into sys.modules before import
        original_modules = sys.modules.copy()
        sys.modules["dlt.sources.sql_database"] = mock_sql_db_module
        
        try:
            pipeline = FluentPipeline()
            pipeline.from_sql_table("postgresql://test", "users", schema="public")
            
            call_kwargs = mock_sql_table.call_args[1]
            assert call_kwargs["schema"] == "public"
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

    def test_from_sql_table_none_credentials(self):
        """Test from_sql_table with None credentials raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.from_sql_table(None, "users")

    def test_from_sql_table_empty_table_name(self):
        """Test from_sql_table with empty table name raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.from_sql_table("postgresql://test", "")

    def test_from_sql_table_invalid_schema(self):
        """Test from_sql_table with invalid schema raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.from_sql_table("postgresql://test", "users", schema="   ")


class TestFromSqlQuery:
    """Test from_sql_query method."""

    def test_from_sql_query_string_credentials(self):
        """Test from_sql_query with string credentials."""
        # Mock the sql_database module in sys.modules before import
        mock_sql_db_module = Mock()
        mock_db = MagicMock()
        mock_resource = MagicMock()
        mock_db.with_resources.return_value = mock_resource
        mock_sql_database = MagicMock(return_value=mock_db)
        mock_sql_db_module.sql_database = mock_sql_database
        
        # Insert mock into sys.modules before import
        original_modules = sys.modules.copy()
        sys.modules["dlt.sources.sql_database"] = mock_sql_db_module
        
        try:
            pipeline = FluentPipeline()
            result = pipeline.from_sql_query("postgresql://test", "SELECT * FROM users", "users")
            
            assert result is pipeline
            assert pipeline._source is mock_resource
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

    def test_from_sql_query_none_credentials(self):
        """Test from_sql_query with None credentials raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.from_sql_query(None, "SELECT * FROM users", "users")

    def test_from_sql_query_empty_query(self):
        """Test from_sql_query with empty query raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.from_sql_query("postgresql://test", "", "users")

    def test_from_sql_query_empty_table_name(self):
        """Test from_sql_query with empty table name raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.from_sql_query("postgresql://test", "SELECT * FROM users", "")


class TestFromS3:
    """Test from_s3 method."""

    def test_from_s3_basic(self):
        """Test from_s3 with basic parameters."""
        # Mock the filesystem module in sys.modules before import
        mock_fs_module = Mock()
        mock_source = MagicMock()
        mock_filesystem = MagicMock(return_value=mock_source)
        mock_fs_module.filesystem = mock_filesystem
        
        # Insert mock into sys.modules before import
        original_modules = sys.modules.copy()
        sys.modules["dlt.sources.filesystem"] = mock_fs_module
        
        try:
            pipeline = FluentPipeline()
            result = pipeline.from_s3("s3://bucket", file_glob="*.csv")
            
            assert result is pipeline
            assert pipeline._source is mock_source
            mock_filesystem.assert_called_once()
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

    def test_from_s3_with_credentials(self):
        """Test from_s3 with credentials."""
        # Mock the filesystem module in sys.modules before import
        mock_fs_module = Mock()
        mock_source = MagicMock()
        mock_filesystem = MagicMock(return_value=mock_source)
        mock_fs_module.filesystem = mock_filesystem
        mock_credentials = MagicMock()
        
        # Insert mock into sys.modules before import
        original_modules = sys.modules.copy()
        sys.modules["dlt.sources.filesystem"] = mock_fs_module
        
        try:
            pipeline = FluentPipeline()
            pipeline.from_s3("s3://bucket", credentials=mock_credentials)
            
            call_kwargs = mock_filesystem.call_args[1]
            assert call_kwargs["credentials"] is mock_credentials
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

    def test_from_s3_empty_bucket_url(self):
        """Test from_s3 with empty bucket_url raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.from_s3("")

    def test_from_s3_invalid_file_glob(self):
        """Test from_s3 with invalid file_glob raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.from_s3("s3://bucket", file_glob="   ")

    def test_from_s3_invalid_files_per_page(self):
        """Test from_s3 with invalid files_per_page raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.from_s3("s3://bucket", files_per_page=0)
        
        with pytest.raises(ValidationError):
            pipeline.from_s3("s3://bucket", files_per_page=-1)
        
        with pytest.raises(ValidationError):
            pipeline.from_s3("s3://bucket", files_per_page="not int")

