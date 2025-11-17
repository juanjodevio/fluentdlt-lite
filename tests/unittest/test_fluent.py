"""Unit tests for core FluentPipeline functionality."""

import pytest
from unittest.mock import MagicMock, patch

from fldt import FluentPipeline, ValidationError, ConfigurationError


class TestFluentPipelineInitialization:
    """Test FluentPipeline initialization."""

    def test_init(self):
        """Test that pipeline initializes with empty state."""
        pipeline = FluentPipeline()
        
        assert pipeline._source is None
        assert pipeline._destination is None
        assert pipeline._transformers == []
        assert pipeline._incremental_config is None
        assert pipeline._partition_config is None
        assert pipeline._split_config is None
        assert pipeline._pipeline_name is None
        assert pipeline._dataset_name is None
        assert pipeline._options == {}


class TestAddTransformer:
    """Test add_transformer method."""

    def test_add_transformer(self):
        """Test adding a valid transformer."""
        pipeline = FluentPipeline()
        transformer = lambda x: x
        
        result = pipeline.add_transformer(transformer)
        
        assert result is pipeline  # Method chaining
        assert len(pipeline._transformers) == 1
        assert pipeline._transformers[0] is transformer

    def test_add_multiple_transformers(self):
        """Test adding multiple transformers."""
        pipeline = FluentPipeline()
        transformer1 = lambda x: x
        transformer2 = lambda x: x * 2
        
        pipeline.add_transformer(transformer1)
        pipeline.add_transformer(transformer2)
        
        assert len(pipeline._transformers) == 2
        assert pipeline._transformers[0] is transformer1
        assert pipeline._transformers[1] is transformer2

    def test_add_transformer_invalid(self):
        """Test adding invalid transformer raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError) as exc_info:
            pipeline.add_transformer("not callable")
        
        assert "callable" in str(exc_info.value).lower()

    def test_add_transformer_none(self):
        """Test adding None as transformer raises ValidationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ValidationError):
            pipeline.add_transformer(None)


class TestRun:
    """Test run method."""

    def test_run_no_source(self):
        """Test run without source raises ConfigurationError."""
        pipeline = FluentPipeline()
        pipeline.to("duckdb")
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline.run()
        
        assert "source" in str(exc_info.value).lower()

    def test_run_no_destination(self):
        """Test run without destination raises ConfigurationError."""
        import sys
        from unittest.mock import Mock
        
        # Mock the sql_database module in sys.modules before import
        mock_sql_db_module = Mock()
        mock_source = MagicMock()
        mock_source.resources = {}
        mock_sql_table = MagicMock(return_value=mock_source)
        mock_sql_db_module.sql_table = mock_sql_table
        
        # Insert mock into sys.modules before import
        original_modules = sys.modules.copy()
        sys.modules["dlt.sources.sql_database"] = mock_sql_db_module
        
        try:
            pipeline = FluentPipeline()
            pipeline.from_sql_table("postgresql://test", "users")
            
            with pytest.raises(ConfigurationError) as exc_info:
                pipeline.run()
            
            assert "destination" in str(exc_info.value).lower()
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

    def test_run_invalid_transformer(self):
        """Test run with invalid transformer raises ConfigurationError."""
        pipeline = FluentPipeline()
        # Set source directly to avoid triggering dlt import issues
        pipeline._source = MagicMock()
        pipeline.to("duckdb")
        pipeline._transformers = ["not callable"]
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline.run()
        
        assert "callable" in str(exc_info.value).lower()

    @patch("dlt.pipeline")
    def test_run_basic(self, mock_dlt_pipeline):
        """Test basic run execution."""
        import sys
        from unittest.mock import Mock
        
        # Mock the sql_database module in sys.modules before import
        mock_sql_db_module = Mock()
        mock_source = MagicMock()
        mock_source.resources = {}
        mock_sql_table = MagicMock(return_value=mock_source)
        mock_sql_db_module.sql_table = mock_sql_table
        
        mock_pipeline = MagicMock()
        mock_result = MagicMock()
        mock_pipeline.run.return_value = mock_result
        mock_dlt_pipeline.return_value = mock_pipeline
        
        # Insert mock into sys.modules before import
        original_modules = sys.modules.copy()
        sys.modules["dlt.sources.sql_database"] = mock_sql_db_module
        
        try:
            pipeline = FluentPipeline()
            pipeline.from_sql_table("postgresql://test", "users")
            pipeline.to("duckdb")
            
            result = pipeline.run()
            
            assert result is mock_result
            mock_dlt_pipeline.assert_called_once()
            mock_pipeline.run.assert_called_once()
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

    @patch("dlt.pipeline")
    def test_run_with_custom_pipeline_name(self, mock_dlt_pipeline):
        """Test run with custom pipeline name."""
        import sys
        from unittest.mock import Mock
        
        # Mock the sql_database module in sys.modules before import
        mock_sql_db_module = Mock()
        mock_source = MagicMock()
        mock_source.resources = {}
        mock_sql_table = MagicMock(return_value=mock_source)
        mock_sql_db_module.sql_table = mock_sql_table
        
        mock_pipeline = MagicMock()
        mock_result = MagicMock()
        mock_pipeline.run.return_value = mock_result
        mock_dlt_pipeline.return_value = mock_pipeline
        
        # Insert mock into sys.modules before import
        original_modules = sys.modules.copy()
        sys.modules["dlt.sources.sql_database"] = mock_sql_db_module
        
        try:
            pipeline = FluentPipeline()
            pipeline._pipeline_name = "custom_pipeline"
            pipeline._dataset_name = "custom_dataset"
            pipeline.from_sql_table("postgresql://test", "users")
            pipeline.to("duckdb")
            
            pipeline.run()
            
            mock_dlt_pipeline.assert_called_once_with(
                pipeline_name="custom_pipeline",
                destination="duckdb",
                dataset_name="custom_dataset"
            )
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

