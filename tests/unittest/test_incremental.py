"""Unit tests for incremental loading functionality."""

import pytest
from unittest.mock import MagicMock, patch

from fldt import FluentPipeline, ConfigurationError


class TestWithIncremental:
    """Test with_incremental method."""

    def test_with_incremental_basic(self):
        """Test with_incremental with basic parameters."""
        pipeline = FluentPipeline()
        result = pipeline.with_incremental("updated_at")
        
        assert result is pipeline
        assert pipeline._incremental_config is not None
        assert pipeline._incremental_config["cursor_path"] == "updated_at"
        assert pipeline._incremental_config["range_start"] == "closed"
        assert pipeline._incremental_config["range_end"] == "closed"

    def test_with_incremental_with_initial_value(self):
        """Test with_incremental with initial value."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at", initial_value="2024-01-01")
        
        assert pipeline._incremental_config["initial_value"] == "2024-01-01"

    def test_with_incremental_with_range_settings(self):
        """Test with_incremental with custom range settings."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at", range_start="open", range_end="open")
        
        assert pipeline._incremental_config["range_start"] == "open"
        assert pipeline._incremental_config["range_end"] == "open"

    def test_with_incremental_with_kwargs(self):
        """Test with_incremental with additional kwargs."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at", last_value_func=lambda x: x)
        
        assert "last_value_func" in pipeline._incremental_config


class TestApplyIncremental:
    """Test _apply_incremental method."""

    def test_apply_incremental_no_config(self):
        """Test applying incremental when not configured."""
        pipeline = FluentPipeline()
        mock_source = MagicMock()
        
        result = pipeline._apply_incremental(mock_source)
        
        assert result is mock_source

    @patch("dlt.sources.incremental")
    def test_apply_incremental_with_resources(self, mock_incremental):
        """Test applying incremental to source with resources."""
        mock_incremental_obj = MagicMock()
        mock_incremental.return_value = mock_incremental_obj
        
        pipeline = FluentPipeline()
        pipeline._incremental_config = {
            "cursor_path": "updated_at",
            "initial_value": None,
            "range_start": "closed",
            "range_end": "closed",
        }
        
        mock_resource = MagicMock()
        mock_source = MagicMock()
        mock_source.resources = {"table1": mock_resource}
        mock_source.with_resources = MagicMock(return_value=mock_source)
        
        result = pipeline._apply_incremental(mock_source)
        
        assert result is mock_source
        mock_incremental.assert_called_once()
        mock_source.with_resources.assert_called_once()

    @patch("dlt.sources.incremental")
    def test_apply_incremental_with_set_incremental(self, mock_incremental):
        """Test applying incremental to source with set_incremental method."""
        mock_incremental_obj = MagicMock()
        mock_incremental.return_value = mock_incremental_obj
        
        pipeline = FluentPipeline()
        pipeline._incremental_config = {
            "cursor_path": "updated_at",
            "initial_value": None,
            "range_start": "closed",
            "range_end": "closed",
        }
        
        mock_source = MagicMock()
        mock_source.set_incremental = MagicMock()
        del mock_source.resources
        
        result = pipeline._apply_incremental(mock_source)
        
        assert result is mock_source
        mock_source.set_incremental.assert_called_once_with(mock_incremental_obj)

    @patch("dlt.sources.incremental")
    def test_apply_incremental_with_incremental_attribute(self, mock_incremental):
        """Test applying incremental to source with incremental attribute."""
        mock_incremental_obj = MagicMock()
        mock_incremental.return_value = mock_incremental_obj
        
        pipeline = FluentPipeline()
        pipeline._incremental_config = {
            "cursor_path": "updated_at",
            "initial_value": None,
            "range_start": "closed",
            "range_end": "closed",
        }
        
        mock_source = MagicMock()
        del mock_source.resources
        del mock_source.set_incremental
        
        result = pipeline._apply_incremental(mock_source)
        
        assert result is mock_source
        assert mock_source.incremental is mock_incremental_obj

    def test_apply_incremental_invalid_cursor_path(self):
        """Test applying incremental with invalid cursor_path raises ConfigurationError."""
        pipeline = FluentPipeline()
        pipeline._incremental_config = {
            "cursor_path": None,
            "initial_value": None,
            "range_start": "closed",
            "range_end": "closed",
        }
        
        mock_source = MagicMock()
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline._apply_incremental(mock_source)
        
        assert "cursor_path" in str(exc_info.value).lower()

    def test_apply_incremental_no_support(self):
        """Test applying incremental to unsupported source raises ConfigurationError."""
        pipeline = FluentPipeline()
        pipeline._incremental_config = {
            "cursor_path": "updated_at",
            "initial_value": None,
            "range_start": "closed",
            "range_end": "closed",
        }
        
        mock_source = MagicMock()
        del mock_source.resources
        del mock_source.set_incremental
        del mock_source.incremental
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline._apply_incremental(mock_source)
        
        assert "incremental loading" in str(exc_info.value).lower()

