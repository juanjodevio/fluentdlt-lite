"""Unit tests for transformer chain functionality."""

import pytest
from unittest.mock import MagicMock, patch

from fldt import FluentPipeline, ConfigurationError


class TestApplyTransformers:
    """Test _apply_transformers method."""

    def test_apply_transformers_no_transformers(self):
        """Test applying transformers when none are configured."""
        pipeline = FluentPipeline()
        mock_source = MagicMock()
        
        result = pipeline._apply_transformers(mock_source)
        
        assert result is mock_source

    def test_apply_transformers_with_resources(self):
        """Test applying transformers to source with resources."""
        pipeline = FluentPipeline()
        transformer = lambda x: x
        
        mock_resource = MagicMock()
        mock_resource.add_map = MagicMock()
        
        mock_source = MagicMock()
        mock_source.resources = {"table1": mock_resource}
        
        pipeline._transformers = [transformer]
        
        result = pipeline._apply_transformers(mock_source)
        
        assert result is mock_source
        mock_resource.add_map.assert_called_once_with(transformer)

    def test_apply_transformers_multiple_resources(self):
        """Test applying transformers to multiple resources."""
        pipeline = FluentPipeline()
        transformer = lambda x: x
        
        mock_resource1 = MagicMock()
        mock_resource1.add_map = MagicMock()
        mock_resource2 = MagicMock()
        mock_resource2.add_map = MagicMock()
        
        mock_source = MagicMock()
        mock_source.resources = {
            "table1": mock_resource1,
            "table2": mock_resource2,
        }
        
        pipeline._transformers = [transformer]
        
        pipeline._apply_transformers(mock_source)
        
        mock_resource1.add_map.assert_called_once_with(transformer)
        mock_resource2.add_map.assert_called_once_with(transformer)

    def test_apply_transformers_multiple_transformers(self):
        """Test applying multiple transformers."""
        pipeline = FluentPipeline()
        transformer1 = lambda x: x
        transformer2 = lambda x: x * 2
        
        mock_resource = MagicMock()
        mock_resource.add_map = MagicMock()
        
        mock_source = MagicMock()
        mock_source.resources = {"table1": mock_resource}
        
        pipeline._transformers = [transformer1, transformer2]
        
        pipeline._apply_transformers(mock_source)
        
        assert mock_resource.add_map.call_count == 2
        mock_resource.add_map.assert_any_call(transformer1)
        mock_resource.add_map.assert_any_call(transformer2)

    def test_apply_transformers_no_resources_attribute(self):
        """Test applying transformers to source without resources raises ConfigurationError."""
        pipeline = FluentPipeline()
        transformer = lambda x: x
        
        mock_source = MagicMock()
        del mock_source.resources
        
        pipeline._transformers = [transformer]
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline._apply_transformers(mock_source)
        
        assert "resources" in str(exc_info.value).lower()

    def test_apply_transformers_resource_no_add_map(self):
        """Test applying transformers to resource without add_map raises ConfigurationError."""
        pipeline = FluentPipeline()
        transformer = lambda x: x
        
        mock_resource = MagicMock()
        del mock_resource.add_map
        
        mock_source = MagicMock()
        mock_source.resources = {"table1": mock_resource}
        
        pipeline._transformers = [transformer]
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline._apply_transformers(mock_source)
        
        assert "add_map" in str(exc_info.value).lower()

