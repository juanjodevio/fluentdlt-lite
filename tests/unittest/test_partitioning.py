"""Unit tests for partitioning and split loading functionality."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from fldt import FluentPipeline, ConfigurationError


class TestWithPartitioning:
    """Test with_partitioning method."""

    def test_with_partitioning_basic(self):
        """Test with_partitioning with basic parameters."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at")
        result = pipeline.with_partitioning(strategy="date", num_partitions=5)
        
        assert result is pipeline
        assert pipeline._partition_config is not None
        assert pipeline._partition_config["strategy"] == "date"
        assert pipeline._partition_config["num_partitions"] == 5

    def test_with_partitioning_with_partition_size(self):
        """Test with_partitioning with partition_size."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at")
        pipeline.with_partitioning(strategy="date", partition_size=timedelta(days=7))
        
        assert pipeline._partition_config["partition_size"] == timedelta(days=7)

    def test_with_partitioning_no_incremental(self):
        """Test with_partitioning without incremental raises ConfigurationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline.with_partitioning(strategy="date", num_partitions=5)
        
        assert "incremental" in str(exc_info.value).lower()

    def test_with_partitioning_invalid_strategy(self):
        """Test with_partitioning with invalid strategy raises ConfigurationError."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at")
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline.with_partitioning(strategy="invalid", num_partitions=5)
        
        error_msg = str(exc_info.value).lower()
        assert "date" in error_msg and "range" in error_msg

    def test_with_partitioning_no_num_partitions_or_size(self):
        """Test with_partitioning without num_partitions or partition_size raises ConfigurationError."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at")
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline.with_partitioning(strategy="date")
        
        assert "num_partitions or partition_size" in str(exc_info.value).lower()

    def test_with_partitioning_both_num_partitions_and_size(self):
        """Test with_partitioning with both num_partitions and partition_size raises ConfigurationError."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at")
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline.with_partitioning(
                strategy="date",
                num_partitions=5,
                partition_size=timedelta(days=7)
            )
        
        assert "both num_partitions and partition_size" in str(exc_info.value).lower()

    def test_with_partitioning_with_split_config(self):
        """Test with_partitioning with existing split_config raises ConfigurationError."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at")
        pipeline.with_split_loading()
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline.with_partitioning(strategy="date", num_partitions=5)
        
        assert "cannot be used together" in str(exc_info.value).lower()


class TestWithSplitLoading:
    """Test with_split_loading method."""

    def test_with_split_loading_basic(self):
        """Test with_split_loading with basic parameters."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at")
        result = pipeline.with_split_loading()
        
        assert result is pipeline
        assert pipeline._split_config is not None
        assert pipeline._split_config["chunk_size"] == 1000
        assert pipeline._split_config["pages_per_run"] == 2

    def test_with_split_loading_custom_params(self):
        """Test with_split_loading with custom parameters."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at")
        pipeline.with_split_loading(chunk_size=500, pages_per_run=3, row_order="desc")
        
        assert pipeline._split_config["chunk_size"] == 500
        assert pipeline._split_config["pages_per_run"] == 3
        assert pipeline._incremental_config["row_order"] == "desc"

    def test_with_split_loading_no_incremental(self):
        """Test with_split_loading without incremental raises ConfigurationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline.with_split_loading()
        
        assert "incremental" in str(exc_info.value).lower()

    def test_with_split_loading_invalid_row_order(self):
        """Test with_split_loading with invalid row_order raises ConfigurationError."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at")
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline.with_split_loading(row_order="invalid")
        
        error_msg = str(exc_info.value).lower()
        assert "asc" in error_msg and "desc" in error_msg

    def test_with_split_loading_with_partition_config(self):
        """Test with_split_loading with existing partition_config raises ConfigurationError."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at")
        pipeline.with_partitioning(strategy="date", num_partitions=5)
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline.with_split_loading()
        
        assert "cannot be used together" in str(exc_info.value).lower()

    def test_with_split_loading_sets_row_order(self):
        """Test that with_split_loading sets row_order in incremental config."""
        pipeline = FluentPipeline()
        pipeline.with_incremental("updated_at")
        pipeline.with_split_loading(row_order="desc")
        
        assert pipeline._incremental_config["row_order"] == "desc"
        assert pipeline._incremental_config["range_start"] == "open"


class TestCreatePartitionRanges:
    """Test _create_partition_ranges method."""

    def test_create_partition_ranges_date_num_partitions(self):
        """Test creating date partition ranges with num_partitions."""
        pipeline = FluentPipeline()
        min_val = datetime(2024, 1, 1)
        max_val = datetime(2024, 1, 31)
        
        ranges = pipeline._create_partition_ranges(
            "updated_at",
            min_val,
            max_val,
            num_partitions=3,
            partition_size=None,
            strategy="date"
        )
        
        assert len(ranges) == 3
        assert ranges[0]["start"] == min_val
        assert ranges[-1]["end"] == max_val

    def test_create_partition_ranges_date_partition_size(self):
        """Test creating date partition ranges with partition_size."""
        pipeline = FluentPipeline()
        min_val = datetime(2024, 1, 1)
        max_val = datetime(2024, 1, 15)
        
        ranges = pipeline._create_partition_ranges(
            "updated_at",
            min_val,
            max_val,
            num_partitions=None,
            partition_size=timedelta(days=5),
            strategy="date"
        )
        
        assert len(ranges) >= 1
        assert ranges[0]["start"] == min_val
        assert ranges[-1]["end"] == max_val

    def test_create_partition_ranges_range_num_partitions(self):
        """Test creating range partition ranges with num_partitions."""
        pipeline = FluentPipeline()
        min_val = 0
        max_val = 100
        
        ranges = pipeline._create_partition_ranges(
            "id",
            min_val,
            max_val,
            num_partitions=4,
            partition_size=None,
            strategy="range"
        )
        
        assert len(ranges) == 4
        assert ranges[0]["start"] == min_val
        assert ranges[-1]["end"] == max_val

    def test_create_partition_ranges_invalid_range(self):
        """Test creating partition ranges with invalid range raises ConfigurationError."""
        pipeline = FluentPipeline()
        
        with pytest.raises(ConfigurationError) as exc_info:
            pipeline._create_partition_ranges(
                "id",
                100,
                50,  # min > max
                num_partitions=4,
                partition_size=None,
                strategy="range"
            )
        
        assert "min_value" in str(exc_info.value).lower()

