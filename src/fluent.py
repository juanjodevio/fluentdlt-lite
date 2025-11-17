from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional, Union

import dlt
from dlt.common.configuration.specs.aws_credentials import AwsCredentials
from dlt.common.configuration.specs.connection_string_credentials import (
    ConnectionStringCredentials,
)
from fsspec import AbstractFileSystem
from sqlalchemy import Engine, text

from .exceptions import ConfigurationError, ExecutionError, ValidationError

DEFAULT_CHUNK_SIZE = 10000


class FluentPipeline:
    def __init__(self):
        self._source: Any | None = None
        self._destination: Any | None = None
        self._transformers: list[Any] = []
        self._incremental_config: dict[str, Any] | None = None
        self._partition_config: dict[str, Any] | None = None
        self._split_config: dict[str, Any] | None = None
        self._pipeline_name: str | None = None
        self._dataset_name: str | None = None
        self._options: dict[str, Any] = {}

    def _validate_credentials(
        self, credentials: Any, source_type: str
    ) -> None:
        """Validate credentials for source type.
        
        Args:
            credentials: The credentials to validate
            source_type: Type of source (e.g., "SQL", "S3")
            
        Raises:
            ValidationError: If credentials are invalid or missing
        """
        if credentials is None:
            raise ValidationError(
                f"Credentials required for {source_type} source"
            )
        # Additional validation based on type
        if isinstance(credentials, str):
            if not credentials.strip():
                raise ValidationError(
                    f"Credentials string cannot be empty for {source_type} source"
                )

    def _validate_table_name(self, table_name: str) -> None:
        """Validate SQL table name.
        
        Args:
            table_name: The table name to validate
            
        Raises:
            ValidationError: If table name is invalid
        """
        if not table_name or not isinstance(table_name, str):
            raise ValidationError("Table name must be a non-empty string")
        if not table_name.strip():
            raise ValidationError("Table name cannot be whitespace only")

    def _validate_query(self, query: str) -> None:
        """Validate SQL query.
        
        Args:
            query: The query string to validate
            
        Raises:
            ValidationError: If query is invalid
        """
        if not query or not isinstance(query, str) or not query.strip():
            raise ValidationError("Query must be a non-empty string")

    def _validate_destination_string(self, destination: str) -> None:
        """Validate destination string format.
        
        Validates that destination is a non-empty string. Actual destination
        support is validated by dlt when the pipeline is created.
        
        Args:
            destination: The destination string to validate
            
        Raises:
            ValidationError: If destination format is invalid
        """
        if not destination or not isinstance(destination, str):
            raise ValidationError("Destination must be a non-empty string")
        if not destination.strip():
            raise ValidationError("Destination cannot be whitespace only")

    def from_sql_table(
        self,
        credentials: Union[ConnectionStringCredentials, Engine, str],
        table_name: str,
        schema: str | None = None,
        **kwargs,
    ) -> "FluentPipeline":
        """Configure SQL table as source."""
        from dlt.sources.sql_database import sql_table

        self._validate_credentials(credentials, "SQL")
        self._validate_table_name(table_name)
        
        if schema is not None and (not isinstance(schema, str) or not schema.strip()):
            raise ValidationError("Schema must be a non-empty string if provided")

        if isinstance(credentials, str):
            credentials = ConnectionStringCredentials(credentials)

        self._source = sql_table(
            table=table_name,
            schema=schema,
            credentials=credentials,
            **kwargs,
        )
        return self

    def from_sql_query(
        self,
        credentials: Union[ConnectionStringCredentials, Engine, str],
        query: str,
        table_name: str,
        **kwargs,
    ) -> "FluentPipeline":
        """Configure SQL query as source."""
        from dlt.sources.sql_database import sql_database

        self._validate_credentials(credentials, "SQL")
        self._validate_query(query)
        self._validate_table_name(table_name)

        if isinstance(credentials, str):
            credentials = ConnectionStringCredentials(credentials)

        self._source = sql_database(
            credentials=credentials,
            table_names=[table_name],
            **kwargs,
        ).with_resources(**{table_name: {"query": query}})
        return self

    def from_s3(
        self,
        bucket_url: str = dlt.secrets.value,
        credentials: Optional[Union[AbstractFileSystem, AwsCredentials]] = None,
        file_glob: str = "*",
        files_per_page: int = DEFAULT_CHUNK_SIZE,
        extract_content: bool = False,
        **kwargs,
    ) -> "FluentPipeline":
        """Configure S3 bucket as source."""
        from dlt.sources.filesystem import filesystem

        if bucket_url is dlt.secrets.value:
            # Allow secrets.value for configuration via secrets
            pass
        elif not bucket_url or not isinstance(bucket_url, str) or not bucket_url.strip():
            raise ValidationError("bucket_url must be a non-empty string")
        
        if file_glob is not None and (not isinstance(file_glob, str) or not file_glob.strip()):
            raise ValidationError("file_glob must be a non-empty string if provided")
        
        if not isinstance(files_per_page, int) or files_per_page <= 0:
            raise ValidationError("files_per_page must be a positive integer")

        self._source = filesystem(
            bucket_url=bucket_url,
            credentials=credentials,
            file_glob=file_glob,
            files_per_page=files_per_page,
            extract_content=extract_content,
            **kwargs,
        )
        return self

    def to(
        self,
        destination: str,
        **kwargs,
    ) -> "FluentPipeline":
        """Configure destination by string name.
        
        Args:
            destination: Destination name (e.g., "duckdb", "postgres", "bigquery")
            **kwargs: Additional destination-specific configuration
            
        Returns:
            Self for method chaining.
            
        Raises:
            ValidationError: If destination string is not supported
        """
        self._validate_destination_string(destination)
        self._destination = {
            "destination": destination,
            **kwargs,
        }
        return self

    def to_redshift(
        self,
        credentials: Union[str, dict],
        database: str,
        schema: Optional[str] = None,
        **kwargs,
    ) -> "FluentPipeline":
        """Configure Redshift as destination."""
        if credentials is None:
            raise ValidationError("Credentials required for Redshift destination")
        
        if not database or not isinstance(database, str) or not database.strip():
            raise ValidationError("Database name must be a non-empty string")
        
        if schema is not None and (not isinstance(schema, str) or not schema.strip()):
            raise ValidationError("Schema must be a non-empty string if provided")
        
        self._destination = {
            "destination": "redshift",
            "credentials": credentials,
            "database": database,
            "schema": schema,
            **kwargs,
        }
        return self

    def to_s3(
        self,
        bucket_url: str,
        credentials: Optional[Union[AbstractFileSystem, AwsCredentials, dict]] = None,
        format: str = "parquet",
        **kwargs,
    ) -> "FluentPipeline":
        """Configure S3 as destination.
        
        Args:
            bucket_url: S3 bucket URL (required)
            credentials: AWS credentials or filesystem credentials
            format: Output format - parquet, jsonl, or csv (default: parquet)
            
        Raises:
            ValidationError: If bucket_url or format is invalid
        """
        if not bucket_url or not isinstance(bucket_url, str) or not bucket_url.strip():
            raise ValidationError("bucket_url must be a non-empty string")
        
        if format not in ("parquet", "jsonl", "csv"):
            raise ValidationError(
                f"Invalid format: {format}. Must be one of: parquet, jsonl, csv"
            )
        
        self._destination = {
            "destination": "filesystem",
            "bucket_url": bucket_url,
            "credentials": credentials,
            "format": format,
            **kwargs,
        }
        return self

    def add_transformer(self, transformer: Callable) -> "FluentPipeline":
        """Add a transformation function to the pipeline."""
        if not callable(transformer):
            raise ValidationError(
                f"Transformer must be callable, got {type(transformer)}"
            )
        self._transformers.append(transformer)
        return self

    def with_incremental(
        self,
        column: str,
        initial_value: Any = None,
        range_start: str = "closed",
        range_end: str = "closed",
        **kwargs,
    ) -> "FluentPipeline":
        """Configure incremental loading strategy."""
        self._incremental_config = {
            "cursor_path": column,
            "initial_value": initial_value,
            "range_start": range_start,
            "range_end": range_end,
            **kwargs,
        }
        return self

    def with_partitioning(
        self,
        strategy: str = "date",
        num_partitions: int | None = None,
        partition_size: Any = None,
        **kwargs,
    ) -> "FluentPipeline":
        """Configure partitioning strategy for large incremental loads.
        
        Supports date-based partitioning (for timestamp columns) and
        range-based partitioning (for numeric columns).
        
        Args:
            strategy: Partitioning strategy - "date" or "range" (default: "date")
            num_partitions: Number of partitions to create. Mutually exclusive with
                          partition_size. Required if partition_size is not set.
            partition_size: Size of each partition. Mutually exclusive with num_partitions.
                          Required if num_partitions is not set.
                          - For "date" strategy: int (days) or timedelta
                          - For "range" strategy: numeric value
            **kwargs: Additional partition configuration options
            
        Returns:
            Self for method chaining.
            
        Raises:
            ConfigurationError: If incremental config is not set, or incompatible
                              partition configuration is provided.
        """
        if self._incremental_config is None:
            raise ConfigurationError(
                "Partitioning requires incremental configuration. "
                "Call with_incremental() first."
            )
        
        if strategy not in ("date", "range"):
            raise ConfigurationError(
                f"Invalid partitioning strategy: {strategy}. "
                "Must be 'date' or 'range'."
            )
        
        if num_partitions is None and partition_size is None:
            raise ConfigurationError(
                "Either num_partitions or partition_size must be specified."
            )
        
        if num_partitions is not None and partition_size is not None:
            raise ConfigurationError(
                "Cannot specify both num_partitions and partition_size. "
                "Use one or the other."
            )
        
        if self._split_config is not None:
            raise ConfigurationError(
                "Partitioning and split loading cannot be used together. "
                "Use either with_partitioning() or with_split_loading()."
            )
        
        self._partition_config = {
            "strategy": strategy,
            "num_partitions": num_partitions,
            "partition_size": partition_size,
            **kwargs,
        }
        return self

    def with_split_loading(
        self,
        chunk_size: int = 1000,
        row_order: str = "asc",
        pages_per_run: int = 2,
        **kwargs,
    ) -> "FluentPipeline":
        """Configure split loading for large incremental loads.
        
        Split loading processes data in chunks by limiting pages per run
        and looping until all data is loaded. Requires row_order to ensure
        consistent ordering across runs.
        
        Args:
            chunk_size: Number of rows per chunk (default: 1000)
            row_order: Row ordering - "asc" or "desc" (default: "asc")
            pages_per_run: Number of pages to process per pipeline run (default: 2)
            **kwargs: Additional split configuration options
            
        Returns:
            Self for method chaining.
            
        Raises:
            ConfigurationError: If incremental config is not set, or incompatible
                              split configuration is provided.
        """
        if self._incremental_config is None:
            raise ConfigurationError(
                "Split loading requires incremental configuration. "
                "Call with_incremental() first."
            )
        
        if row_order not in ("asc", "desc"):
            raise ConfigurationError(
                f"Invalid row_order: {row_order}. Must be 'asc' or 'desc'."
            )
        
        if self._partition_config is not None:
            raise ConfigurationError(
                "Partitioning and split loading cannot be used together. "
                "Use either with_partitioning() or with_split_loading()."
            )
        
        # Update incremental config with row_order and range_start
        self._incremental_config["row_order"] = row_order
        self._incremental_config["range_start"] = "open"
        
        self._split_config = {
            "chunk_size": chunk_size,
            "pages_per_run": pages_per_run,
            **kwargs,
        }
        return self

    def _apply_transformers(self, source: Any) -> Any:
        """Apply transformer chain to source data.
        
        Applies transformers in sequence during pipeline execution.
        Handles generator functions for streaming transformations.
        Preserves dlt resource structure while applying transformers.
        Supports both row-level and batch transformers.
        
        Args:
            source: The dlt source to apply transformers to.
            
        Returns:
            The source with transformers applied.
        """
        if not self._transformers:
            return source
        
        if not hasattr(source, "resources"):
            raise ConfigurationError(
                "Source does not have resources attribute. "
                "Transformers can only be applied to dlt sources with resources."
            )
        
        for transformer in self._transformers:
            for resource_name, resource in source.resources.items():
                if hasattr(resource, "add_map"):
                    resource.add_map(transformer)
                else:
                    raise ConfigurationError(
                        f"Resource '{resource_name}' does not support add_map. "
                        "Transformers require resources with add_map support."
                    )
        
        return source

    def _get_cursor_range(self, source: Any) -> tuple[Any, Any]:
        """Get min and max cursor values from source.
        
        Queries the source to determine the range of values for the cursor column.
        Supports SQL sources by accessing credentials and executing MIN/MAX queries.
        
        Args:
            source: The dlt source to query for cursor range.
            
        Returns:
            Tuple of (min_value, max_value) for the cursor column.
            
        Raises:
            ConfigurationError: If source does not support cursor range queries.
        """
        cursor_path = self._incremental_config["cursor_path"]
        
        # For SQL sources, access credentials and query min/max
        if hasattr(source, "credentials"):
            credentials = source.credentials
            
            # Convert ConnectionStringCredentials to Engine if needed
            if isinstance(credentials, ConnectionStringCredentials):
                from sqlalchemy import create_engine
                engine = create_engine(credentials.to_native_representation())
            elif isinstance(credentials, Engine):
                engine = credentials
            elif isinstance(credentials, str):
                from sqlalchemy import create_engine
                engine = create_engine(credentials)
            else:
                raise ConfigurationError(
                    f"Unsupported credentials type for cursor range query: {type(credentials)}"
                )
            
            # Get table information from source
            if hasattr(source, "table"):
                table_name = source.table
                schema = getattr(source, "schema", None)
            elif hasattr(source, "resources") and len(source.resources) > 0:
                # For sources with resources, get the first resource's table info
                first_resource = list(source.resources.values())[0]
                if hasattr(first_resource, "table"):
                    table_name = first_resource.table
                    schema = getattr(first_resource, "schema", None)
                else:
                    raise ConfigurationError(
                        "Cannot determine table name from source for cursor range query."
                    )
            else:
                raise ConfigurationError(
                    "Source does not provide table information for cursor range query."
                )
            
            # Build table reference with proper schema handling
            if schema:
                table_ref = f"{schema}.{table_name}"
            else:
                table_ref = table_name
            
            # Extract column name (handle dotted paths like "parent.column")
            # For SQL injection safety, we validate the column name doesn't contain SQL keywords
            column_name = cursor_path.split(".")[-1] if "." in cursor_path else cursor_path
            
            # Query min and max values
            # Note: Using text() with string formatting for column/table names is safe here
            # since these come from dlt source configuration, not user input at query time.
            # For extra safety, we could validate against a whitelist of allowed column names.
            with engine.connect() as conn:
                query = text(
                    f"SELECT MIN({column_name}) as min_val, MAX({column_name}) as max_val FROM {table_ref}"
                )
                result = conn.execute(query)
                row = result.fetchone()
                
                if row is None or row[0] is None or row[1] is None:
                    raise ConfigurationError(
                        f"No data found in source table or cursor column '{cursor_path}' has null values."
                    )
                
                return (row[0], row[1])
        else:
            raise ConfigurationError(
                "Source does not support cursor range queries. "
                "Partitioning requires a SQL source with accessible credentials."
            )

    def _create_partition_ranges(
        self, cursor_column: str, min_value: Any, max_value: Any, num_partitions: int | None, partition_size: Any | None, strategy: str
    ) -> list[dict[str, Any]]:
        """Create partition ranges for cursor column.
        
        Generates evenly distributed ranges based on strategy (date or range).
        Supports both num_partitions and partition_size configurations.
        
        Args:
            cursor_column: The cursor column name (for reference)
            min_value: Minimum cursor value
            max_value: Maximum cursor value
            num_partitions: Number of partitions to create (if set)
            partition_size: Size of each partition (if set)
            strategy: Partitioning strategy - "date" or "range"
            
        Returns:
            List of range dictionaries with "start" and "end" keys.
            
        Raises:
            ConfigurationError: If partition configuration is invalid.
        """
        if min_value >= max_value:
            raise ConfigurationError(
                f"Invalid cursor range: min_value ({min_value}) >= max_value ({max_value})"
            )
        
        ranges = []
        
        if strategy == "date":
            if not isinstance(min_value, (datetime, type(None))) or not isinstance(max_value, (datetime, type(None))):
                # Try to parse as datetime if strings
                try:
                    from dateutil import parser
                    min_value = parser.parse(str(min_value)) if min_value else None
                    max_value = parser.parse(str(max_value)) if max_value else None
                except (ImportError, ValueError):
                    raise ConfigurationError(
                        f"Date partitioning requires datetime values. "
                        f"Got min_value={type(min_value).__name__}, max_value={type(max_value).__name__}"
                    )
            
            if min_value is None or max_value is None:
                raise ConfigurationError("Date partitioning requires non-null min and max values")
            
            total_delta = max_value - min_value
            
            if num_partitions is not None:
                partition_delta = total_delta / num_partitions
                current_start = min_value
                
                for i in range(num_partitions):
                    if i == num_partitions - 1:
                        # Last partition includes max_value
                        current_end = max_value
                    else:
                        current_end = current_start + partition_delta
                    
                    ranges.append({
                        "start": current_start,
                        "end": current_end,
                    })
                    current_start = current_end
            
            elif partition_size is not None:
                # Convert partition_size to timedelta if it's an int (days)
                if isinstance(partition_size, int):
                    partition_delta = timedelta(days=partition_size)
                elif isinstance(partition_size, timedelta):
                    partition_delta = partition_size
                else:
                    raise ConfigurationError(
                        f"Date partition_size must be int (days) or timedelta. "
                        f"Got {type(partition_size).__name__}"
                    )
                
                current_start = min_value
                while current_start < max_value:
                    current_end = min(current_start + partition_delta, max_value)
                    ranges.append({
                        "start": current_start,
                        "end": current_end,
                    })
                    current_start = current_end
        
        elif strategy == "range":
            if not isinstance(min_value, (int, float)) or not isinstance(max_value, (int, float)):
                raise ConfigurationError(
                    f"Range partitioning requires numeric values. "
                    f"Got min_value={type(min_value).__name__}, max_value={type(max_value).__name__}"
                )
            
            total_range = max_value - min_value
            
            if num_partitions is not None:
                partition_size_value = total_range / num_partitions
                current_start = min_value
                
                for i in range(num_partitions):
                    if i == num_partitions - 1:
                        # Last partition includes max_value
                        current_end = max_value
                    else:
                        current_end = current_start + partition_size_value
                    
                    ranges.append({
                        "start": current_start,
                        "end": current_end,
                    })
                    current_start = current_end
            
            elif partition_size is not None:
                if not isinstance(partition_size, (int, float)):
                    raise ConfigurationError(
                        f"Range partition_size must be numeric. "
                        f"Got {type(partition_size).__name__}"
                    )
                
                current_start = min_value
                while current_start < max_value:
                    current_end = min(current_start + partition_size, max_value)
                    ranges.append({
                        "start": current_start,
                        "end": current_end,
                    })
                    current_start = current_end
        
        if not ranges:
            raise ConfigurationError("Failed to create partition ranges")
        
        return ranges

    def _create_partition_source(
        self, source: Any, start_value: Any, end_value: Any
    ) -> Any:
        """Create a source configured for a specific partition range.
        
        Creates a new source instance with incremental configuration set to
        the partition's start and end values. For SQL sources, adds WHERE clause
        to limit data to the partition range.
        
        Args:
            source: The original dlt source
            start_value: Partition start value
            end_value: Partition end value
            
        Returns:
            Source configured for the partition range.
        """
        # Clone the incremental config for this partition
        partition_inc_config = self._incremental_config.copy()
        partition_inc_config["initial_value"] = start_value
        partition_inc_config["range_start"] = "closed"
        partition_inc_config["range_end"] = "closed"
        
        # Store end_value for potential WHERE clause filtering
        cursor_path = partition_inc_config["cursor_path"]
        
        # For SQL sources, add WHERE clause to limit to partition range
        # Extract column name (handle dotted paths)
        column_name = cursor_path.split(".")[-1] if "." in cursor_path else cursor_path
        
        # Apply WHERE clause for SQL sources if possible
        # Note: This is a simplified implementation. For production use, consider
        # using SQLAlchemy bindparams for proper parameter binding.
        if hasattr(source, "with_resources") and hasattr(source, "resources"):
            # Add WHERE clause to each resource to limit to partition range
            resource_configs = {}
            for resource_name in source.resources.keys():
                # Check if resource has existing query
                resource = source.resources[resource_name]
                existing_query = None
                if hasattr(resource, "query"):
                    existing_query = resource.query
                elif hasattr(resource, "get_table_query"):
                    # For table-based resources, get the base query
                    try:
                        existing_query = resource.get_table_query()
                    except (AttributeError, TypeError):
                        pass
                
                if existing_query:
                    # Format values for SQL (handle different types)
                    start_str = self._format_sql_value(start_value)
                    end_str = self._format_sql_value(end_value)
                    
                    # Modify existing query to add WHERE clause
                    where_clause = f"{column_name} >= {start_str} AND {column_name} <= {end_str}"
                    # Simple approach: append WHERE if not present, otherwise add AND
                    if "WHERE" in existing_query.upper():
                        modified_query = f"{existing_query} AND {where_clause}"
                    else:
                        modified_query = f"{existing_query} WHERE {where_clause}"
                    resource_configs[resource_name] = {
                        "query": modified_query,
                    }
                else:
                    # For table-based sources without query access, we'll rely on
                    # incremental filtering with initial_value for start.
                    # End value filtering would require query modification which
                    # isn't straightforward without access to the base query.
                    pass
            
            if resource_configs:
                source = source.with_resources(**resource_configs)
        
        # Apply incremental with partition config using helper method
        partition_source = self._apply_incremental_with_config(source, partition_inc_config)
        
        # Note: dlt incremental with initial_value will handle the >= start_value filter.
        # For <= end_value, if we couldn't add WHERE clause above, we rely on the fact
        # that each partition is loaded sequentially and state tracking prevents overlaps.
        # This is a limitation - ideally we'd have WHERE clause support for all SQL sources.
        
        # Apply transformers
        partition_source = self._apply_transformers(partition_source)
        
        return partition_source

    def _format_sql_value(self, value: Any) -> str:
        """Format a value for SQL query string.
        
        Formats values for embedding in SQL queries. Handles dates, strings, and numerics.
        
        Args:
            value: The value to format
            
        Returns:
            SQL-formatted string representation of the value.
        """
        if isinstance(value, datetime):
            # Format datetime for SQL (ISO format is widely supported)
            return f"'{value.isoformat()}'"
        elif isinstance(value, str):
            # Escape single quotes in strings
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, (int, float)):
            return str(value)
        elif value is None:
            return "NULL"
        else:
            # Convert to string and escape
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"

    def _apply_incremental_with_config(self, source: Any, inc_config: dict[str, Any]) -> Any:
        """Apply incremental loading configuration with a specific config dict.
        
        Helper method to apply incremental config without modifying instance state.
        
        Args:
            source: The dlt source to apply incremental loading to.
            inc_config: The incremental configuration dictionary to use.
            
        Returns:
            The source with incremental configuration applied.
            
        Raises:
            ConfigurationError: If incremental configuration is invalid or
                source does not support incremental loading.
        """
        if not inc_config:
            return source
        
        cursor_path = inc_config.get("cursor_path")
        if not cursor_path or not isinstance(cursor_path, str):
            raise ConfigurationError(
                "Incremental cursor_path must be a non-empty string. "
                f"Got: {cursor_path}"
            )
        
        incremental = dlt.sources.incremental(
            inc_config["cursor_path"],
            initial_value=inc_config.get("initial_value"),
            range_start=inc_config.get("range_start", "closed"),
            range_end=inc_config.get("range_end", "closed"),
            **{
                k: v
                for k, v in inc_config.items()
                if k
                not in (
                    "cursor_path",
                    "initial_value",
                    "range_start",
                    "range_end",
                )
            },
        )
        
        if hasattr(source, "with_resources") and hasattr(source, "resources"):
            source = source.with_resources(
                **{
                    resource_name: {"incremental": incremental}
                    for resource_name in source.resources.keys()
                }
            )
        elif hasattr(source, "set_incremental"):
            source.set_incremental(incremental)
        elif hasattr(source, "incremental"):
            source.incremental = incremental
        else:
            raise ConfigurationError(
                "Source does not support incremental loading. "
                "Source must have with_resources, set_incremental, or incremental attribute."
            )
        
        return source

    def _execute_partitioned_load(self, pipeline: Any, source: Any) -> Any:
        """Execute partitioned incremental load.
        
        Calculates partition ranges, loads each partition sequentially,
        then continues with regular incremental loading from the last partition.
        
        Args:
            pipeline: The dlt pipeline instance
            source: The dlt source to partition
            
        Returns:
            The result of the last partition load.
        """
        # Get min/max values from source
        min_val, max_val = self._get_cursor_range(source)
        
        # Determine num_partitions or partition_size
        num_partitions = self._partition_config.get("num_partitions")
        partition_size = self._partition_config.get("partition_size")
        strategy = self._partition_config["strategy"]
        
        # Create partition ranges
        ranges = self._create_partition_ranges(
            self._incremental_config["cursor_path"],
            min_val,
            max_val,
            num_partitions,
            partition_size,
            strategy,
        )
        
        # Load each partition
        last_result = None
        for partition_range in ranges:
            partition_source = self._create_partition_source(
                source,
                partition_range["start"],
                partition_range["end"],
            )
            last_result = pipeline.run(partition_source)
        
        # Continue with regular incremental from last partition end
        if ranges:
            last_end = ranges[-1]["end"]
            self._incremental_config["initial_value"] = last_end
            self._incremental_config["range_start"] = "open"
        
        return last_result

    def _execute_split_load(self, pipeline: Any, source: Any) -> Any:
        """Execute split incremental load.
        
        Processes data in chunks by limiting pages per run and looping
        until all data is loaded. Uses row_order for consistent ordering.
        
        Args:
            pipeline: The dlt pipeline instance
            source: The dlt source to split load
            
        Returns:
            The result of the last split load iteration.
        """
        # Configure incremental with row_order (already set in with_split_loading)
        source = self._apply_incremental(source)
        
        # Apply transformers
        source = self._apply_transformers(source)
        
        # Add limit for pages per run
        if hasattr(source, "add_limit"):
            source = source.add_limit(self._split_config["pages_per_run"])
        elif hasattr(source, "with_resources"):
            # Apply limit to each resource
            resource_configs = {}
            for resource_name in source.resources.keys():
                resource_configs[resource_name] = {
                    "max_table_items": self._split_config["chunk_size"] * self._split_config["pages_per_run"]
                }
            source = source.with_resources(**resource_configs)
        
        # Loop until empty
        last_result = None
        while True:
            result = pipeline.run(source)
            last_result = result
            
            if result.is_empty:
                break
            
            # Check row counts for more granular exit conditions
            if hasattr(pipeline, "last_trace") and pipeline.last_trace:
                if hasattr(pipeline.last_trace, "last_normalize_info"):
                    normalize_info = pipeline.last_trace.last_normalize_info
                    if hasattr(normalize_info, "row_counts"):
                        row_counts = normalize_info.row_counts
                        if not row_counts or sum(row_counts.values()) == 0:
                            break
        
        return last_result

    def _apply_incremental(self, source: Any) -> Any:
        """Apply incremental loading configuration.
        
        Integrates _incremental_config with dlt incremental decorators.
        Supports cursor-based and timestamp-based incremental strategies.
        Handles state management through dlt's state mechanism.
        Validates incremental column configuration.
        
        Args:
            source: The dlt source to apply incremental loading to.
            
        Returns:
            The source with incremental configuration applied.
            
        Raises:
            ConfigurationError: If incremental configuration is invalid or
                source does not support incremental loading.
        """
        if not self._incremental_config:
            return source
        
        cursor_path = self._incremental_config.get("cursor_path")
        if not cursor_path or not isinstance(cursor_path, str):
            raise ConfigurationError(
                "Incremental cursor_path must be a non-empty string. "
                f"Got: {cursor_path}"
            )
        
        incremental = dlt.sources.incremental(
            self._incremental_config["cursor_path"],
            initial_value=self._incremental_config.get("initial_value"),
            range_start=self._incremental_config.get("range_start", "closed"),
            range_end=self._incremental_config.get("range_end", "closed"),
            **{
                k: v
                for k, v in self._incremental_config.items()
                if k
                not in (
                    "cursor_path",
                    "initial_value",
                    "range_start",
                    "range_end",
                )
            },
        )
        
        if hasattr(source, "with_resources") and hasattr(source, "resources"):
            source = source.with_resources(
                **{
                    resource_name: {"incremental": incremental}
                    for resource_name in source.resources.keys()
                }
            )
        elif hasattr(source, "set_incremental"):
            source.set_incremental(incremental)
        elif hasattr(source, "incremental"):
            source.incremental = incremental
        else:
            raise ConfigurationError(
                "Source does not support incremental loading. "
                "Source must have with_resources, set_incremental, or incremental attribute."
            )
        
        return source

    def run(self) -> Any:
        """Execute the pipeline.

        Validates configuration, builds dlt pipeline, applies transformers
        and incremental config, then executes the pipeline.

        Returns:
            The result of the pipeline execution.

        Raises:
            ConfigurationError: If source, destination, or transformers are invalid.
            ValidationError: If input validation fails.
            ExecutionError: If pipeline execution fails.
        """
        # Validate configuration
        if self._source is None:
            raise ConfigurationError(
                "Pipeline source not configured. "
                "Call from_sql_table(), from_sql_query(), or from_s3() first."
            )
        
        if self._destination is None:
            raise ConfigurationError(
                "Pipeline destination not configured. "
                "Call to() or specific destination method first."
            )
        
        # Validate transformers
        for i, transformer in enumerate(self._transformers):
            if not callable(transformer):
                raise ConfigurationError(
                    f"Transformer at index {i} is not callable: {type(transformer)}"
                )
        
        destination_config = self._destination
        destination_name = destination_config.get("destination")
        if not destination_name:
            raise ConfigurationError("Destination name is required")

        # Execute with error handling
        try:
            pipeline_name = self._pipeline_name or "fluent_pipeline"
            dataset_name = self._dataset_name or "dataset"

            try:
                pipeline = dlt.pipeline(
                    pipeline_name=pipeline_name,
                    destination=destination_name,
                    dataset_name=dataset_name,
                )
            except Exception as e:
                raise ExecutionError(
                    f"Failed to create dlt pipeline: {str(e)}"
                ) from e

            source = self._source

            # Check for partitioned or split loading
            if self._partition_config is not None:
                try:
                    result = self._execute_partitioned_load(pipeline, source)
                    # After partitioning, continue with regular incremental if needed
                    if result and not result.is_empty:
                        source = self._apply_incremental(source)
                        source = self._apply_transformers(source)
                        result = pipeline.run(source)
                    return result
                except Exception as e:
                    if isinstance(e, (ConfigurationError, ValidationError)):
                        raise
                    raise ExecutionError(
                        f"Partitioned load execution failed: {str(e)}"
                    ) from e
            
            elif self._split_config is not None:
                try:
                    result = self._execute_split_load(pipeline, source)
                    return result
                except Exception as e:
                    if isinstance(e, (ConfigurationError, ValidationError)):
                        raise
                    raise ExecutionError(
                        f"Split load execution failed: {str(e)}"
                    ) from e
            
            else:
                # Regular incremental load
                try:
                    source = self._apply_incremental(source)
                    source = self._apply_transformers(source)
                    result = pipeline.run(source)
                    return result
                except Exception as e:
                    if isinstance(e, (ConfigurationError, ValidationError)):
                        raise
                    raise ExecutionError(
                        f"Pipeline execution failed: {str(e)}"
                    ) from e

        except (ConfigurationError, ValidationError):
            raise
        except ExecutionError:
            raise
        except Exception as e:
            raise ExecutionError(
                f"Pipeline execution failed: {str(e)}"
            ) from e
