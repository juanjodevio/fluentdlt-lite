from typing import Any, Callable, Dict, Optional, Union

import dlt
from dlt.common.configuration.specs.aws_credentials import AwsCredentials
from dlt.common.configuration.specs.connection_string_credentials import (
    ConnectionStringCredentials,
)
from fsspec import AbstractFileSystem
from sqlalchemy import Engine

from .exceptions import ConfigurationError, ExecutionError, ValidationError

DEFAULT_CHUNK_SIZE = 10000


class FluentPipeline:
    def __init__(self):
        self._source: Any | None = None
        self._destination: Any | None = None
        self._transformers: list[Any] = []
        self._incremental_config: dict[str, Any] | None = None
        self._pipeline_name: str | None = None
        self._dataset_name: str | None = None
        self._options: dict[str, Any] = {}

    def from_sql_table(
        self,
        credentials: Union[ConnectionStringCredentials, Engine, str],
        table_name: str,
        schema: str | None = None,
        **kwargs,
    ) -> "FluentPipeline":
        """Configure SQL table as source."""
        from dlt.sources.sql_database import sql_table

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

        if not query or not query.strip():
            raise ValidationError("Query must be a non-empty string")

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

        self._source = filesystem(
            bucket_url=bucket_url,
            credentials=credentials,
            file_glob=file_glob,
            files_per_page=files_per_page,
            extract_content=extract_content,
            **kwargs,
        )
        return self

    def to_redshift(
        self,
        credentials: Union[str, dict],
        database: str,
        schema: Optional[str] = None,
        **kwargs,
    ) -> "FluentPipeline":
        """Configure Redshift as destination."""
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
        """
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

    def run(self) -> Any:
        """Execute the pipeline.

        Validates configuration, builds dlt pipeline, applies transformers
        and incremental config, then executes the pipeline.

        Returns:
            The result of the pipeline execution.

        Raises:
            ConfigurationError: If source or destination is not set.
            ExecutionError: If pipeline execution fails.
        """
        if self._source is None:
            raise ConfigurationError("Source must be set before running pipeline")
        if self._destination is None:
            raise ConfigurationError("Destination must be set before running pipeline")

        destination_config = self._destination
        destination_name = destination_config.get("destination")
        if not destination_name:
            raise ConfigurationError("Destination name is required")

        try:
            pipeline_name = self._pipeline_name or "fluent_pipeline"
            dataset_name = self._dataset_name or "dataset"

            pipeline = dlt.pipeline(
                pipeline_name=pipeline_name,
                destination=destination_name,
                dataset_name=dataset_name,
            )

            source = self._source

            if self._transformers:
                for transformer in self._transformers:
                    source = transformer(source)

            if self._incremental_config:
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
                            resource: {"incremental": incremental}
                            for resource in source.resources.keys()
                        }
                    )
                elif hasattr(source, "incremental"):
                    source.incremental = incremental

            result = pipeline.run(source)
            return result

        except ConfigurationError:
            raise
        except Exception as e:
            raise ExecutionError(f"Pipeline execution failed: {str(e)}") from e
