from typing import Any, Union, Optional, Dict

from dlt.common.configuration.specs.connection_string_credentials import (
    ConnectionStringCredentials,
)
from dlt.common.configuration.specs.aws_credentials import AwsCredentials
from fsspec import AbstractFileSystem
from sqlalchemy import Engine
import dlt

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
    ) -> "FluentPipeline":
        pass

    def from_sql_query(
        self,
        credentials: Union[ConnectionStringCredentials, Engine, str],
        query: str,
        table_name: str,
    ) -> "FluentPipeline":
        pass

    def from_s3(
        self,
        bucket_url: str = dlt.secrets.value,
        credentials: Optional[
            Union[AbstractFileSystem, AwsCredentials]
        ] = None,
        file_glob: str = "*",
        files_per_page: int = DEFAULT_CHUNK_SIZE,
        extract_content: bool = False,
        kwargs: Optional[Dict[str, Any]] = None,
        client_kwargs: Optional[Dict[str, Any]] = None,
        incremental: Optional[dlt.sources.incremental[Any]] = None,
    ) -> "FluentPipeline":
        pass

    def to_redshift():
        pass

    def to_s3():
        pass
