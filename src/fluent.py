from typing import Any, Union
from sqlalchemy import Engine
from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials
class FLuentPipeline:
    def __init__(self):
        self._source: Any | None = None
        self._destination: Any | None = None
        self._transformers: list[Any] = []
        self._incremental_config: dict[str, Any] | None = None
        self._pipeline_name: str | None = None
        self._dataset_name: str | None = None
        self._options: dict[str, Any] = {}

    def from_sql_table(self, credentials: Union[ConnectionStringCredentials, Engine,
                                 str] ,table_name: str, schema: str | None = None) -> "FLuentPipeline":
        pass