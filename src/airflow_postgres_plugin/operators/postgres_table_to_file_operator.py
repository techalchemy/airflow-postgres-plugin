# -*- coding=utf-8 -*-

import os
import tempfile
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_postgres_plugin.hooks.postgres_hook import PostgresHook


class PostgresTableToFileOperator(BaseOperator):

    template_fields = ("table", "filepath", "schema")

    @apply_defaults
    def __init__(self, conn_id: str, table: str, filepath: str, schema: str = "public"):
        super(PostgresTableToFileOperator, self).__init__()
        self.conn_id = conn_id
        self.table = table
        self.filepath = filepath
        self.schema = schema
        self._hook: Optional[PostgresHook] = None

    @property
    def hook(self) -> PostgresHook:
        if self._hook is None:
            self._hook = PostgresHook(self.conn_id, schema=self.schema)
        assert self._hook is not None
        return self._hook

    @hook.setter
    def hook(self, val):
        self._hook = val

    def execute(self, context: Dict[str, Any]) -> str:

        if not isinstance(self.filepath, str):
            # generate temporary if no filepath given
            new_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
            new_file.close()
            self.filepath = new_file.name
            self.log.debug(
                f"no filepath given, creating temporary file at {self.filepath!r}"
            )

        self.log.info(
            f"exporting data from table {self.table!r} on {self.hook!r} to "
            f"{self.filepath!r}"
        )
        try:
            self.hook.dump(self.table, self.filepath)
        except Exception as exc:
            raise AirflowException(
                f"Failed writing table {self.table} to file {self.filepath}: {exc}"
            )
        return self.filepath
