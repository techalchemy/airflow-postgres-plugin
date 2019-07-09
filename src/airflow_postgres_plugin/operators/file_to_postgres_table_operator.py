# -*- coding=utf-8 -*-

import hashlib
import os
import tempfile
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_postgres_plugin.hooks.postgres_hook import PostgresHook


class FileToPostgresTableOperator(BaseOperator):

    template_fields = ("table", "filepath", "schema")

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        table: str,
        filepath: str,
        schema: str = "public",
        temp_table: bool = True,
        *args,
        **kwargs,
    ):
        super(FileToPostgresTableOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.filepath = filepath
        self.schema = schema
        self.temp_table = temp_table
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

        table = self.table
        if self.temp_table:
            random_hash = hashlib.sha256(os.urandom(128)).hexdigest()[:8]
            temp_name = f"_temp_{table}_{random_hash}"
            self.log.debug(
                f"temporary table requested, creating temporary table at {temp_name!r}"
            )
            table = self.hook.duplicate_table(
                table, temp_name, include_constraints=False
            ).name

        self.log.info(
            f"importing data from file {self.filepath!r} to table {table!r} on "
            f"{self.hook!r}"
        )
        try:
            self.hook.load(table, self.filepath)
        except Exception as exc:
            raise AirflowException(f"Failed to load table with exception: {exc!r}")
        return table
