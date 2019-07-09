# -*- coding=utf-8 -*-

import logging
import os
import tempfile
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_postgres_plugin.hooks.postgres_hook import PostgresHook


class PostgresToFileOperator(BaseOperator):

    template_fields = ("sql", "sql_args", "filepath", "schema")

    @apply_defaults
    def __init__(
        self, conn_id: str, sql: str, sql_args: str, filepath: str, schema: str = "public"
    ):
        super(PostgresToFileOperator, self).__init__()
        self.conn_id = conn_id
        self.sql = sql
        self.sql_args = sql_args
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

        statement = self.sql % tuple(self.sql_args.split(","))
        self.log.info(
            f"exporting data from executing {statement!r} on "
            f"{self.hook!r} to {self.filepath!r}"
        )
        try:
            self.hook.export(self.sql, self.filepath, parameters=self.sql_args.split(","))
        except Exception as exc:
            raise AirflowException(f"Failed exporting data: {exc!r}")
        return self.filepath
