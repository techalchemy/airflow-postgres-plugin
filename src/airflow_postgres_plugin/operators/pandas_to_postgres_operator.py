# -*- coding=utf-8 -*-
import contextlib
import csv
import hashlib
import os
import tempfile
from typing import IO, Any, Dict, Optional, Tuple, Union

from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy.exc import NoSuchTableError

from airflow_postgres_plugin.hooks.postgres_hook import PostgresHook


class PandasToPostgresTableOperator(BaseOperator):

    template_fields = ("table", "filepath", "schema", "compression", "templates_dict")

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        table: str,
        schema: str = "public",
        sep: str = ",",
        compression: str = "infer",
        chunksize: int = 10000,
        templates_dict: Dict[str, str] = None,
        filepath: Union[IO, str] = None,
        quoting: int = csv.QUOTE_MINIMAL,
        s3_conn_id: str = None,
        include_index: bool = False,
    ):
        super(PandasToPostgresTableOperator, self).__init__()
        self.conn_id = conn_id
        self.table = table
        self.schema = schema
        self.sep = sep
        self.compression = compression
        self.chunksize = chunksize
        self.quoting = quoting
        self.templates_dict = templates_dict
        self.filepath = filepath
        self.schema = schema
        self.s3_conn_id = s3_conn_id
        self.include_index = include_index
        self._s3_hook: Optional[S3Hook] = None
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

    @property
    def s3_hook(self) -> S3Hook:
        if self._s3_hook is None:
            self._s3_hook = S3Hook(self.s3_conn_id, verify=False)
        assert self._s3_hook is not None
        return self._s3_hook

    @s3_hook.setter
    def s3_hook(self, val):
        self._s3_hook = val

    def _get_s3_credentials(self) -> Tuple[str, str]:
        self.log.info("retrieving S3 credentials for access using s3fs")
        s3_credentials = self.s3_hook.get_credentials()
        s3_key_id = s3_credentials.access_key
        s3_access_key = s3_credentials.secret_key
        return s3_key_id, s3_access_key

    def execute(self, context: Dict[str, Any]) -> Optional[str]:
        s3_key_id, s3_access_key = self._get_s3_credentials()
        with temp_environ():
            if s3_key_id is not None:
                os.environ["AWS_ACCESS_KEY_ID"] = s3_key_id
            if s3_access_key is not None:
                os.environ["AWS_SECRET_ACCESS_KEY"] = s3_access_key
            self.log.info(
                f"importing csv data from file: {self.filepath} on {self.hook!r}"
            )
            try:
                self.hook.load_pandas(
                    table=self.table,
                    schema=self.schema,
                    sep=self.sep,
                    compression=self.compression,
                    chunksize=self.chunksize,
                    filepath=self.filepath,
                    quoting=self.quoting,
                    templates_dict=self.templates_dict,
                    include_index=self.include_index,
                )
            except Exception as exc:
                raise AirflowException(f"Failed to load table with exception: {exc!r}")
        return self.table


@contextlib.contextmanager
def temp_environ():
    """Allow the ability to set os.environ temporarily"""
    environ = dict(os.environ)
    try:
        yield

    finally:
        os.environ.clear()
        os.environ.update(environ)
