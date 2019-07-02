# -*- coding=utf-8 -*-

import contextlib
import csv
import datetime
import decimal
import io
from typing import IO, Any, Dict, Generator, List, Optional, Type, Union

import pandas
import postgres_copy
import psycopg2
import sqlalchemy
from airflow.exceptions import AirflowException
from airflow.hooks.dbapi_hook import DbApiHook
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.schema import MetaData

TBasic = Union[str, int, bool]


class PostgresHook(DbApiHook):

    conn_name_attr: str = "postgres_conn_id"
    default_conn_name: str = "postgres_default"
    supports_autocommit: bool = True

    def __init__(
        self,
        postgres_conn_id: str,
        database: str = None,
        schema: str = None,
        connection: str = None,
        *args,
        **kwargs,
    ) -> None:
        super(PostgresHook, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.schema = schema
        self.connection = self.get_connection(self.postgres_conn_id)

    @staticmethod
    def _serialize_cell(cell, conn):
        return cell

    def get_conn(self) -> psycopg2._psycopg.connection:
        connection_args: Dict[str, TBasic]
        connection_args = {
            "host": self.connection.host,
            "user": self.connection.login,
            "password": self.connection.password,
            "dbname": self.database or self.connection.schema,
            "port": self.connection.port,
        }

        if isinstance(self.schema, str):
            connection_args["options"] = f"-c search_path={self.schema}"

        for (key, value) in self.connection.extra_dejson.items():
            if key in (
                "sslmode",
                "sslcert",
                "sslkey",
                "sslrootkey",
                "sslcrl",
                "application_name",
                "keepalives_idle",
            ):
                connection_args[key] = value

        self.log.info(f"establishing connection to postgres at {self.connection.host!r}")
        return psycopg2.connect(**connection_args)

    def get_sqlalchemy_sessionmaker(self) -> Type[Session]:
        engine: sqlalchemy.engine.Engine = self.get_sqlalchemy_engine()
        self.log.info(f"buliding sqlalchemy sessionmaker instance with engine {engine!r}")
        return sessionmaker(bind=engine)

    def get_sqlalchemy_session(self) -> Session:
        self.log.info(f"building sqlalchemy session")
        return self.get_sqlalchemy_sessionmaker()()

    def get_sqlalchemy_metadata(self) -> MetaData:
        engine = self.get_sqlalchemy_engine()
        schema = self.schema or self.connection.schema
        self.log.info(
            f"building sqlalchemy metadata with engine {engine!r} "
            f"with schema {schema!r}"
        )
        return MetaData(bind=engine, schema=schema)

    def get_sqlalchemy_table(self, name: str) -> sqlalchemy.Table:
        self.log.info(f"getting introspected sqlalchemy table {name!r}")
        return sqlalchemy.Table(
            name,
            self.get_sqlalchemy_metadata(),
            autoload=True,
            autoload_with=self.get_sqlalchemy_engine(),
        )

    @contextlib.contextmanager
    def sqlalchemy_session(self) -> Generator[Session, None, None]:
        session = self.get_sqlalchemy_session()
        self.log.info(f"entering sqlalchemy session context with session {session!r}")
        try:
            yield session
            session.commit()
        except Exception as exc:
            self.log.exception(f"exception occured during session {session!r}: {exc!r}")
            session.rollback()
        finally:
            self.log.info(f"closing sqlalchemy session {session!r}")
            session.close()

    def duplicate_table(
        self, name: str, duplicate_name: str, include_constraints: bool = False
    ) -> sqlalchemy.Table:
        self.log.info(f"duplicating table {name!r} to {duplicate_name!r}")
        metadata = self.get_sqlalchemy_metadata()
        original = self.get_sqlalchemy_table(name)
        columns = [_.copy() for _ in original.columns] + (
            [_.copy() for _ in original.constraints] if include_constraints else []
        )
        duplicate = sqlalchemy.Table(duplicate_name, metadata, *columns)
        try:
            metadata.create_all(tables=[duplicate])
        except Exception as exc:
            raise AirflowException(f"failed to duplicate table: {duplicate} => {exc!r}")
        self.log.info(f"Successfully duplicated table {duplicate}")
        return duplicate

    def load(self, table: str, filepath: str) -> None:
        self.log.info(f"loading table {table!r} with content at {filepath!r}")
        with open(filepath, "r") as fp:
            reader = csv.reader(fp)
            row = next(iter(reader), None)
            if not row:
                self.log.warning(
                    f"failed to load data from file {filepath!r}: File is empty."
                )
                return None
            columns = tuple(row[:])
            fp.seek(0)
            self.log.info(f"found columns: {columns}")
            target_table = self.get_sqlalchemy_table(table)
            self.log.info(f"writing to columns: {target_table.columns.keys()}")

            postgres_copy.copy_from(
                fp,
                self.get_sqlalchemy_table(table),
                self.get_sqlalchemy_engine(),
                format="csv",
                header=True,
                columns=columns,
            )

    def load_pandas(
        self,
        table: str,
        schema: str = "public",
        sep: str = ",",
        compression: str = "infer",
        chunksize: int = 10000,
        filepath: Union[IO, str] = None,
        quoting: int = csv.QUOTE_MINIMAL,
        include_index: bool = False,
    ) -> Optional[str]:
        engine = self.get_sqlalchemy_engine()
        self.log.info("checking database for destination table...")
        if not engine.has_table(table, schema=schema):
            self.log.warn(f"no such table for loading data: {schema}.{table}")
            return None
        self.log.info(f"loading table {table!r}")
        sqlalchemy_table = self.get_sqlalchemy_table(table)
        columns = [c for c in sqlalchemy_table.columns if c.name != "id"]
        col_types = {c.name: c.type for c in columns}
        conversions = {
            name: c.python_type if c.python_type is not decimal.Decimal else "float64"
            for name, c in col_types.items()
            if c.python_type not in (datetime.date, datetime.datetime)
        }
        sql_args = {
            "name": table,
            "con": engine,
            "schema": schema,
            "if_exists": "append",
            "chunksize": chunksize,
            "method": "multi",
            "index": include_index,
            "dtype": col_types,
        }
        df_stream = pandas.read_csv(
            filepath,
            sep=sep,
            parse_dates=True,
            infer_datetime_format=True,
            compression=compression,
            chunksize=chunksize,
            quoting=quoting,
            dtype=conversions,
            error_bad_lines=False,
            warn_bad_lines=True,
            usecols=[c.name for c in columns],
        )
        for df in df_stream:
            df.to_sql(**sql_args)
        return table

    def dump(self, table: str, filepath: str) -> None:
        self.log.info(f"dumping content of table {table!r} to {filepath!r}")
        with self.sqlalchemy_session() as session:
            with open(filepath, "w") as fp:
                postgres_copy.copy_to(
                    session.query(self.get_sqlalchemy_table(table)),
                    fp,
                    self.get_sqlalchemy_engine(),
                    format="csv",
                    header=True,
                )

    def query(
        self,
        sql: str,
        parameters: List[str],
        include_headers: bool = True,
        returns_rows: bool = True,
    ) -> Generator[Optional[List[Any]], None, None]:
        if parameters is None:
            parameters = []

        if len(parameters) > 0:
            sql = sql % tuple(parameters)

        self.log.info(f"executing query {sql!r}")
        sql_text = sqlalchemy.text(sql)
        with self.sqlalchemy_session() as session:
            try:
                results = session.execute(sql_text)
            except Exception as exc:
                raise AirflowException(
                    f"failed to execute query: {sql_text} with exception: {exc!r}"
                )
            if include_headers:
                yield results.keys()
            if not returns_rows:
                return None
            for row in results:
                yield row

    def export(
        self,
        sql: str,
        filepath: str,
        parameters: List[str],
        include_headers: bool = True,
        **kwargs,
    ) -> str:
        self.log.info(f"writing results of sql {sql!r} to {filepath!r}")
        with open(filepath, "w") as fp:
            writer = csv.writer(fp)
            for result in self.query(
                sql, parameters=parameters, include_headers=include_headers
            ):
                if result is not None:
                    writer.writerow(result)
        return filepath
