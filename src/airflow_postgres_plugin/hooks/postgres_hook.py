# -*- coding=utf-8 -*-

import atexit
import contextlib
import csv
import datetime
import decimal
import hashlib
import io
import os
from typing import IO, Any, Dict, Generator, List, Optional, Type, Union

import dateutil.utils
import pandas
import postgres_copy
import psycopg2
import sqlalchemy
from airflow.exceptions import AirflowException
from airflow.hooks.dbapi_hook import DbApiHook
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.schema import MetaData

TBasic = Union[str, int, bool]


class PostgresHook(DbApiHook):

    conn_name_attr: str = "postgres_conn_id"
    default_conn_name: str = "postgres_default"
    supports_autocommit: bool = True

    def __init__(
        self, postgres_conn_id: str, database: str = None, schema: str = None
    ) -> None:
        super(PostgresHook, self).__init__()
        self.postgres_conn_id = postgres_conn_id
        self._engine: sqlalchemy.engine.Engine = None
        self.database = database
        self.schema = schema

    @staticmethod
    def _serialize_cell(cell, conn):
        return cell

    @property
    def engine(self):
        if not self._engine:
            self._engine = self.get_sqlalchemy_engine()
            atexit.register(self._engine.dispose)
        return self._engine

    def get_conn(self) -> psycopg2._psycopg.connection:
        connection = self.get_connection(self.postgres_conn_id)
        if connection.schema and not self.database:
            self.database = connection.schema
        connection_args: Dict[str, TBasic]
        connection_args = {
            "host": connection.host,
            "user": connection.login,
            "password": connection.password,
            "dbname": self.database or connection.schema,
            "port": connection.port,
        }
        schema = next(
            iter([schema for schema in [self.database, connection.schema]]), None
        )
        if isinstance(schema, str):
            connection_args["options"] = f"-c search_path={schema}"

        for (key, value) in connection.extra_dejson.items():
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

        self.log.info(f"establishing connection to postgres at {connection.host!r}")
        return psycopg2.connect(**connection_args)

    def get_sqlalchemy_sessionmaker(self) -> Type[Session]:
        self.log.info(
            f"buliding sqlalchemy sessionmaker instance with engine {self.engine!r}"
        )
        return scoped_session(sessionmaker(bind=self.engine, expire_on_commit=False))

    def get_sqlalchemy_session(self) -> Session:
        self.log.info(f"building sqlalchemy session")
        return self.get_sqlalchemy_sessionmaker()()

    def get_sqlalchemy_metadata(self, schema: str = None) -> MetaData:
        if not schema:
            schema = self.schema
        self.log.info(
            f"building sqlalchemy metadata with engine {self.engine!r} "
            f"with schema {schema!r}"
        )
        return MetaData(bind=self.engine, schema=schema)

    def get_sqlalchemy_table(self, name: str, schema: str = None) -> sqlalchemy.Table:
        self.log.info(f"getting introspected sqlalchemy table {name!r}")
        return sqlalchemy.Table(
            name,
            self.get_sqlalchemy_metadata(schema=schema),
            autoload=True,
            autoload_with=self.engine,
        )

    @contextlib.contextmanager
    def sqlalchemy_session(self) -> Generator[Session, None, None]:
        with contextlib.closing(self.get_sqlalchemy_session()) as session:
            self.log.info(f"entering sqlalchemy session context with session {session!r}")
            try:
                yield session
                if not self.get_autocommit(session):
                    session.commit()
            except Exception as exc:
                self.log.exception(
                    f"exception occured during session {session!r}: {exc!r}"
                )
                session.rollback()

    def duplicate_table_to_temp_table(
        self,
        from_table: str,
        temp_name: str = None,
        schema: str = None,
        include_constraints: bool = False,
    ):
        if not temp_name:
            random_hash = hashlib.sha256(os.urandom(128)).hexdigest()[:6]
            date = dateutil.utils.today().strftime("%Y-%m-%d")
            temp_name = f"_temp_{from_table}_{random_hash}_{date}"
        if not schema:
            schema = self.schema
        self.log.debug(
            f"temporary table requested, creating temporary table at {temp_name!r}"
        )
        table = self.duplicate_table(
            from_table, temp_name, include_constraints=include_constraints, schema=schema
        )
        return table

    def duplicate_table(
        self,
        name: str,
        duplicate_name: str,
        include_constraints: bool = False,
        schema: str = None,
    ) -> sqlalchemy.Table:
        self.log.info(f"duplicating table {name!r} to {duplicate_name!r}")
        metadata = self.get_sqlalchemy_metadata(schema=schema)
        original = self.get_sqlalchemy_table(name, schema=schema)
        columns = [_.copy() for _ in original.columns] + (
            [_.copy() for _ in original.constraints] if include_constraints else []
        )
        duplicate = sqlalchemy.Table(duplicate_name, metadata, *columns, schema=schema)
        try:
            metadata.create_all(tables=[duplicate])
        except Exception as exc:
            raise AirflowException(f"failed to duplicate table: {duplicate} => {exc!r}")
        self.log.info(f"Successfully duplicated table {duplicate}")
        return duplicate

    def upsert(
        self,
        from_table: sqlalchemy.Table,
        to_table: sqlalchemy.Table,
        extra_constraints: List[str] = None,
        **upsert_params,
    ):
        insert_statement: sqlalchemy.sql.dml.Insert = insert(to_table).from_select(
            from_table.columns.keys(), from_table.select()
        )
        inspected: sqlalchemy.Table = sqlalchemy.inspect(to_table)
        primary_keys: List[str] = [_.name for _ in inspected.primary_key]
        if isinstance(extra_constraints, list) and len(extra_constraints) > 0:
            primary_keys = extra_constraints
        upsert_statement: sqlalchemy.sql.dml.Insert = insert_statement.on_conflict_do_update(
            index_elements=primary_keys,
            set_={
                column.name: getattr(insert_statement.excluded, column.name)
                for column in inspected.columns
            },
        )
        with self.sqlalchemy_session() as session:
            upsert_text = ""
            if "start_datetime" in upsert_params:
                upsert_text += "SET polling_interval.start_datetime TO :start_datetime;"
            if "end_datetime" in upsert_params:
                upsert_text += "\nSET polling_interval.end_datetime TO :end_datetime;"
            if upsert_text:
                session.execute(sqlalchemy.text(upsert_text), upsert_params)
            session.execute(sqlalchemy.text(str(upsert_statement)))

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
                self.engine,
                format="csv",
                header=True,
                columns=columns,
            )

    def get_table_if_exists(
        self, table: str, schema: str = "public", engine: sqlalchemy.engine.Engine = None
    ) -> Optional[sqlalchemy.Table]:
        self.log.info("checking database for destination table...")
        if not self.engine.has_table(table, schema=schema):
            self.log.warn(f"no such table for loading data: {schema}.{table}")
            return None
        self.log.info(f"loading table {table!r}")
        sqlalchemy_table = self.get_sqlalchemy_table(table, schema=schema)
        return sqlalchemy_table

    def get_sqlalchemy_col_types(
        self, table: sqlalchemy.Table, exclude: List[str] = None
    ) -> Dict[str, Type]:
        if exclude is None:
            exclude = []
        elif isinstance(exclude, str):
            exclude = [exclude]
        return {c.name: c.type for c in table.columns if c.name not in exclude}

    def get_sqlalchemy_table_python_types(
        self, table: sqlalchemy.Table, exclude: List[str] = None
    ) -> Dict[str, Type]:
        if exclude is None:
            exclude = []
        elif isinstance(exclude, str):
            exclude = [exclude]
        column_types = self.get_sqlalchemy_col_types(table, exclude=exclude)
        conversions = {
            name: c.python_type if c.python_type is not decimal.Decimal else "float64"
            for name, c in column_types.items()
            if c.python_type not in (datetime.date, datetime.datetime)
        }
        return conversions

    def load_df(
        self,
        df: pandas.DataFrame,
        table: str,
        schema: str = "public",
        chunksize: int = 10000,
        include_index: bool = False,
        engine: Optional[sqlalchemy.engine.Engine] = None,
        conn: Optional[psycopg2.extensions.connection] = None,
        col_type_map: Dict[str, sqlalchemy.sql.type_api.TypeEngine] = None,
        create_tables: bool = False,
    ) -> Optional[str]:
        if not schema:
            schema = self.schema
        if not engine and not conn:
            engine = self.engine
        if engine and not create_tables and not engine.has_table(table, schema=schema):
            return None
        if not col_type_map:
            col_type_map = self.get_sqlalchemy_col_types(
                table=self.get_table_if_exists(table, schema=schema, engine=self.engine),
                exclude=["id"],
            )
        sql_args = {
            "name": table,
            "con": engine or conn,
            "schema": schema,
            "if_exists": "append",
            "chunksize": chunksize,
            "method": "multi",
            "index": include_index,
            "dtype": col_type_map,
        }
        df.to_sql(**sql_args)
        return table

    def stream_csv_to_df(
        self,
        csv_file: Union[IO, str],
        schema: str = "public",
        sep: str = ",",
        compression: str = "infer",
        chunksize: int = None,
        table: Optional[str] = None,
        quoting: int = csv.QUOTE_MINIMAL,
        include_index: bool = False,
        col_type_map: Dict[str, Type] = None,
    ) -> Generator[None, None, pandas.DataFrame]:
        read_kwargs = {
            "sep": sep,
            "parse_dates": True,
            "infer_datetime_format": True,
            "compression": compression,
            "chunksize": chunksize,
            "quoting": quoting,
            "error_bad_lines": False,
            "warn_bad_lines": True,
        }
        if table is not None and not col_type_map:
            table_instance = self.get_table_if_exists(table, schema=schema)
            col_type_map = self.get_sqlalchemy_table_python_types(
                table_instance, exclude=["id"]
            )
        if col_type_map:
            read_kwargs["dtype"] = col_type_map
            read_kwargs["usecols"] = list(col_type_map.keys())
        df_stream = pandas.read_csv(csv_file, **read_kwargs)
        return df_stream

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
        templates_dict: Dict[str, str] = None,
        create_tables: bool = False,
    ) -> Optional[str]:
        target_table = self.get_table_if_exists(table, schema=schema, engine=self.engine)
        if target_table is None and not create_tables:
            return None
        temp_table = self.duplicate_table_to_temp_table(table, schema=schema)
        col_type_map = self.get_sqlalchemy_col_types(temp_table, exclude=["id"])
        csv_python_types = self.get_sqlalchemy_table_python_types(
            temp_table, exclude=["id"]
        )
        df_stream_kwargs = {
            "schema": schema,
            "sep": sep,
            "compression": compression,
            "chunksize": chunksize,
            "quoting": quoting,
            "include_index": include_index,
            "col_type_map": csv_python_types,
        }
        with contextlib.closing(self.get_conn()) as conn:
            self.set_autocommit(conn, True)
            try:
                for df in self.stream_csv_to_df(filepath, **df_stream_kwargs):
                    self.load_df(  # type: ignore
                        df,
                        table=temp_table.name,
                        conn=conn,
                        chunksize=chunksize,
                        include_index=include_index,
                        schema=schema,
                        col_type_map=col_type_map,
                        create_tables=create_tables,
                    )
            except Exception as exc:
                raise AirflowException(
                    f"Failed loading dataframes for table {table}:\n" f"{exc!r}"
                )
        return temp_table.name

    def dump(self, table: str, filepath: str) -> None:
        self.log.info(f"dumping content of table {table!r} to {filepath!r}")
        with self.sqlalchemy_session() as session:
            with open(filepath, "w") as fp:
                postgres_copy.copy_to(
                    session.query(self.get_sqlalchemy_table(table)),
                    fp,
                    self.engine,
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

    def copy_expert(self, sql, filename, open=open):
        """
        Executes SQL using psycopg2 copy_expert method.
        Necessary to execute COPY command without access to a superuser.

        Note: if this method is called with a "COPY FROM" statement and
        the specified input file does not exist, it creates an empty
        file and no data is loaded, but the operation succeeds.
        So if users want to be aware when the input file does not exist,
        they have to check its existence by themselves.
        """
        if not os.path.isfile(filename):
            with open(filename, "w"):
                pass

        with open(filename, "r+") as f:
            with contextlib.closing(self.get_conn()) as conn:
                with contextlib.closing(conn.cursor()) as cur:
                    cur.copy_expert(sql, f)
                    f.truncate(f.tell())
                    conn.commit()

    def bulk_load(self, table, tmp_file):
        """
        Loads a tab-delimited file into a database table
        """
        self.copy_expert("COPY {table} FROM STDIN".format(table=table), tmp_file)

    def bulk_dump(self, table, tmp_file):
        """
        Dumps a database table into a tab-delimited file
        """
        self.copy_expert("COPY {table} TO STDOUT".format(table=table), tmp_file)
