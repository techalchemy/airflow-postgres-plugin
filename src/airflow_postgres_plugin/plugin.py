# -*- coding: utf-8 -*-
from typing import List

from airflow.plugins_manager import AirflowPlugin

from airflow_postgres_plugin.hooks.postgres_hook import PostgresHook
from airflow_postgres_plugin.operators import (
    FileToPostgresTableOperator,
    PandasToPostgresTableOperator,
    PostgresTableToFileOperator,
    PostgresToFileOperator,
)


class PostgresPlugin(AirflowPlugin):
    """Apache Airflow Postgres Plugin."""

    name = "postgres_plugin"
    hooks: List = [PostgresHook]
    operators: List = [
        PostgresToFileOperator,
        PostgresTableToFileOperator,
        FileToPostgresTableOperator,
        PandasToPostgresTableOperator,
    ]
    executors: List = []
    macros: List = []
    admin_views: List = []
    flask_blueprints: List = []
    menu_links: List = []
