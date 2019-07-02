# -*- coding=utf-8 -*-
from typing import List

from airflow.plugins_manager import AirflowPlugin

from .hooks import PostgresHook
from .operators import (
    FileToPostgresTableOperator,
    PandasToPostgresTableOperator,
    PostgresTableToFileOperator,
    PostgresToFileOperator,
)

__all__ = ["PostgresPlugin"]


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
