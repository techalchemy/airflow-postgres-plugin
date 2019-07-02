# -*- coding=utf-8 -*-
# Copyright (c) 2019 Dan Ryan
# MIT License <https://opensource.org/licenses/mit>
from typing import List

from airflow.plugins_manager import AirflowPlugin

from .hooks import PostgresHook
from .operators import (
    FileToPostgresTableOperator,
    PandasToPostgresTableOperator,
    PostgresTableToFileOperator,
    PostgresToFileOperator,
)

__version__ = "0.0.2"
__all__ = ["PostgresPlugin", "__version__"]


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
