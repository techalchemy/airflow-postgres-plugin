import codecs
import configparser
import pathlib

import setuptools

BASE_DIR = pathlib.Path(__file__).parent

setuptools.setup(
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    package_data={"": ["LICENSE*", "README*"]},
    entry_points={
        "airflow.plugins": [
            "postgres_plugin = airflow_postgres_plugin.plugin:PostgresPlugin"
        ]
    },
)
