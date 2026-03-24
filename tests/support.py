from __future__ import annotations

import importlib


def humemdb_class():
    return importlib.import_module("humemdb").HumemDB


def translate_sql():
    return importlib.import_module("humemdb").translate_sql


def runtime_module():
    return importlib.import_module("humemdb.runtime")


def vector_module():
    return importlib.import_module("humemdb.vector")
