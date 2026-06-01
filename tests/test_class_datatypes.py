# -*- coding: utf-8 -*-
# PanCakesORM v5.0.0 | Test Suite
# Copyright (c) 2026 AeroGenCreator (https://github.com/AeroGenCreator)
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
# ==============================================================================


# Modulos Python
import datetime
from pathlib import Path

from pancakes.models.model import PanCakesORM
from pancakes.sql import datatype

# GLOBAL PATH para pruebas
dir_ = Path.cwd() / "data" / "test_env"
file = dir_ / "sql_datatype.sqlite"


class Category(PanCakesORM):
    _table = "category"
    _depends = "self"

    name = datatype.Char(comment="Categoria Producto")
    producto_ids = datatype.One2Many(
        references="producto", inverse_column="category_id"
    )


class Producto(PanCakesORM):
    _table = "producto"
    _depends = ["category"]

    name = datatype.Char(comment="Nombre Producto")
    qty = datatype.Int(comment="Cantidad Stock")
    price = datatype.Float(comment="Precio Producto")
    saleable = datatype.Bool(comment="Es Vendible")
    extras = datatype.Text(comment="Notas Extras")
    sold = datatype.TimeStamp(comment="Fecha Hora Venta")
    registry = datatype.Date(comment="Fecha Ingreso")
    category_id = datatype.ForeignKey(
        comment="Producto Categoria M:1",
        second_table="category",
        column_id="category_id",
    )


timestamp = datetime.datetime(2026, 1, 1)
fecha = datetime.date(2026, 1, 1)

Category.i(category=[(None, "Vinos y Licores")])
Category.i(
    producto=[(None, "Lambrusco", 10, 120, True, "Espumosos", timestamp, fecha, 1)]
)


def test_tipos_datos_mas_fechas():
    container = Producto.link("category").all().container()

    assert container == {
        "producto": {
            "@main_table@": True,
            "@depends@": ["category"],
            "producto_id": {
                "vector": [1],
                "label": "PRODUCTO ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "INTEGER",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": ["Lambrusco"],
                "label": "Nombre Producto",
                "position": 1,
                "readonly": False,
                "default": None,
                "required": False,
                "python_type": "str",
                "primary_key": False,
                "sql_type": "VARCHAR",
                "second_table": False,
                "foreign_key": False,
            },
            "qty": {
                "vector": [10],
                "label": "Cantidad Stock",
                "position": 2,
                "readonly": False,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": False,
                "sql_type": "INTEGER",
                "second_table": False,
                "foreign_key": False,
            },
            "price": {
                "vector": [120.0],
                "label": "Precio Producto",
                "position": 3,
                "readonly": False,
                "default": None,
                "required": False,
                "python_type": "float",
                "primary_key": False,
                "sql_type": "FLOAT",
                "second_table": False,
                "foreign_key": False,
            },
            "saleable": {
                "vector": [True],
                "label": "Es Vendible",
                "position": 4,
                "readonly": False,
                "default": None,
                "required": False,
                "python_type": "bool",
                "primary_key": False,
                "sql_type": "BOOLEAN",
                "second_table": False,
                "foreign_key": False,
            },
            "extras": {
                "vector": ["Espumosos"],
                "label": "Notas Extras",
                "position": 5,
                "readonly": False,
                "default": None,
                "required": False,
                "python_type": "str",
                "primary_key": False,
                "sql_type": "TEXT",
                "second_table": False,
                "foreign_key": False,
            },
            "sold": {
                "vector": ["2026-01-01T00:00:00"],
                "label": "Fecha Hora Venta",
                "position": 6,
                "readonly": False,
                "default": None,
                "required": False,
                "python_type": "datetime",
                "primary_key": False,
                "sql_type": "TIMESTAMP",
                "second_table": False,
                "foreign_key": False,
            },
            "registry": {
                "vector": ["2026-01-01"],
                "label": "Fecha Ingreso",
                "position": 7,
                "readonly": False,
                "default": None,
                "required": False,
                "python_type": "date",
                "primary_key": False,
                "sql_type": "DATE",
                "second_table": False,
                "foreign_key": False,
            },
            "category_id": {
                "vector": [1],
                "label": "Producto Categoria M:1",
                "position": 8,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": False,
                "sql_type": "FOREIGN KEY",
                "second_table": "category",
                "foreign_key": "category_id",
            },
        },
        "category": {
            "@main_table@": False,
            "@depends@": ["self"],
            "category_id": {
                "vector": [1],
                "label": "CATEGORY ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "INTEGER",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": ["Vinos y Licores"],
                "label": "Categoria Producto",
                "position": 1,
                "readonly": False,
                "default": None,
                "required": False,
                "python_type": "str",
                "primary_key": False,
                "sql_type": "VARCHAR",
                "second_table": False,
                "foreign_key": False,
            },
        },
    }


def test_One2Many_yields_data():
    container = Category.all().container()

    # QUERY NO DEVUELVE ONE2MANY -> SE OBTIENE EN SCHEMA
    assert container == {
        "category": {
            "@main_table@": True,
            "@depends@": ["self"],
            "category_id": {
                "vector": [1],
                "label": "CATEGORY ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "INTEGER",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": ["Vinos y Licores"],
                "label": "Categoria Producto",
                "position": 1,
                "readonly": False,
                "default": None,
                "required": False,
                "python_type": "str",
                "primary_key": False,
                "sql_type": "VARCHAR",
                "second_table": False,
                "foreign_key": False,
            },
        }
    }
