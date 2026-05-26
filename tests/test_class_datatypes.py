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
    producto=[
        (None, "Lambrusco", 10, 120, True, "Espumosos", timestamp, fecha, 1)
    ]
)

def test_tipos_datos_mas_fechas():
    container = Producto.link("category").all().container(label=True)

    assert container == [
        {
            "category": {
                "CATEGORY ID": [1],
                "Categoria Producto": ["Vinos y Licores"],
            },
            "producto": {
                "PRODUCTO ID": [1],
                "Nombre Producto": ["Lambrusco"],
                "Cantidad Stock": [10],
                "Precio Producto": [120.0],
                "Es Vendible": [1],
                "Notas Extras": ["Espumosos"],
                "Fecha Hora Venta": [datetime.datetime(2026, 1, 1, 0, 0)],
                "Fecha Ingreso": [datetime.date(2026, 1, 1)],
                "Producto Categoria M:1": [1],
            },
            "@positions@": {
                "category": {"Categoria Producto": 1, "CATEGORY ID": 0},
                "producto": {
                    "PRODUCTO ID": 0,
                    "Nombre Producto": 1,
                    "Cantidad Stock": 2,
                    "Precio Producto": 3,
                    "Es Vendible": 4,
                    "Notas Extras": 5,
                    "Fecha Hora Venta": 6,
                    "Fecha Ingreso": 7,
                    "Producto Categoria M:1": 8,
                },
            },
        }
    ]
