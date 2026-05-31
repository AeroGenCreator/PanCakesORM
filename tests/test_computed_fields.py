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

timestamp = datetime.datetime(2026, 1, 1)
fecha = datetime.date(2026, 1, 1)

# GLOBAL PATH para pruebas
dir_ = Path.cwd() / "data" / "test_env"
file = dir_ / "comp_field.sqlite"


class Category(PanCakesORM):
    _table = "category"
    _depends = "self"
    _db_dir = dir_
    _db_file = file

    name = datatype.Char(comment="Categoria Producto")
    producto_ids = datatype.One2Many(
        references="category", inverse_column="category_id"
    )


class Producto(PanCakesORM):
    _table = "producto"
    _depends = ["category"]
    _db_dir = dir_
    _db_file = file

    def _tax_price(self):
        return (self.price.value * 0.16) + self.price.value

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
    tax_price = datatype.Float(comment="Precio + Impuesto", compute="_tax_price")

Category.i(category=[(None, "Vinos y Licores")])
Producto.i(
    producto=[
        (None, "Lambrusco", 10, 120, True, "Espumosos", timestamp, fecha, 1, None)
    ]
)

def test_computed_field():
    dicc = Producto.all().dictionary()

    assert dicc == [
        {
            "producto__producto_id": 1,
            "producto__name": "Lambrusco",
            "producto__qty": 10,
            "producto__price": 120.0,
            "producto__saleable": 1,
            "producto__extras": "Espumosos",
            "producto__sold": datetime.datetime(2026, 1, 1, 0, 0),
            "producto__registry": datetime.date(2026, 1, 1),
            "producto__category_id": 1,
            "producto__tax_price": 139.2,
        }
    ]
