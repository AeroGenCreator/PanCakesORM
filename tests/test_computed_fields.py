# -*- coding: utf-8 -*-
# PanCakesORM v6.0.0 | Test Suite
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

# === DECLARACION DE MODELOS ===

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
    """
    NOTA: 
    Los campos computados no pueden ser tomados como valor estatico:
    
    Por ejemplo:
    Puedes calcular el precio con impuesto multiplicando un valor estatico
    por otro, incluso si son campos de la columna.

    Estatico1 = 0.16
    Estatico2 = self.price.value
    tax_price = Estatico1 * Estatico2
    
    Pero si queremos calcular a traves de algun campo computado, como valor
    estatico se obtendra un error; el dato no existe en buffer.

    Ejemplo: Queremos calcular la posible cantidad monetaria de venta:
    
    Erroneo:
    posible_venta = self.tax_price.value * self.qty.value ❌
    
    Para poder realizar algo similar debemos calcular nuevamente los valores
    independientes 'estaticos':

    estatico1 = 0.16
    estatico2 = self.price.value
    estatico3 = self.qty.value
    posible_venta = ((estatico1 * estatico2) + estatico2) * estatico3 ✅

    A FUTURO SE CONSIDERA UN DECORADOR QUE MEJORE ESTA SITUACION:
    Por el momento esta sintaxis es la unica forma de computar por
    dependencias. Gracias por la comprension! 🥞
    """

    _table = "producto"
    _depends = ["category"]
    _db_dir = dir_
    _db_file = file

    # === CAMPOS ===

    name = datatype.Char(comment="Nombre Producto")
    qty = datatype.Int(comment="Cantidad Stock")
    price = datatype.Float(comment="Precio Producto")
    saleable = datatype.Bool(comment="Es Vendible", compute="_sale_it")
    extras = datatype.Text(comment="Notas Extras")
    sold = datatype.TimeStamp(comment="Fecha Hora Venta")
    registry = datatype.Date(comment="Fecha Ingreso")
    category_id = datatype.ForeignKey(
        comment="Producto Categoria M:1",
        second_table="category",
        column_id="category_id",
    )
    tax_price = datatype.Float(
        comment="Precio + Impuesto", compute="_tax_price"
    )
    possible = datatype.Float(
        comment="Posible Ganancia Total", compute="_total_posible")

    # === FUNCIONES DE COMPUTO PARA LOS CAMPOS DEL MODELO ===

    def _tax_price(self):
        return (self.price.value * 0.16) + self.price.value

    def _total_posible(self):
        return ((self.price.value * 0.16) + self.price.value) * self.qty.value

    def _sale_it(self):
        if (
            ((self.price.value * 0.16) + self.price.value) > 150 or 
            self.qty.value > 20
        ):
            return True 
        else:
            return False

# === PRUEBAS ===

Category.i(category=[(None, "Vinos y Licores")])
Producto.i(
    producto=[
        (
            None,
            "Lambrusco",
            10,
            120,
            None,
            "Espumosos",
            timestamp,
            fecha,
            1,
            None,
            None
        )
    ]
)


def test_computed_fields_when_insert():
    dicc = Producto.all().dictionary()

    assert dicc == [
        {
            "producto__producto_id": 1,
            "producto__name": "Lambrusco",
            "producto__qty": 10,
            "producto__price": 120.0,
            "producto__saleable": 0,
            "producto__extras": "Espumosos",
            "producto__sold": datetime.datetime(2026, 1, 1, 0, 0),
            "producto__registry": datetime.date(2026, 1, 1),
            "producto__category_id": 1,
            "producto__tax_price": 139.2,
            "producto__possible": 1392.0
        }
    ]


def test_computed_fields_when_update():
    """
    Actualizamos el precio, pero como tax_price depende de precio
    en su compudacion, se actualiza el valor tambien por si solo
    """
    Producto.u(producto__price__producto_id__same=[200, 1])
    dicc = Producto.all().dictionary()

    assert dicc == [
        {
            "producto__producto_id": 1,
            "producto__name": "Lambrusco",
            "producto__qty": 10,
            "producto__price": 200.0,
            "producto__saleable": 1,
            "producto__extras": "Espumosos",
            "producto__sold": datetime.datetime(2026, 1, 1, 0, 0),
            "producto__registry": datetime.date(2026, 1, 1),
            "producto__category_id": 1,
            "producto__tax_price": 232.0,
            "producto__possible": 2320.0
        }
    ]

def test_computed_more_than_one_alteration():
    Producto.u(
        producto__price__producto_id__same=[100, 1],
        producto__qty__producto_id__same=[5, 1]
    )
    dicc = Producto.all().dictionary()

    assert dicc == [
        {
            'producto__producto_id': 1,
            'producto__name': 'Lambrusco',
            'producto__qty': 5,
            'producto__price': 100.0,
            'producto__saleable': 0,
            'producto__extras': 'Espumosos',
            'producto__sold': datetime.datetime(2026, 1, 1, 0, 0),
            'producto__registry': datetime.date(2026, 1, 1),
            'producto__category_id': 1,
            'producto__tax_price': 116.0,
            'producto__possible': 580.0
        }
    ]

def test_instert_more():
    Producto.i(
        producto=[
            (
                None,
                "Gato Negro",
                20,
                250,
                None,
                "Tinto",
                timestamp,
                fecha,
                1,
                None,
                None
            )
        ],
        category=[
            (None, "Harinas")
        ]
    )

    pr = Producto.all().dictionary()
    ct = Category.all().dictionary()

    assert pr == [
        {
            'producto__producto_id': 1,
            'producto__name': 'Lambrusco',
            'producto__qty': 5,
            'producto__price': 100.0,
            'producto__saleable': 0,
            'producto__extras': 'Espumosos',
            'producto__sold': datetime.datetime(2026, 1, 1, 0, 0),
            'producto__registry': datetime.date(2026, 1, 1),
            'producto__category_id': 1,
            'producto__tax_price': 116.0,
            'producto__possible': 580.0
        },
        {
            'producto__producto_id': 2,
            'producto__name': 'Gato Negro',
            'producto__qty': 20,
            'producto__price': 250.0,
            'producto__saleable': 1,
            'producto__extras': 'Tinto',
            'producto__sold': datetime.datetime(2026, 1, 1, 0, 0),
            'producto__registry': datetime.date(2026, 1, 1),
            'producto__category_id': 1,
            'producto__tax_price': 290.0,
            'producto__possible': 5800.0
        }
    ]

    assert ct == [
        {'category__category_id': 1, 'category__name': 'Vinos y Licores'},
        {'category__category_id': 2, 'category__name': 'Harinas'}
    ]

def test_computed_fields_when_list():
    Producto.u(producto__price__producto_id__in=[50, [1, 2]])
    res = Producto.all().dictionary()

    assert res == [
        {
            'producto__producto_id': 1,
            'producto__name': 'Lambrusco',
            'producto__qty': 5,
            'producto__price': 50.0,
            'producto__saleable': 0,
            'producto__extras': 'Espumosos',
            'producto__sold': datetime.datetime(2026, 1, 1, 0, 0),
            'producto__registry': datetime.date(2026, 1, 1),
            'producto__category_id': 1,
            'producto__tax_price': 58.0,
            'producto__possible': 290.0
        },
        {
            'producto__producto_id': 2,
            'producto__name': 'Gato Negro',
            'producto__qty': 20,
            'producto__price': 50.0,
            'producto__saleable': 0,
            'producto__extras': 'Tinto',
            'producto__sold': datetime.datetime(2026, 1, 1, 0, 0),
            'producto__registry': datetime.date(2026, 1, 1),
            'producto__category_id': 1,
            'producto__tax_price': 58.0,
            'producto__possible': 1160.0
        }
    ]
