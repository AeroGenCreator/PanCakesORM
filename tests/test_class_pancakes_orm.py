# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

""" Test Main Class -*- PanCakesORM -*- """

# Modulos Propios
from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype

# Modulos Python
from pathlib import Path
from datetime import date

# Modulos Desarrollo
import ipdb
import pandas as pd

# GLOBAL PATH para pruebas
dir_ = Path.cwd() / 'data' / 'test_env'
file =  dir_ / 'class.sqlite'

# Crear una tablas:
class Category(PanCakesORM):
    _table = 'category'
    _db_dir = dir_
    _db_file = file

    name = sql_datatype.Char(comment='Categoria', nls=False)

class Product(PanCakesORM):
    _table = 'product'
    _db_dir = dir_
    _db_file = file

    name = sql_datatype.Char(comment='Producto', nls=False)
    price = sql_datatype.Float(comment='Precio', nls=False)
    category_id = sql_datatype.ForeignKey(
            second_table = 'category',
            column_id = 'category_id',
            comment = 'Categoria'
        )

class Client(PanCakesORM):
    _table = 'client'
    _db_dir = dir_
    _db_file = file

    name = sql_datatype.Char(comment='Nombre', nls=False)
    surname = sql_datatype.Char(comment='Apellido', nls=False)

class Sale(PanCakesORM):
    _table = 'sale'
    _db_dir = dir_
    _db_file = file

    code = sql_datatype.Char(comment='Folio', nls=False)
    date = sql_datatype.Text(comment='Fecha', nls=False)

class SaleLine(PanCakesORM):
    _table = 'sale_line'
    _db_dir = dir_
    _db_file = file

    client_id = sql_datatype.ForeignKey(
            second_table='client',
            column_id = 'client_id'
        )
    sale_id = sql_datatype.ForeignKey(
            second_table='sale',
            column_id='sale_id'
        )
    product_id = sql_datatype.ForeignKey(
            second_table='product',
            column_id='product_id'
        )
    amount = sql_datatype.Float(comment='Cantidad', nls=False)

def test_write():
    # Escribo sobre todas las tablas definidas arriba:
    Category.write(
            [
                (None, 'Lacteos'),
                (None, 'Harinas'),
                (None, 'Extras'),
            ]
        )

    Product.write(
        [
            (None, 'Mantequilla', 10.5, None),
            (None, 'Harina Trigo', 20, None),
            (None, 'Huevo', 15.5, None),
            (None, 'Levadura', 5, None)
        ]
    )

    Client.write(
            [
                (None, 'Andres', 'Lopez'),
                (None, 'Polar', 'Poo')
            ]
        )

    Sale.write(
            [
                (None, 'S1', date.today().isoformat())
            ]
        )

    SaleLine.write(
            [
                (None, 1, 1, 2, 3),
                (None, 1, 1, 3, 2),
                (None, 1, 1, 4, 5),
                (None, 1, 1, 1, 8)
            ]
        )

    result = Category.return_all()
    assert result == [
        (1, 'Lacteos'),
        (2, 'Harinas'),
        (3, 'Extras')
    ]

def test_update():
    Product.update(
            [
                {
                'column':'category_id',
                'value':1,
                'on':'name',
                'key':'Mantequilla',
                },
                {
                'column':'category_id',
                'value':2,
                'on':'name',
                'key':'Harina Trigo',
                },
                {
                'column':'category_id',
                'value':3,
                'on':'name',
                'key':'Huevo',
                },
                {
                'column':'category_id',
                'value':3,
                'on':'name',
                'key':'Levadura',
                },

            ]
        )

    result = Product.return_all()

    assert result == [
        (1, 'Mantequilla', 10.5, 1),
        (2, 'Harina Trigo', 20.0, 2),
        (3, 'Huevo', 15.5, 3),
        (4, 'Levadura', 5.0, 3)
    ]

def test_update_by_id():
    Product.update(
        [
            {
            "column": 'price',
            "value": 18,
            "on": "product_id",
            "key": "1"
            }
        ]
    )

    result = Product.return_all()
    assert result[0] == (1, 'Mantequilla', 18.0, 1)

def test_delete():

    Product.write(
        [
            (None, 'Leche', 20.5, 1)
        ]
    )

    Product.delete(
        [
            {
            'on':'name',
            'key':'Mantequilla'
            }
        ],
        force = True
    )

    result = Product.return_all()

    assert result == [
        (2, 'Harina Trigo', 20.0, 2),
        (3, 'Huevo', 15.5, 3),
        (4, 'Levadura', 5.0, 3),
        (5, 'Leche', 20.5, 1)
    ]

def test_output():
    result, cols = SaleLine.pancakes(
        join=[
            {
            'join':'inner',
            'extra':'sale',
            'fkey':'sale_id',
            'origin':'sale_line',
            'id':'sale_id'
            },
            {
            'join':'inner',
            'extra':'client',
            'fkey':'client_id',
            'origin':'sale_line',
            'id':'client_id'
            },
            {
            'join':'left',
            'extra':'product',
            'fkey':'product_id',
            'origin':'sale_line',
            'id':'product_id'
            }
        ]
    )
    
    assert result == [
        (1, 1, 1, 2, 3.0, 1, 'S1', '2026-03-24', 1, 'Andres', 'Lopez', 2, 'Harina Trigo', 20.0, 2),
        (2, 1, 1, 3, 2.0, 1, 'S1', '2026-03-24', 1, 'Andres', 'Lopez', 3, 'Huevo', 15.5, 3),
        (3, 1, 1, 4, 5.0, 1, 'S1', '2026-03-24', 1, 'Andres', 'Lopez', 4, 'Levadura', 5.0, 3),
        (4, 1, 1, 1, 8.0, 1, 'S1', '2026-03-24', 1, 'Andres', 'Lopez', None, None, None, None)
    ]