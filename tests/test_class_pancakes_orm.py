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
    _depends = ["category"]

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
    _depends = ["client", "sale", "product"]

    client_id = sql_datatype.ForeignKey(
            second_table='client',
            column_id = 'client_id',
            comment="Rel1"
        )
    sale_id = sql_datatype.ForeignKey(
            second_table='sale',
            column_id='sale_id',
            comment="Rel2"
        )
    product_id = sql_datatype.ForeignKey(
            second_table='product',
            column_id='product_id',
            comment="Rel3"
        )
    amount = sql_datatype.Float(comment='Cantidad', nls=False)

def test_insert():
    Category.insert(
        params=[
            {
            'table':'category',
            'data':[
                    (None, 'Lacteos'),
                    (None, 'Harinas'),
                    (None, 'Extras')
                ]
            },
            {
            'table':'product',
            'data':[
                    (None, 'Mantequilla', 10.5, None),
                    (None, 'Harina Trigo', 20, None),
                    (None, 'Huevo', 15.5, None),
                    (None, 'Levadura', 5, None)
                ]
            },
            {
            'table':'client',
            'data':[
                    (None, 'Andres', 'Lopez'),
                    (None, 'Polar', 'Poo')
                ]
            },
            {
            'table':'sale',
            'data':[(None, 'S1', '2026-03-28')]
            },
            {
            'table':'sale_line',
            'data':[
                    (None, 1, 1, 2, 3),
                    (None, 1, 1, 3, 2),
                    (None, 1, 1, 4, 5),
                    (None, 1, 1, 1, 8)
                ]
            }
        ]
    )
    
    res1 = Category.return_all()
    res2 = Product.return_all()
    res3 = Client.return_all()
    res4 = Sale.return_all()
    res5 = SaleLine.return_all()

    assert res1 == [
    (1, 'Lacteos'),
    (2, 'Harinas'),
    (3, 'Extras')
    ]
    assert res2 == [
    (1, 'Mantequilla', 10.5, None),
    (2, 'Harina Trigo', 20.0, None),
    (3, 'Huevo', 15.5, None),
    (4, 'Levadura', 5.0, None)
    ]
    assert res3 == [
    (1, 'Andres', 'Lopez'),
    (2, 'Polar', 'Poo')
    ]
    assert res4 == [(1, 'S1', '2026-03-28')]
    assert res5 == [
    (1, 1, 1, 2, 3.0),
    (2, 1, 1, 3, 2.0), 
    (3, 1, 1, 4, 5.0), 
    (4, 1, 1, 1, 8.0)
    ]

def test_update():
    Category.update(
        params=[
            {
            'table':'product',
            'name':'category_id',
            'data':3,
            'condition':[
                    {
                    'column':'product_id',
                    'operator':'in',
                    'value':[2,3],
                    }
                ]
            },
            {
            'table':'product',
            'name':'category_id',
            'data':2,
            'condition':[
                    {
                    'column':'product_id',
                    'operator':'=',
                    'value':1,
                    }
                ]
            },
            {
            'table':'product',
            'name':'category_id',
            'data':1,
            'condition':[
                    {
                    'column':'product_id',
                    'operator':'=',
                    'value':4,
                    }
                ]
            }
        ]
    )
    result = Product.return_all()

    assert result == [
        (1, 'Mantequilla', 10.5, 2),
        (2, 'Harina Trigo', 20.0, 3),
        (3, 'Huevo', 15.5, 3),
        (4, 'Levadura', 5.0, 1)
    ]
