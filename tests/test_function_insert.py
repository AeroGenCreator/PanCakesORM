# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at 
# http://www.apache.org/licenses/LICENSE-2.0

""" Test Main Class -*- PanCakesORM -*- """

# Modulos Propios
from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype
from pancakes.cook.furnace import insert

# Modulos Python
from pathlib import Path
from datetime import date

# Modulos Desarrollo
import ipdb
import pandas as pd

# GLOBAL PATH para pruebas
dir_ = Path.cwd() / 'data' / 'test_env'
file =  dir_ / 'insert.sqlite'

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

def test_insert_one_table_one_tuple():
    insert(
        db_path=file,
        params=[
            {
            'table':'category',
            'data':[
                    (None, "Lacteos")
                ]
            }
        ]
    )

    res = Category.return_all()
    assert res == [(1, 'Lacteos')]

def test_insert_two_tables_one_tuple():
    insert(
        db_path=file,
        params=[
            {
            'table':'category',
            'data':[
                    (None, "Harinas")
                ]
            },
            {
            'table':'product',
            'data':[
                    (None, 'Leche', 25.5, 1)
                ]
            }
        ]
    )

    res1 = Category.return_all()
    res2 = Product.return_all()

    assert res1 == [(1, 'Lacteos'),(2, 'Harinas')]
    assert res2 == [(1, 'Leche', 25.5, 1)]

def test_insert_one_table_multiple_tuples():
    insert(
        db_path=file,
        params=[
            {
            'table':'category',
            'data':[
                    (None, "Semillas"),
                    (None, "Extras"),
                    (None, "Moldes")
                ]
            }
        ]
    )

    res = Category.return_all()
    assert res == [
        (1, 'Lacteos'),
        (2, 'Harinas'),
        (3, 'Semillas'),
        (4, 'Extras'),
        (5, 'Moldes')
    ]

def test_insert_multiple_tables_multiple_tuples():
    insert(
        db_path=file,
        params=[
            {
            'table':'client',
            'data':[
                    (None, "Andres", "Lopez"),
                    (None, "Polar", "Poo")
                ]
            },
            {
            'table':'product',
            'data':[
                    (None, 'Mantequilla', 20, 1),
                    (None, 'Capacillo', 0.50, 5),
                    (None, 'Ajonjoli', 15, 3),
                    (None, 'Chispas Chocolate', 20, 4),
                    (None, 'Harina Trigo', 20, 2),
                ]
            },
            {
            'table':'sale',
            'data':[
                    (None, 'F01', '2026-03-27')
                ]
            }
        ]
    )

    res1 = Client.return_all()
    res2 = Product.return_all()
    res3 = Sale.return_all()

    assert res1 == [(1, 'Andres', 'Lopez'), (2, 'Polar', 'Poo')]
    assert res2 == [
        (1, 'Leche', 25.5, 1),
        (2, 'Mantequilla', 20.0, 1),
        (3, 'Capacillo', 0.5, 5),
        (4, 'Ajonjoli', 15.0, 3),
        (5, 'Chispas Chocolate', 20.0, 4),
        (6, 'Harina Trigo', 20.0, 2)
    ]
    assert res3 == [(1, 'F01', '2026-03-27')]
