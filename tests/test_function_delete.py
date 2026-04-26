# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at 
# http://www.apache.org/licenses/LICENSE-2.0

""" Test Delete -*- PanCakesORM -*- """

# Modulos Propios
from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype
from pancakes.cook.furnace import insert
from pancakes.cook.clean import delete

# Modulos Python
from pathlib import Path
from datetime import datetime

# Modulos Desarrollo
import ipdb
import pandas as pd

# GLOBAL PATH para pruebas
dir_path = Path.cwd() / 'data' / 'test_env'
file_path =  dir_path / 'delete.sqlite'

# Crear una tablas:
class Category(PanCakesORM):
    _table = 'category'
    _db_dir = dir_path
    _db_file = file_path

    name = sql_datatype.Char(comment='Categoria', required=False)

class Product(PanCakesORM):
    _table = 'product'
    _db_dir = dir_path
    _db_file = file_path

    name = sql_datatype.Char(comment='Producto', required=False)
    price = sql_datatype.Float(comment='Precio', required=False)
    category_id = sql_datatype.ForeignKey(
            second_table = 'category',
            column_id = 'category_id',
            comment = 'Categoria'
        )

class Client(PanCakesORM):
    _table = 'client'
    _db_dir = dir_path
    _db_file = file_path

    name = sql_datatype.Char(comment='Nombre', required=False)
    surname = sql_datatype.Char(comment='Apellido', required=False)

class SaleLine(PanCakesORM):
    _table = 'sale_line'
    _db_dir = dir_path
    _db_file = file_path

    client_id = sql_datatype.ForeignKey(
            second_table='client',
            column_id = 'client_id',
            comment="Rel1"
        )
    product_id = sql_datatype.ForeignKey(
            second_table='product',
            column_id='product_id',
            on_del='cascade',
            comment="Rel2"
        )

insert(
    db_path=file_path,
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
        }
    ]
)

def test_delete_one_row_ind_condition():
    delete(
        db_path=file_path,
        params=[
            {
            'table':'product',
            'condition':[
                    {
                    'column':'name',
                    'operator':'=',
                    'value':'Levadura',
                    'logic':'',
                    }
                ]
            }
        ]
    )

    res = Product.return_all()
    
    assert res == [
        (1, 'Mantequilla', 10.5, None),
        (2, 'Harina Trigo', 20.0, None),
        (3, 'Huevo', 15.5, None)
    ]

def test_delete_one_row_iter_condition():
    delete(
        db_path=file_path,
        params=[
            {
            'table':'client',
            'condition':[
                    {
                    'column':'surname',
                    'operator':'in',
                    'value':('Poo', 'Lopez')
                    }
                ]
            }
        ]
    )

    res = Client.return_all()

    assert res == []

def test_delete_one_row_between_condition():
    delete(
        db_path=file_path,
        params=[
            {
            'table':'category',
            'condition':[
                    {
                    'column':'category_id',
                    'operator':'between',
                    'value':[1,3]
                    }
                ]
            }
        ]
    )

    res = Category.return_all()

    assert res == []

def test_new_data():
    insert(
        db_path=file_path,
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
            }
        ]
    )

def test_delete_multi_table_one_row():
    delete(
        db_path=file_path,
        params=[
            {
            'table':'product',
            'condition':[
                    {
                    'column':'product_id',
                    'operator':'=',
                    'value':4
                    }
                ]
            },
            {
            'table':'client',
            'condition':[
                    {
                    'column':'client_id',
                    'operator':'>=',
                    'value':1
                    }
                ]
            },
        ]
    )

    res1 = Product.return_all()
    res2 = Client.return_all()

    assert res1 == [
        (1, 'Mantequilla', 10.5, None),
        (2, 'Harina Trigo', 20.0, None),
        (3, 'Huevo', 15.5, None),
        (5, 'Harina Trigo', 20.0, None),
        (6, 'Huevo', 15.5, None),
        (7, 'Levadura', 5.0, None)
    ]
    assert res2 == []

def test_delete_multiple_condition():
    delete(
        db_path=file_path,
        params=[
            {
            'table':'product',
            'condition':[
                    {
                    'column':'name',
                    'operator':'=',
                    'value':'Huevo',
                    'logic':'AND'
                    },
                    {
                    'column':'product_id',
                    'operator':'>',
                    'value':3
                    }
                ]
            }
        ]
    )

    res = Product.return_all()

    assert res == [
        (1, 'Mantequilla', 10.5, None),
        (2, 'Harina Trigo', 20.0, None),
        (3, 'Huevo', 15.5, None),
        (5, 'Harina Trigo', 20.0, None),
        (7, 'Levadura', 5.0, None)
    ]

def test_delete_all_multiple_table():
    delete(
        db_path=file_path,
        params=[
            {
            'table':'category',
            },
            {
            'table':'product',
            },
        ],
        delete_all=True
    )

    res1 = Category.return_all()
    res2 = Product.return_all()

    assert res1 == []
    assert res2 == []

def test_new_relational_data():
    insert(
        db_path=file_path,
        params=[
            {
            'table':'client',
            'data':[
                    (None, 'Andres', 'Lopez')
                ]
            },
            {
            'table':'product',
            'data':[
                    (None, 'Leche', 25, None)
                ]
            },
            {
            'table':'sale_line',
            'data':[
                    (None,1,1)
                ]
            },
        ]
    )
    res1 = Client.return_all()
    res2 = Product.return_all() 
    res3 = SaleLine.return_all() 

    assert res1 == [(1, 'Andres', 'Lopez')]
    assert res2 == [(1, 'Leche', 25.0, None)]
    assert res3 == [(1, 1, 1)]

def test_delete_relationals_by_cascade():
    delete(
        db_path=file_path,
        params=[
            {
            'table':'product',
            'condition':[
                    {
                    'column':'product_id',
                    'operator':'=',
                    'value':1,
                    }
                ]
            }
        ]
    )

    res1 = Product.return_all()
    res2 = Client.return_all()
    res3 = SaleLine.return_all()

    assert res1 == []
    assert res2 == [(1, 'Andres', 'Lopez')]
    assert res3 == []

def test_delete_relationals_by_force():
    insert(
        db_path=file_path,
        params=[
            {
            'table':'product',
            'data':[
                    (None, 'Chocolate', 30, None)
                ]
            },
            {
            'table':'sale_line',
            'data':[
                (None, 1, 1)
                ]
            }
        ]
    )

    delete(
        db_path=file_path,
        params=[
            {
            'table':'client',
            'condition':[
                    {
                    'column':'client_id',
                    'operator':'=',
                    'value':1
                    }
                ]
            }
        ],
        force=True
    )

    res = Client.return_all()

    assert res == []