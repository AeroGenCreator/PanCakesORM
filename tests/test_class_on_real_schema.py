# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at 
# http://www.apache.org/licenses/LICENSE-2.0

"""
Test Main Class -*- PanCakesORM -*- 

Este fichero testea las tablas de manera dinamica.
Cada ejecucion primera de un fichero '.py' evalua el atributo de clase
'cls._loop_validation'. A traves del metodo de clase
cls._which_loop(), si el atributo es False, se sincroniza la tabla en
tiempo real, de lo contrario, no se intenta una sincronizacion
manteniendo hacia el estado del esquema optimizado.
"""

from pancakes.models.model import PanCakesORM
from pancakes.sql import datatype
from pathlib import Path

import sqlite3
import pytest

import pandas as pd

DIR = Path.cwd() / 'data'
FILE = DIR / 'dynamic.sqlite'

# Creacion de tablas basado en dependencias.
class Product(PanCakesORM):
    _table = 'product'
    _db_dir = DIR
    _db_file = FILE
    _depends = ["category"]

    name = datatype.Char(comment="Product Name")
    category_id = datatype.ForeignKey(
        second_table="category",
        column_id="category_id",
        on_del="cascade",
        comment="Pro Cat Rel"
    )

class Category(PanCakesORM):
    _table = "category"
    _db_dir = DIR
    _db_file = FILE

    name = datatype.Char(comment="Category Name")

class SaleLine(PanCakesORM):
    _table = "sale_line"
    _db_dir = DIR
    _db_file = FILE
    _depends = ['product', 'category', 'client', 'sale']
    _group_constraint = ('product_id', 'category_id')

    product_id = datatype.ForeignKey(
        second_table="product",
        column_id="product_id",
        on_del="cascade",
        comment="Sale Line Pro Rel"
    )
    category_id = datatype.ForeignKey(
        second_table="category",
        column_id="category_id",
        on_del="cascade",
        comment="Sale Line Cat Rel"
    )
    client_id = datatype.ForeignKey(
        second_table="client",
        column_id="client_id",
        on_del="cascade",
        comment="Sale Line Cli Rel"
    )
    sale_id = datatype.ForeignKey(
        second_table="sale",
        column_id="sale_id",
        on_del="cascade",
        comment="Sale Line Sale Rel"
    )
    quantity = datatype.Float(comment="Quantity")

class Client(PanCakesORM):
    _table = "client"
    _db_dir = DIR
    _db_file = FILE

    name = datatype.Char(comment="Client Name")

class Sale(PanCakesORM):
    _table = "sale"
    _db_dir = DIR
    _db_file = FILE

    name = datatype.Char(comment="Sale Code")

Product.i(product=[(None, "Pan", None)])
Category.i(category=[(None, "Panaderia")])

def test_dependenncies():
    
    assert Product.table_exists() == True
    assert Category.table_exists() == True
    assert SaleLine.table_exists() == True
    assert Client.table_exists() == True
    assert Sale.table_exists() == True

def test_group_constraint():
    SaleLine.i(sale_line=[(None, 1, 1, None, None, None)])
    with pytest.raises(sqlite3.IntegrityError):
        SaleLine.i(sale_line=[(None, 1, 1, None, None, None)])

def test_sinchronized_schema():

    class Product2(Product):
        _table = 'product'
        _db_dir = DIR
        _db_file = FILE
        _depends = ["category"]

        name = datatype.Char(comment="Product Name")

    api = Product2.all().to_dict()
    
    # OJO: Ya no existe la columna category_id (La sincronizacion funciona)
    assert api == [{'product__product_id': 1, 'product__name': 'Pan'}]

def test_quitar_group_constraint():
    
    class SaleLine2(SaleLine):
        _table = "sale_line"
        _db_dir = DIR
        _db_file = FILE
        _depends = ['product', 'category', 'client', 'sale']

        product_id = datatype.ForeignKey(
            second_table="product",
            column_id="product_id",
            on_del="cascade",
            comment="rel1"
        )
        category_id = datatype.ForeignKey(
            second_table="category",
            column_id="category_id",
            on_del="cascade",
            comment="rel2"
        )
        client_id = datatype.ForeignKey(
            second_table="client",
            column_id="client_id",
            on_del="cascade",
            comment="rel3"
        )
        sale_id = datatype.ForeignKey(
            second_table="sale",
            column_id="sale_id",
            on_del="cascade",
            comment="rel4"
        )
        quantity = datatype.Float(comment="Quantity")

    SaleLine2.i(sale_line=[(None, 1, 1, None, None, None)])

    api = SaleLine2.all().to_dict()

    # Quito el constraint de manera dinamica, la base de datos sola lo interpreta
    assert api == [
    {'sale_line__sale_line_id': 1, 'sale_line__product_id': 1, 'sale_line__category_id': 1, 'sale_line__client_id': None, 'sale_line__sale_id': None, 'sale_line__quantity': None},
    {'sale_line__sale_line_id': 2, 'sale_line__product_id': 1, 'sale_line__category_id': 1, 'sale_line__client_id': None, 'sale_line__sale_id': None, 'sale_line__quantity': None}]