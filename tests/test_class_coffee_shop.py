# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

""" Test CoffeeShop Class -*- PanCakesORM -*- """

# Modulos Propios
from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype
from pancakes.tool.idu import CoffeeShop
from pancakes.tool.box import QueryBox

# Modulos Python
from pathlib import Path
from datetime import date

# Modulos Desarrollo
import ipdb
import pandas as pd
import pytest

# GLOBAL PATH para pruebas
dir_ = Path.cwd() / 'data' / 'test_env'
file =  dir_ / 'coffee_shop.sqlite'

class User(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = 'user'

    name = sql_datatype.Char(comment='Usuario')

class Product(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = 'product'

    name = sql_datatype.Char(comment='Producto')

class UserDos(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = 'user_dos'

    name = sql_datatype.Char(comment='Usuario')
    user_id = sql_datatype.ForeignKey(
        second_table='user',
        column_id='user_id',
        comment="User User Dos Rel"
    )

class Category(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = 'category'

    name = sql_datatype.Char(comment='Unique Category')

# --*-- INSERT --*--
def test_method_i():
    coffee = CoffeeShop()

    box = QueryBox() # Esta manera de hacer query no funciona (debemos corregirla)

    coffee.i(db_path=file, user=[(None, "Andres")])
    api = User.all().to_dict()

    assert api == [{'user__user_id': 1, 'user__name': 'Andres'}]

def test_method_i_on_class():
    User.i(user=[(None, "Polar")])
    api = User.all().to_dict()

    assert api == [
    {'user__user_id': 1, 'user__name': 'Andres'},
    {'user__user_id': 2, 'user__name': 'Polar'}
    ]

# --*-- UPDATE --*--
def test_method_u_one_table_all():
    coffee = CoffeeShop()
    coffee.u(db_path=file, user__name="Po", update_all=True)
    api = User.all().to_dict()

    api == [
    {'user__user_id': 1, 'user__name': 'Po'},
    {'user__user_id': 2, 'user__name': 'Po'}
    ]

def test_method_u_one_table_multiple_rows():
    coffee = CoffeeShop()
    coffee.u(db_path=file, user__name__user_id__in=["Andres", [1, 2]])
    api = User.all().to_dict()

    api == [
    {'user__user_id': 1, 'user__name': 'Andres'},
    {'user__user_id': 2, 'user__name': 'Andres'}
    ]

def test_method_u_one_table_one_row():
    coffee = CoffeeShop()
    coffee.u(db_path=file, user__name__user_id__same=("Malteada", 1))
    api = User.all().to_dict()

    assert api == [
    {'user__user_id': 1, 'user__name': 'Malteada'},
    {'user__user_id': 2, 'user__name': 'Andres'}
    ]

# --*-- INSERT MULTIPLES TABLAS --*--
def test_method_i_multiple_tables():
    coffee = CoffeeShop()
    coffee.i(db_path=file, user=[(None, "Lupita")], product=[(None, "IPad")])

    res1 = User.all().to_dict()
    res2 = Product.all().to_dict()

    assert res1 == [
    {'user__user_id': 1, 'user__name': 'Malteada'},
    {'user__user_id': 2, 'user__name': 'Andres'},
    {'user__user_id': 3, 'user__name': 'Lupita'}
    ]
    assert res2 == [{'product__product_id': 1, 'product__name': 'IPad'}]

# --*-- UPDATE MULTIPLES TABLAS --*--
def test_method_u_multiple_tables():
    coffee = CoffeeShop()
    coffee.u(
        db_path=file,
        user__name__user_id__gtsm=("Guadalupe", 3),
        product__name__product_id__same=("Apple Ipad", 1))

    res1 = User.all().to_dict()
    res2 = Product.all().to_dict()

    assert res1 == [
    {'user__user_id': 1, 'user__name': 'Malteada'},
    {'user__user_id': 2, 'user__name': 'Andres'},
    {'user__user_id': 3, 'user__name': 'Guadalupe'}
    ]
    assert res2 == [{'product__product_id': 1, 'product__name': 'Apple Ipad'}]

# --*-- DELETE --*--
def test_method_d_one_table():
    coffee = CoffeeShop()
    row, col = User.filter(user__name__in=["Andres", "Guadalupe"]).id().all().raw(line_up=True)
    ids = row[0]
    coffee.d(db_path=file, user__user_id__in=ids)
    api = User.all().to_dict()

    assert api == [{'user__user_id': 1, 'user__name': 'Malteada'}]

def test_method_d_multi_table():
    coffee = CoffeeShop()
    coffee.d(
        db_path=file,
        user__user_id__same=1,
        product__product_id__same=1
    )
    res1 = User.all().to_dict()
    res2 = Product.all().to_dict()

    assert res1 == []
    assert res2 == []

# --*-- HELPERS COMO METODO() --*--
def test_classmethod_i():
    User.i(
        user=[(None, "Andres")],
        product=[(None, "MacBook")])

    res1 = User.all().to_dict()
    res2 = Product.all().to_dict()

    assert res1 == [{'user__user_id': 1, 'user__name': 'Andres'}]
    assert res2 == [{'product__product_id': 1, 'product__name': 'MacBook'}]

def test_classmethod_u():
    User.u(user__name__user_id__same=["Polar", 1])
    api = User.all().to_dict()

    assert api == [{'user__user_id': 1, 'user__name': 'Polar'}]

def test_classmethod_d():
    User.d(user__name__like="%olar")

    api = User.all().to_dict()

    assert api == []

# --*-- TEST NOMBRES REPETIDOS --*--

def test_duplicated_name_when_output():
    User.i(user=[(None, "Tabla1")])
    UserDos.i(user_dos=[(None, "Tabla1", 1)])

    row1, col1 = User.link('user_dos').all().raw()
    row2, col2 = User.link('user_dos').all().raw(label=True)
    dicc1 = User.link('user_dos').all().to_dict()
    dicc2 = User.link('user_dos').all().to_dict(label=True)
    api1 = User.link('user_dos').all().to_json()
    api2 = User.link('user_dos').all().to_json(label=True)
    
    assert col1 == ['user__user_id', 'user__name', 'user_dos__user_dos_id', 'user_dos__name', 'user_dos__user_id']
    assert col2 == ['USER ID 0', 'Usuario 1', 'USER_DOS ID 2', 'Usuario 3', 'User User Dos Rel 4']
    assert dicc1 == [{'user__user_id': 1, 'user__name': 'Tabla1', 'user_dos__user_dos_id': 1, 'user_dos__name': 'Tabla1', 'user_dos__user_id': 1}]
    assert dicc2 == [{'USER ID 0': 1, 'Usuario 1': 'Tabla1', 'USER_DOS ID 2': 1, 'Usuario 3': 'Tabla1', 'User User Dos Rel 4': 1}]
    assert api1 == {'user': {'user_id': [1], 'name': ['Tabla1']}, 'user_dos': {'user_dos_id': [1], 'name': ['Tabla1'], 'user_id': [1]}}
    assert api2 == {'user': {'USER ID 0': [1], 'Usuario 1': ['Tabla1']}, 'user_dos': {'USER_DOS ID 2': [1], 'Usuario 3': ['Tabla1'], 'User User Dos Rel 4': [1]}}

def test_error_queries_relaciones():
    dicc = Category.link('user').all().to_dict()
    assert dicc == []

def test_vacios():
    row, col = Category.all().raw()
    dicc = Category.all().to_dict()
    api = Category.all().to_json()

    assert row == []
    assert col == ['category__category_id', 'category__name']
    assert dicc == []
    assert api == {}
