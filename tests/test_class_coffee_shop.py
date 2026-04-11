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






