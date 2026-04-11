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

# --*-- INSERT --*--
def test_method_i():
    coffee = CoffeeShop()
    box = QueryBox()

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