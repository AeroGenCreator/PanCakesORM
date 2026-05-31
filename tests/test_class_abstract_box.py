# -*- coding: utf-8 -*-
# PanCakesORM v5.0.0 | Test Suite
# Copyright (c) 2026 AeroGenCreator (https://github.com/AeroGenCreator)
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
# ==============================================================================

"""Test AbstractBox Class -*- PanCakesORM -*-"""

# Modulos Python
from pathlib import Path

from pancakes.abstract.abstract_box import AbstractBox
from pancakes.models.model import PanCakesORM
from pancakes.sql import datatype

# GLOBAL PATH para pruebas
dir_ = Path.cwd() / "data" / "test_env"
file = dir_ / "abs_box_shop.sqlite"


class User(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = "user"

    name = datatype.Char(comment="Usuario")


class Product(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = "product"

    name = datatype.Char(comment="Producto")


class UserDos(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = "user_dos"

    name = datatype.Char(comment="Usuario")
    user_id = datatype.ForeignKey(
        second_table="user", column_id="user_id", comment="User User Dos Rel"
    )


class Category(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = "category"

    name = datatype.Char(comment="Unique Category")


# --*-- INSERT --*--
def test_method_i():
    abs_box = AbstractBox(model=User)

    abs_box.i(db_path=file, user=[(None, "Andres")])
    api = User.all().dictionary()
    
    assert api == [{"user__user_id": 1, "user__name": "Andres"}]


def test_method_i_on_class():
    User.i(user=[(None, "Polar")])
    api = User.all().dictionary()

    assert api == [
        {"user__user_id": 1, "user__name": "Andres"},
        {"user__user_id": 2, "user__name": "Polar"},
    ]


# --*-- UPDATE --*--
def test_method_u_same():
    abs_box = AbstractBox(model=User)
    abs_box.u(user__name__user_id__same=["Po", 1])
    api = User.all().dictionary()

    assert api == [{'user__user_id': 1, 'user__name': 'Po'}, {'user__user_id': 2, 'user__name': 'Polar'}]

def test_method_u_between():
    User.u(user__name__user_id__btwn=["Andres", [1,2]])
    api = User.all().dictionary()

    assert api == [{'user__user_id': 1, 'user__name': 'Andres'}, {'user__user_id': 2, 'user__name': 'Andres'}]

def test_method_u_one_table_multiple_rows():
    abs_box = AbstractBox(model=User)
    abs_box.u(user__name__user_id__in=["PanCakesORM", [1, 2]])
    api = User.all().dictionary()

    api == [
        {"user__user_id": 1, "user__name": "PanCakesORM"},
        {"user__user_id": 2, "user__name": "PanCakesORM"},
    ]


def test_method_u_one_table_one_row():
    abs_box = AbstractBox(model=User)
    abs_box.u(user__name__user_id__same=("Malteada", 1))
    api = User.all().dictionary()

    assert api == [
        {"user__user_id": 1, "user__name": "Malteada"},
        {"user__user_id": 2, "user__name": "PanCakesORM"},
    ]


# --*-- INSERT MULTIPLES TABLAS --*--
def test_method_i_multiple_tables():
    abs_box = AbstractBox(model=User)
    abs_box.i(db_path=file, user=[(None, "Lupita")], product=[(None, "IPad")])

    res1 = User.all().dictionary()
    res2 = Product.all().dictionary()

    assert res1 == [
        {"user__user_id": 1, "user__name": "Malteada"},
        {"user__user_id": 2, "user__name": "PanCakesORM"},
        {"user__user_id": 3, "user__name": "Lupita"},
    ]
    assert res2 == [{"product__product_id": 1, "product__name": "IPad"}]


# --*-- UPDATE MULTIPLES TABLAS --*--
def test_method_u_multiple_tables():
    abs_box = AbstractBox(model=User)
    abs_box.u(
        user__name__user_id__gtsm=("Guadalupe", 3),
        product__name__product_id__same=("Apple Ipad", 1),
    )

    res1 = User.all().dictionary()
    res2 = Product.all().dictionary()

    assert res1 == [
        {"user__user_id": 1, "user__name": "Malteada"},
        {"user__user_id": 2, "user__name": "PanCakesORM"},
        {"user__user_id": 3, "user__name": "Guadalupe"},
    ]
    assert res2 == [{"product__product_id": 1, "product__name": "Apple Ipad"}]


# --*-- DELETE --*--
def test_method_d_one_table():
    abs_box = AbstractBox(model=User)
    row, col = (
        User.filter(user__name__in=["PanCakesORM", "Guadalupe"])
        .all(ids=True)
        .raw(align=True)
    )
    ids = row[0]
    abs_box.d(db_path=file, user__user_id__in=ids)
    api = User.all().dictionary()

    assert api == [{"user__user_id": 1, "user__name": "Malteada"}]


def test_method_d_multi_table():
    abs_box = AbstractBox(model=User)
    abs_box.d(db_path=file, user__user_id__same=1, product__product_id__same=1)
    res1 = User.all().dictionary()
    res2 = Product.all().dictionary()

    assert res1 == [{"user__user_id": None, "user__name": None}]
    assert res2 == [{"product__product_id": None, "product__name": None}]


# --*-- HELPERS COMO METODO() --*--
def test_classmethod_i():
    User.i(user=[(None, "Andres")], product=[(None, "MacBook")])

    res1 = User.all().dictionary()
    res2 = Product.all().dictionary()

    assert res1 == [{"user__user_id": 1, "user__name": "Andres"}]
    assert res2 == [{"product__product_id": 1, "product__name": "MacBook"}]


def test_classmethod_u():
    User.u(user__name__user_id__same=["Polar", 1])
    api = User.all().dictionary()

    assert api == [{"user__user_id": 1, "user__name": "Polar"}]


def test_classmethod_d():
    User.d(user__name__like="%olar")

    api = User.all().dictionary()

    assert api == [{"user__user_id": None, "user__name": None}]


# --*-- TEST NOMBRES REPETIDOS --*--

def test_duplicated_name_when_output():
    User.i(user=[(None, "Tabla1")])
    UserDos.i(user_dos=[(None, "Tabla1", 1)])

    row1, col1 = User.link("user_dos").all().raw()
    row2, col2 = User.link("user_dos").all().raw(label=True)
    dicc1 = User.link("user_dos").all().dictionary()
    dicc2 = User.link("user_dos").all().dictionary(label=True)
    api1 = User.link("user_dos").all().container()

    assert col1 == [
        "user__user_id",
        "user__name",
        "user_dos__user_dos_id",
        "user_dos__name",
        "user_dos__user_id",
    ]
    assert col2 == [
        "user__user_id__0",
        "user__name__1",
        "user_dos__user_dos_id__2",
        "user_dos__name__3",
        "user_dos__user_id__4",
    ]
    assert dicc1 == [
        {
            "user__user_id": 1,
            "user__name": "Tabla1",
            "user_dos__user_dos_id": 1,
            "user_dos__name": "Tabla1",
            "user_dos__user_id": 1,
        }
    ]
    assert dicc2 == [
        {
            "user__user_id__0": 1,
            "user__name__1": "Tabla1",
            "user_dos__user_dos_id__2": 1,
            "user_dos__name__3": "Tabla1",
            "user_dos__user_id__4": 1,
        }
    ]

    assert api1 == {
        "user": {
            "@main_table@": True,
            "@depends@": ["self"],
            "user_id": {
                "vector": [1],
                "label": "USER ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "INTEGER",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": ["Tabla1"],
                "label": "Usuario",
                "position": 1,
                "readonly": False,
                "default": None,
                "required": False,
                "python_type": "str",
                "primary_key": False,
                "sql_type": "VARCHAR",
                "second_table": False,
                "foreign_key": False,
            },
        },
        "user_dos": {
            "@main_table@": False,
            "@depends@": ["self"],
            "user_dos_id": {
                "vector": [1],
                "label": "USER_DOS ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "INTEGER",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": ["Tabla1"],
                "label": "Usuario",
                "position": 1,
                "readonly": False,
                "default": None,
                "required": False,
                "python_type": "str",
                "primary_key": False,
                "sql_type": "VARCHAR",
                "second_table": False,
                "foreign_key": False,
            },
            "user_id": {
                "vector": [1],
                "label": "User User Dos Rel",
                "position": 2,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": False,
                "sql_type": "FOREIGN KEY",
                "second_table": "user",
                "foreign_key": "user_id",
            },
        },
    }


def test_error_queries_relaciones():
    dicc = Category.link("user").all().dictionary()
    assert dicc == [{"category__category_id": None, "category__name": None}]

def test_vacios():
    row, col = Category.all().raw()
    dicc = Category.all().dictionary()
    api = Category.all().container()

    assert row == []
    assert col == ["category__category_id", "category__name"]
    assert dicc == [{"category__category_id": None, "category__name": None}]
    assert api == {
        "category": {
            "@main_table@": True,
            "@depends@": ["self"],
            "category_id": {
                "vector": [None],
                "label": "CATEGORY ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "INTEGER",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": [None],
                "label": "Unique Category",
                "position": 1,
                "readonly": False,
                "default": None,
                "required": False,
                "python_type": "str",
                "primary_key": False,
                "sql_type": "VARCHAR",
                "second_table": False,
                "foreign_key": False,
            },
        }
    }
