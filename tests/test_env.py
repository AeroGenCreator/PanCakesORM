# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""Test QueryBox Class -*- PanCakesORM -*-"""

# Modulos Propios
from pancakes.abstract.abstract_box import AbstractBox
from pancakes.abstract.query_box import QueryBox
from pancakes.models.model import PanCakesORM
from pancakes.orm.delete import delete
from pancakes.orm.insert import insert
from pancakes.orm.query import query
from pancakes.orm.update import update
from pancakes.sql import datatype


class User(PanCakesORM):
    name = datatype.Char(comment="User Name")
    _table = "user"


def test_env_query():
    row, col = query(select="*", _from="user")

    assert row == []
    assert col == ["user_id", "name"]


def test_env_insert():
    insert([{"table": "user", "data": [(None, "User1")]}])
    row, col = query(select="*", _from="user")
    assert len(row) == 1


def test_env_delete():
    delete([{"table": "user"}], delete_all=True)
    row, col = query(select="*", _from="user")
    assert row == []


def test_env_update():
    insert([{"table": "user", "data": [(None, "User2")]}])
    update(
        [
            {
                "table": "user",
                "name": "name",
                "data": "PanCakesORM",
                "condition": [
                    {
                        "column": "name",
                        "operator": "in",
                        "value": ["User2"],
                    }
                ],
            }
        ]
    )
    row, col = query(select="*", _from="user")

    assert row == [(1, "PanCakesORM")]


def test_coffee_insert():
    AbstractBox(model=PanCakesORM).i(user=[(None, "AbstractBox")])
    dicc = User.all().to_dict()

    assert dicc == [
        {"user__user_id": 1, "user__name": "PanCakesORM"},
        {"user__user_id": 2, "user__name": "AbstractBox"},
    ]


def test_coffee_update():
    AbstractBox(model=PanCakesORM).u(
        user__name__user_id__same=("PanCakesORM & AbstractBox & QueryBox", 2)
    )
    dicc = User.all().to_dict()

    assert dicc == [
        {"user__user_id": 1, "user__name": "PanCakesORM"},
        {
            "user__user_id": 2,
            "user__name": "PanCakesORM & AbstractBox & QueryBox",
        },
    ]


def test_coffee_delete():
    AbstractBox(model=PanCakesORM).d(user__user_id__same=1)
    dicc = User.all().to_dict()

    assert dicc == [
        {
            "user__user_id": 2,
            "user__name": "PanCakesORM & AbstractBox & QueryBox",
        }
    ]


def test_querybox():
    """
    QueryBox Puede ser global si lo instanciamos de la siguiente manera.
    Esto debe ser forzoso, es la unica manera de no perder columnas
    con las salidas
    """
    query_user = QueryBox(User)
    dicc = query_user.select().all(_from="user").to_dict()

    assert dicc == [
        {
            "user__user_id": 2,
            "user__name": "PanCakesORM & AbstractBox & QueryBox",
        }
    ]
