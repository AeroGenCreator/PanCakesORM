# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

""" Test QueryBox Class -*- PanCakesORM -*- """

# Modulos Propios
from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype
from pancakes.cook.layer import query
from pancakes.cook.furnace import insert
from pancakes.cook.clean import delete
from pancakes.cook.ingredient import update

# Modulos Desarrollo
import ipdb
import pandas as pd

class User(PanCakesORM):
    _table = "user"

    name = sql_datatype.Char(comment="User Name")

def test_env_query():
    row, col = query(select="*", _from="user")
    
    assert row == []
    assert col == ['user_id', 'name']

def test_env_insert():
    insert([
        {"table": "user", "data": [(None, "User1")]}
    ])
    row, col = query(select="*", _from="user")
    assert len(row) == 1

def test_env_delete():
    delete([{"table": "user"}], delete_all=True)
    row, col = query(select="*", _from="user")
    assert row == []

def test_env_update():
    insert([
        {"table": "user", "data": [(None, "User2")]}
    ])
    update([{
        "table": "user",
        "name": "name",
        "data": "PanCakesORM",
        "condition": [
            {
                "column": "name",
                "operator": "in",
                "value": ["User2"],
            }
        ] 
    }])
    row, col = query(select="*", _from="user")

    assert row == [(1, 'PanCakesORM')]