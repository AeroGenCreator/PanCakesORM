# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""Test QueryBox Class -*- PanCakesORM -*-"""

# Modulos Propios
# Modulos Python
from pathlib import Path

from pancakes.abstract.querybox import QueryBox
from pancakes.models.model import PanCakesORM
from pancakes.orm.insert import insert
from pancakes.sql import datatype

# GLOBAL PATH para pruebas
dir_ = Path.cwd() / "data" / "test_env"
file = dir_ / "querybox_renewed.sqlite"


class Country(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = "country"
    _depends = "self"

    name = datatype.Char(comment="Country")


class Client(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = "client"
    _depends = ["country"]

    name = datatype.Char(comment="Client Name")
    country_id = datatype.ForeignKey(
        second_table="country",
        column_id="country_id",
        comment="Country Rel",
        on_del="cascade",
        on_upd="cascade",
    )


class Sale(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = "sale"
    _depends = ["client"]

    name = datatype.Char(comment="Sale Code")
    client_id = datatype.ForeignKey(
        second_table="client",
        column_id="client_id",
        comment="Cliente Rel",
        on_del="cascade",
        on_upd="cascade",
    )


insert(
    db_path=file,
    params=[
        {"table": "country", "data": [(None, "Mexico"), (None, "Brasil")]},
        {
            "table": "client",
            "data": [
                (None, "Andres", 1),
                (None, "Lupita", 1),
                (None, "Peke", 2),
                (None, "Polar", 1),
                (None, "Malteada", 2),
            ],
        },
        {
            "table": "sale",
            "data": [
                (None, "F1", 1),
                (None, "F2", 3),
                (None, "F3", 4),
                (None, "F4", 1),
                (None, "F5", 3),
                (None, "F6", 3),
                (None, "F7", 5),
                (None, "F8", 3),
                (None, "F9", 2),
            ],
        },
    ],
)

q_country = QueryBox(Country)
q_client = QueryBox(Client)
q_sale = QueryBox(Sale)

""" PRUEBAS CON EL NUEVO MOTOR DE QUERY DECLARATIVO """


def test_select_uno():
    row, col = q_country.select("country__country_id").all()

    assert row == [(1,), (2,)]
    assert col == ["country__country_id"]


def test_select_varios():
    row, col = q_country.select("country__country_id", "country__name").all()

    assert row == [(1, "Mexico"), (2, "Brasil")]
    assert col == ["country__country_id", "country__name"]


def test_select_uno_agg():
    row, col = q_client.select("client__client_id__count").all()

    assert row == [(5,)]
    assert col == ["client__client_id__count"]


def test_select_varios_agg():
    row, col = q_client.select(
        "client__client_id__count", "client__client_id__sum"
    ).all()

    assert row == [(5, 15)]
    assert col == ["client__client_id__count", "client__client_id__sum"]


def test_no_select():
    row, col = q_sale.select().all()

    assert row == [
        (1, "F1", 1),
        (2, "F2", 3),
        (3, "F3", 4),
        (4, "F4", 1),
        (5, "F5", 3),
        (6, "F6", 3),
        (7, "F7", 5),
        (8, "F8", 3),
        (9, "F9", 2),
    ]
    assert col == ["sale__sale_id", "sale__name", "sale__client_id"]


def test_no_select_link_3():
    row, col = q_sale.link("lf__client", "lf__country").all()

    print(row)
    print(col)

def test_no_select_link_2():
    row, col = q_client.link("lf__sale").all()

    print(row)
    print(col)
