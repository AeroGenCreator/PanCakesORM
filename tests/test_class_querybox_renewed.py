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


def test_no_select_add_3():
    row, col = q_sale.add(
        sale__left__client="client_id", client__right__country="country_id"
    ).all()

    assert row == [
        (1, "F1", 1, 1, "Andres", 1, 1, "Mexico"),
        (2, "F2", 3, 3, "Peke", 2, 2, "Brasil"),
        (3, "F3", 4, 4, "Polar", 1, 1, "Mexico"),
        (4, "F4", 1, 1, "Andres", 1, 1, "Mexico"),
        (5, "F5", 3, 3, "Peke", 2, 2, "Brasil"),
        (6, "F6", 3, 3, "Peke", 2, 2, "Brasil"),
        (7, "F7", 5, 5, "Malteada", 2, 2, "Brasil"),
        (8, "F8", 3, 3, "Peke", 2, 2, "Brasil"),
        (9, "F9", 2, 2, "Lupita", 1, 1, "Mexico"),
    ]

    assert col == [
        "sale__sale_id",
        "sale__name",
        "sale__client_id",
        "client__client_id",
        "client__name",
        "client__country_id",
        "country__country_id",
        "country__name",
    ]


def test_no_select_add_2():
    row, col = q_country.add(country__left__client="country_id").all()

    assert row == [
        (1, "Mexico", 1, "Andres", 1),
        (1, "Mexico", 2, "Lupita", 1),
        (1, "Mexico", 4, "Polar", 1),
        (2, "Brasil", 5, "Malteada", 2),
        (2, "Brasil", 3, "Peke", 2),
    ]
    assert col == [
        "country__country_id",
        "country__name",
        "client__client_id",
        "client__name",
        "client__country_id",
    ]


def test_select_special_add():
    row, col = (
        q_sale.select("sale__name", "client__name", "country__name")
        .add(
            sale__right__client="client_id", client__right__country="country_id"
        )
        .all()
    )

    assert row == [
        ("F1", "Andres", "Mexico"),
        ("F2", "Peke", "Brasil"),
        ("F3", "Polar", "Mexico"),
        ("F4", "Andres", "Mexico"),
        ("F5", "Peke", "Brasil"),
        ("F6", "Peke", "Brasil"),
        ("F7", "Malteada", "Brasil"),
        ("F8", "Peke", "Brasil"),
        ("F9", "Lupita", "Mexico"),
    ]
    assert col == ["sale__name", "client__name", "country__name"]


def test_add_3_filter():
    # Ejemplo Many2One para el pais de Malteada
    row, col = (
        q_sale.select("client__name", "country__name")
        .add(
            sale__right__client="client_id", client__right__country="country_id"
        )
        .filter("client__name__like__'%alteada'")
        .all()
    )

    assert row == [("Malteada", "Brasil")]
    assert col == ["client__name", "country__name"]


def test_filter_strings():
    row, col = (
        q_client.select("client__client_id", "client__name")
        .filter("client__name__=__'Malteada'")
        .all()
    )

    assert row == [(5, "Malteada")]
    assert col == ["client__client_id", "client__name"]


def test_seleccion_exacta():
    m = QueryBox(model=Country)
    row, col = (
        m.select("client__name")
        .add(client__inner__country="country_id")
        .filter("client__name__=__'Malteada'")
        .all()
    )

    assert row == [("Malteada",)]
    assert col == ["client__name"]


def test_filter_iterables():
    m = QueryBox(model=Country)
    row, col = (
        m.select("client__name")
        .add(client__inner__country="country_id")
        .filter(
            "client__name__in__['Malteada', 'Polar']@&&@client__client_id__=__5"
        )
        .all()
    )
    assert row == [('Malteada',)]
    assert col == ['client__name']


def test_add_full_control():
    m = QueryBox(model=Country)
    row, col = (
        m.select(
            "client__client_id", "client__name", "sale__name", "country__name"
        )
        .add(
            client__inner__country="country_id", client__inner__sale="client_id"
        )
        .all()
    )

    assert row == [
        (1, "Andres", "F1", "Mexico"),
        (3, "Peke", "F2", "Brasil"),
        (4, "Polar", "F3", "Mexico"),
        (1, "Andres", "F4", "Mexico"),
        (3, "Peke", "F5", "Brasil"),
        (3, "Peke", "F6", "Brasil"),
        (5, "Malteada", "F7", "Brasil"),
        (3, "Peke", "F8", "Brasil"),
        (2, "Lupita", "F9", "Mexico"),
    ]
    assert col == [
        "client__client_id",
        "client__name",
        "sale__name",
        "country__name",
    ]


def test_select_agg_1():
    m = QueryBox(model=Country)
    row, col = (
        m.select("country__name__dcount")
        .add(
            client__inner__country="country_id", client__inner__sale="client_id"
        )
        .all()
    )

    print(row)
    print(col)
