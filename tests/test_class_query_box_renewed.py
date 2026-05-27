# -*- coding: utf-8 -*-
# PanCakesORM v5.0.0 | Test Suite
# Copyright (c) 2026 AeroGenCreator (https://github.com/AeroGenCreator)
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
# ==============================================================================

"""Test QueryBox Class -*- PanCakesORM -*-"""

# Modulos Propios
# Modulos Python
from pathlib import Path

from pancakes.abstract.query_box import QueryBox
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


class Empty(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = "empty"
    _depends = "self"

    name1 = datatype.Char(comment="Empty Col 1")
    name2 = datatype.Char(comment="Empty Col 2")


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
    row, col = q_country.select("country__country_id").all().raw()

    assert row == [(1,), (2,)]
    assert col == ["country__country_id"]


def test_select_varios():
    row, col = (
        q_country.select("country__country_id", "country__name").all().raw()
    )

    assert row == [(1, "Mexico"), (2, "Brasil")]
    assert col == ["country__country_id", "country__name"]


def test_select_uno_agg():
    row, col = q_client.select("client__client_id__count").all().raw()

    assert row == [(5,)]
    assert col == ["client__client_id__count"]


def test_select_varios_agg():
    row, col = (
        q_client.select("client__client_id__count", "client__client_id__sum")
        .all()
        .raw()
    )

    assert row == [(5, 15)]
    assert col == ["client__client_id__count", "client__client_id__sum"]


def test_no_select():
    row, col = q_sale.select().all().raw()

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
    row, col = (
        q_sale.add(
            sale__left__client="client_id", client__right__country="country_id"
        )
        .all()
        .raw()
    )

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
    row, col = q_country.add(country__left__client="country_id").all().raw()

    assert row == [
        (1, "Mexico", 1, "Andres", 1),
        (1, "Mexico", 2, "Lupita", 1),
        (1, "Mexico", 4, "Polar", 1),
        (2, "Brasil", 3, "Peke", 2),
        (2, "Brasil", 5, "Malteada", 2),
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
        .raw()
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
        .filter(client__name__like="%malteada")
        .all()
        .raw()
    )

    assert row == [("Malteada", "Brasil")]
    assert col == ["client__name", "country__name"]


def test_filter_strings():
    row, col = (
        q_client.select("client__client_id", "client__name")
        .filter(client__name__same="Malteada")
        .all()
        .raw()
    )

    assert row == [(5, "Malteada")]
    assert col == ["client__client_id", "client__name"]


def test_seleccion_exacta():
    m = QueryBox(model=Country)
    row, col = (
        m.select("client__name")
        .add(client__inner__country="country_id")
        .filter(client__name__same="Malteada")
        .all()
        .raw()
    )

    assert row == [("Malteada",)]
    assert col == ["client__name"]


def test_filter_iterables():
    m = QueryBox(model=Country)
    row, col = (
        m.select("client__name")
        .add(client__inner__country="country_id")
        .filter(client__name__in__and=["Malteada"], client__client_id__same=5)
        .all()
        .raw()
    )
    assert row == [("Malteada",)]
    assert col == ["client__name"]


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
        .raw()
    )

    assert row == [
        (1, "Andres", "F1", "Mexico"),
        (1, "Andres", "F4", "Mexico"),
        (2, "Lupita", "F9", "Mexico"),
        (3, "Peke", "F2", "Brasil"),
        (3, "Peke", "F5", "Brasil"),
        (3, "Peke", "F6", "Brasil"),
        (3, "Peke", "F8", "Brasil"),
        (4, "Polar", "F3", "Mexico"),
        (5, "Malteada", "F7", "Brasil"),
    ]
    assert col == [
        "client__client_id",
        "client__name",
        "sale__name",
        "country__name",
    ]


def test_select_agg_distincti_count_group_by():
    m = QueryBox(model=Country)
    row, col = (
        m.select("country__name", "client__name__dcount")
        .add(
            client__inner__country="country_id", client__inner__sale="client_id"
        )
        .all()
        .raw()
    )

    assert row == [("Brasil", 2), ("Mexico", 3)]
    assert col == ["country__name", "client__name__dcount"]


def test_order_by_add_select_agg():
    row, col = (
        q_country.select("country__name", "client__name__dcount")
        .add(country__left__client="country_id")
        .filter(country__country_id__in__or=[1, 2], country__country_id__same=3)
        .sort("country__name__desc")
        .all()
        .raw()
    )

    assert row == [("Mexico", 3), ("Brasil", 2)]
    assert col == ["country__name", "client__name__dcount"]


def test_add_full_control_limit():
    m = QueryBox(model=Country)
    row, col = (
        m.select(
            "client__client_id", "client__name", "sale__name", "country__name"
        )
        .add(
            client__inner__country="country_id", client__inner__sale="client_id"
        )
        .chunk(limit=2)
        .all()
        .raw()
    )

    assert row == [(1, "Andres", "F1", "Mexico"), (1, "Andres", "F4", "Mexico")]
    assert col == [
        "client__client_id",
        "client__name",
        "sale__name",
        "country__name",
    ]


def test_add_full_control_limit_offset():
    m = QueryBox(model=Country)
    row, col = (
        m.select(
            "client__client_id", "client__name", "sale__name", "country__name"
        )
        .add(
            client__inner__country="country_id", client__inner__sale="client_id"
        )
        .chunk(limit=2, offset=2)
        .all()
        .raw()
    )

    assert row == [(2, "Lupita", "F9", "Mexico"), (3, "Peke", "F2", "Brasil")]
    assert col == [
        "client__client_id",
        "client__name",
        "sale__name",
        "country__name",
    ]


def test_select_ids():
    m = QueryBox(model=Country)
    row, col = (
        m.select(
            "client__client_id", "client__name", "sale__name", "country__name"
        )
        .add(
            client__inner__country="country_id", client__inner__sale="client_id"
        )
        .all(ids=True)
        .raw()
    )

    assert row == [(1,), (2,), (3,), (4,), (5,)]
    assert col == ["client__client_id__distinct"]


def test_btwn_filter_align_label():
    m = QueryBox(model=Country)
    row, col = (
        m.select(
            "client__client_id", "client__name", "sale__name", "country__name"
        )
        .add(
            client__inner__country="country_id", client__inner__sale="client_id"
        )
        .filter(client__client_id__btwn=[2, 5])
        .all(ids=True)
        .raw(label=True, align=True)
    )

    assert row == [(2, 3, 4, 5)]
    assert col == ["CLIENT ID"]


def test_align_multiple_columns():
    m = QueryBox(model=Country)
    row, col = (
        m.select(
            "client__client_id", "client__name", "sale__name", "country__name"
        )
        .add(
            client__inner__country="country_id", client__inner__sale="client_id"
        )
        .filter(client__client_id__btwn=[2, 5])
        .all()
        .raw(label=True, align=True)
    )

    assert row == [
        (2, 3, 3, 3, 3, 4, 5),
        ("Lupita", "Peke", "Peke", "Peke", "Peke", "Polar", "Malteada"),
        ("F9", "F2", "F5", "F6", "F8", "F3", "F7"),
        ("Mexico", "Brasil", "Brasil", "Brasil", "Brasil", "Mexico", "Brasil"),
    ]
    assert col == ["CLIENT ID", "Client Name", "Sale Code", "Country"]


def test_dictionary_output():
    dicc = q_sale.all().dictionary(label=True)

    assert dicc == [
        {"SALE ID": 1, "Sale Code": "F1", "Cliente Rel": 1},
        {"SALE ID": 2, "Sale Code": "F2", "Cliente Rel": 3},
        {"SALE ID": 3, "Sale Code": "F3", "Cliente Rel": 4},
        {"SALE ID": 4, "Sale Code": "F4", "Cliente Rel": 1},
        {"SALE ID": 5, "Sale Code": "F5", "Cliente Rel": 3},
        {"SALE ID": 6, "Sale Code": "F6", "Cliente Rel": 3},
        {"SALE ID": 7, "Sale Code": "F7", "Cliente Rel": 5},
        {"SALE ID": 8, "Sale Code": "F8", "Cliente Rel": 3},
        {"SALE ID": 9, "Sale Code": "F9", "Cliente Rel": 2},
    ]


def test_container_output_si_label():
    container = q_sale.add(sale__inner__client="client_id").all().container()

    assert container == {
        "sale": {
            "@main_table@": True,
            "@depends@": ["client"],
            "sale_id": {
                "vector": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "label": "SALE ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": [
                    "F1",
                    "F2",
                    "F3",
                    "F4",
                    "F5",
                    "F6",
                    "F7",
                    "F8",
                    "F9",
                ],
                "label": "Sale Code",
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
            "client_id": {
                "vector": [1, 3, 4, 1, 3, 3, 5, 3, 2],
                "label": "Cliente Rel",
                "position": 2,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": False,
                "sql_type": "FOREIGN KEY",
                "second_table": "client",
                "foreign_key": "client_id",
            },
        },
        "client": {
            "@main_table@": False,
            "@depends@": ["country"],
            "client_id": {
                "vector": [1, 3, 4, 1, 3, 3, 5, 3, 2],
                "label": "CLIENT ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": [
                    "Andres",
                    "Peke",
                    "Polar",
                    "Andres",
                    "Peke",
                    "Peke",
                    "Malteada",
                    "Peke",
                    "Lupita",
                ],
                "label": "Client Name",
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
            "country_id": {
                "vector": [1, 2, 1, 1, 2, 2, 2, 2, 1],
                "label": "Country Rel",
                "position": 2,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": False,
                "sql_type": "FOREIGN KEY",
                "second_table": "country",
                "foreign_key": "country_id",
            },
        },
    }


def test_container_output_no_label():
    container = q_sale.add(sale__inner__client="client_id").all().container()

    assert container == {
        "sale": {
            "@main_table@": True,
            "@depends@": ["client"],
            "sale_id": {
                "vector": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "label": "SALE ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": [
                    "F1",
                    "F2",
                    "F3",
                    "F4",
                    "F5",
                    "F6",
                    "F7",
                    "F8",
                    "F9",
                ],
                "label": "Sale Code",
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
            "client_id": {
                "vector": [1, 3, 4, 1, 3, 3, 5, 3, 2],
                "label": "Cliente Rel",
                "position": 2,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": False,
                "sql_type": "FOREIGN KEY",
                "second_table": "client",
                "foreign_key": "client_id",
            },
        },
        "client": {
            "@main_table@": False,
            "@depends@": ["country"],
            "client_id": {
                "vector": [1, 3, 4, 1, 3, 3, 5, 3, 2],
                "label": "CLIENT ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": [
                    "Andres",
                    "Peke",
                    "Polar",
                    "Andres",
                    "Peke",
                    "Peke",
                    "Malteada",
                    "Peke",
                    "Lupita",
                ],
                "label": "Client Name",
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
            "country_id": {
                "vector": [1, 2, 1, 1, 2, 2, 2, 2, 1],
                "label": "Country Rel",
                "position": 2,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": False,
                "sql_type": "FOREIGN KEY",
                "second_table": "country",
                "foreign_key": "country_id",
            },
        },
    }


def test_container_empty_table():
    m = QueryBox(Empty)
    container = m.all().container()

    assert container == {
        "empty": {
            "@main_table@": True,
            "@depends@": ["self"],
            "empty_id": {
                "vector": [None],
                "label": "EMPTY ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "",
                "second_table": False,
                "foreign_key": False,
            },
            "name1": {
                "vector": [None],
                "label": "Empty Col 1",
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
            "name2": {
                "vector": [None],
                "label": "Empty Col 2",
                "position": 2,
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


def test_link_one_table():
    m = QueryBox(Client)

    container = m.link("country").all().container()

    assert container == {
        "client": {
            "@main_table@": True,
            "@depends@": ["country"],
            "client_id": {
                "vector": [1, 2, 3, 4, 5],
                "label": "CLIENT ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": ["Andres", "Lupita", "Peke", "Polar", "Malteada"],
                "label": "Client Name",
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
            "country_id": {
                "vector": [1, 1, 2, 1, 2],
                "label": "Country Rel",
                "position": 2,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": False,
                "sql_type": "FOREIGN KEY",
                "second_table": "country",
                "foreign_key": "country_id",
            },
        },
        "country": {
            "@main_table@": False,
            "@depends@": ["self"],
            "country_id": {
                "vector": [1, 1, 2, 1, 2],
                "label": "COUNTRY ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": ["Mexico", "Mexico", "Brasil", "Mexico", "Brasil"],
                "label": "Country",
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
    }


def test_link_two_tables():
    m = QueryBox(Client)

    container = m.link("country", "sale").all().container()

    assert container == {
        "client": {
            "@main_table@": True,
            "@depends@": ["country"],
            "client_id": {
                "vector": [1, 3, 4, 1, 3, 3, 5, 3, 2],
                "label": "CLIENT ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": [
                    "Andres",
                    "Peke",
                    "Polar",
                    "Andres",
                    "Peke",
                    "Peke",
                    "Malteada",
                    "Peke",
                    "Lupita",
                ],
                "label": "Client Name",
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
            "country_id": {
                "vector": [1, 2, 1, 1, 2, 2, 2, 2, 1],
                "label": "Country Rel",
                "position": 2,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": False,
                "sql_type": "FOREIGN KEY",
                "second_table": "country",
                "foreign_key": "country_id",
            },
        },
        "sale": {
            "@main_table@": False,
            "@depends@": ["client"],
            "sale_id": {
                "vector": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "label": "SALE ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": [
                    "F1",
                    "F2",
                    "F3",
                    "F4",
                    "F5",
                    "F6",
                    "F7",
                    "F8",
                    "F9",
                ],
                "label": "Sale Code",
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
            "client_id": {
                "vector": [1, 3, 4, 1, 3, 3, 5, 3, 2],
                "label": "Cliente Rel",
                "position": 2,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": False,
                "sql_type": "FOREIGN KEY",
                "second_table": "client",
                "foreign_key": "client_id",
            },
        },
        "country": {
            "@main_table@": False,
            "@depends@": ["self"],
            "country_id": {
                "vector": [1, 2, 1, 1, 2, 2, 2, 2, 1],
                "label": "COUNTRY ID",
                "position": 0,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": True,
                "sql_type": "",
                "second_table": False,
                "foreign_key": False,
            },
            "name": {
                "vector": [
                    "Mexico",
                    "Brasil",
                    "Mexico",
                    "Mexico",
                    "Brasil",
                    "Brasil",
                    "Brasil",
                    "Brasil",
                    "Mexico",
                ],
                "label": "Country",
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
    }


# ----------------------------------------------------
# ESTA SECCION TESTE EL METODO LINK SOBRE UN CASO REAL
# ----------------------------------------------------

# ----------------------------------------------------

"""-*- Test Self oriented link -*-"""


class Category(PanCakesORM):
    _table = "category"
    _depends = "self"
    _db_dir = dir_
    _db_file = file

    category = datatype.Char(comment="Category", unique=True)


class Product(PanCakesORM):
    _table = "product"
    _depends = ["category"]
    _group_constraint = ("product_name", "category_id")
    _db_dir = dir_
    _db_file = file

    product_name = datatype.Char(comment="Product Name", unique=True)
    category_id = datatype.ForeignKey(
        comment="Product Category Rel",
        second_table="category",
        column_id="category_id",
    )


class Recipe(PanCakesORM):
    _table = "recipe"
    _depends = ["self"]
    _db_dir = dir_
    _db_file = file

    recipe_ingredient = datatype.Char(comment="Recipe Ingredient", unique=True)


class RecipeProductLine(PanCakesORM):
    _table = "recipe_product_line"
    _depends = ["recipe", "product", "category"]
    _group_constraint = ("recipe_id", "product_id", "category_id")
    _db_dir = dir_
    _db_file = file

    recipe_id = datatype.ForeignKey(
        comment="Recipe Line Rel", second_table="recipe", column_id="recipe_id"
    )
    product_id = datatype.ForeignKey(
        comment="Product Line Rel",
        second_table="product",
        column_id="product_id",
    )
    category_id = datatype.ForeignKey(
        comment="Category Line Rel",
        second_table="category",
        column_id="category_id",
    )


Category.i(category=[(None, "Dessert")])
Product.i(product=[(None, "Concha", 1)])
Recipe.i(recipe=[(None, "Sugar"), (None, "Milk"), (None, "Salt")])
RecipeProductLine.i(
    recipe_product_line=[(None, 1, 1, 1), (None, 2, 1, 1), (None, 3, 1, 1)]
)


def test_real_1():
    m = QueryBox(Category)
    dicc = m.all().dictionary(label=True)

    assert dicc == [{"CATEGORY ID": 1, "Category": "Dessert"}]


def test_real_2():
    m = QueryBox(Product)
    dicc2 = (
        m.select("product__product_name", "category__category")
        .link("category")
        .all()
        .dictionary(label=True)
    )
    assert dicc2 == [{"Product Name": "Concha", "Category": "Dessert"}]


def test_real_3():
    m = QueryBox(Recipe)
    dicc3 = m.all().dictionary(label=True)
    assert dicc3 == [
        {"RECIPE ID": 1, "Recipe Ingredient": "Sugar"},
        {"RECIPE ID": 2, "Recipe Ingredient": "Milk"},
        {"RECIPE ID": 3, "Recipe Ingredient": "Salt"},
    ]


def test_real_4():
    m = QueryBox(RecipeProductLine)
    dicc4 = (
        m.link("product", "category", "recipe")
        .select(
            "category__category",
            "product__product_name",
            "recipe__recipe_ingredient",
        )
        .all()
        .dictionary(label=True)
    )

    assert dicc4 == [
        {
            "Category": "Dessert",
            "Product Name": "Concha",
            "Recipe Ingredient": "Milk",
        },
        {
            "Category": "Dessert",
            "Product Name": "Concha",
            "Recipe Ingredient": "Salt",
        },
        {
            "Category": "Dessert",
            "Product Name": "Concha",
            "Recipe Ingredient": "Sugar",
        },
    ]


def test_final_link():
    m = QueryBox(Product)
    dicc5 = (
        m.link("recipe", "category", "recipe_product_line")
        .select(
            "recipe_product_line__recipe_product_line_id",
            "category__category",
            "product__product_name",
            "recipe__recipe_ingredient",
        )
        .all()
        .dictionary(label=True)
    )

    assert dicc5 == [
        {
            "RECIPE_PRODUCT_LINE ID": 1,
            "Category": "Dessert",
            "Product Name": "Concha",
            "Recipe Ingredient": "Sugar",
        },
        {
            "RECIPE_PRODUCT_LINE ID": 2,
            "Category": "Dessert",
            "Product Name": "Concha",
            "Recipe Ingredient": "Milk",
        },
        {
            "RECIPE_PRODUCT_LINE ID": 3,
            "Category": "Dessert",
            "Product Name": "Concha",
            "Recipe Ingredient": "Salt",
        },
    ]


def test_group_by():
    m = QueryBox(Product)
    dicc = (
        m.link("recipe", "category", "recipe_product_line")
        .select("recipe__recipe_ingredient__dcount", "category__category")
        .group(category="category")
        .all()
        .dictionary(label=True)
    )

    assert dicc == [{"Recipe Ingredient DCOUNT": 3, "Category": "Dessert"}]
