# -*- coding: utf-8 -*-
# PanCakesORM v6.0.0 | Test Suite
# Copyright (c) 2026 AeroGenCreator (https://github.com/AeroGenCreator)
# Licensed under the Apache License, Version 2.0 (the "License");
# You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
# ==============================================================================

from pathlib import Path

from pancakes.models.model import PanCakesORM
from pancakes.sql import datatype

"""
- ATENCION!

ANTES DE CORRER ESTE SCRIPT CON PYTEST ES IMPORTANTE EJECUTAR EL .sh QUE SE
HAYA EN ESTE DIRECTORIO DE 'TESTING'. EL MISMO GENERARA LA BASE DE DATOS
LAS TABLAS E INFORMACION. EL CUAL SERA VALIDADA POR ESTE SCRIPT AL COMPUTAR
EL CAMPO 'name'.

PanCakesORM/tests/dbForCompute.sh

Solo se computan cambos cuando se hace query a una tabla & a todas
sus columnas,

Si se unen mas tablas o no se seleccionan todas las columnas
los campos no seran computados.

Tambien se puede hacer manual en la base de datos usando los siguientes
comandos Sqlite3.

La ruta de la base de datos debe ser exacamente esta:

PanCakesORM/data/test_env/computar_queries.sqlite

COMANDOS SQLITE3

CREATE TABLE IF NOT EXISTS state(state_id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE IF NOT EXISTS department(
    department_id INTEGER PRIMARY KEY,
    name TEXT,
    location TEXT,
    state_id INTEGER,
   FOREIGN KEY (state_id)
      REFERENCES state (state_id)
         ON DELETE CASCADE
         ON UPDATE NO ACTION
);

INSERT INTO state(name) VALUES("Tlaxcala");
INSERT INTO department(location, state_id) VALUES("Chiautempan", 1);

"""

# GLOBAL PATH para pruebas
dir_ = Path.cwd() / "data" / "test_env"
file = dir_ / "computar_queries.sqlite"


class State(PanCakesORM):
    _table = "state"
    _depends = "self"
    _db_dir = dir_
    _db_file = file

    name = datatype.Char(comment="State", required=True)


class Department(PanCakesORM):
    _table = "department"
    _depends = "self"
    _db_dir = dir_
    _db_file = file

    # Para testear callable, s tring al pasar computes.
    # Basta con cambiarlos en compute=<> AQUI.

    # Testeado como callable - Funciono
    def computar_nombre_como_callable(self):
        NAME = None
        if self.location.value and self.state_id.value:
            NAME = f"{self.location.value} {self.state_id.value}"
        self.name.value = NAME
        return self.name.value

    name = datatype.Char(comment="Department", compute="computar_nombre")
    location = datatype.Char(comment="Location", required=True)
    state_id = datatype.ForeignKey(
        comment="State",
        second_table="state",
        column_id="state_id",
    )

    # Testeado como string - Funciono
    def computar_nombre(self):
        NAME = None
        if self.location.value and self.state_id.value:
            NAME = f"{self.location.value} {self.state_id.value}"
        self.name.value = NAME
        return self.name.value


# === TEST AUTOMATICOS ===


# Salida de diccionario
def test_computar_queries():
    dicc = Department.all().dictionary()

    assert dicc == [
        {
            "department__department_id": 1,
            "department__name": "Chiautempan 1",
            "department__location": "Chiautempan",
            "department__state_id": 1,
        }
    ]


# Salida en crudo
def test_computar_queries_raw():
    rows, cols = Department.all().raw()

    assert rows == [(1, "Chiautempan 1", "Chiautempan", 1)]
    assert cols == [
        "department__department_id",
        "department__name",
        "department__location",
        "department__state_id",
    ]


# Salida en container
def test_computar_queries_container():
    container = Department.all().container()

    assert container == {
        "department": {
            "@main_table@": True,
            "@depends@": ["self"],
            "department_id": {
                "vector": [1],
                "label": "DEPARTMENT ID",
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
                "vector": ["Chiautempan 1"],
                "label": "Department",
                "position": 1,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "str",
                "primary_key": False,
                "sql_type": "VARCHAR",
                "second_table": False,
                "foreign_key": False,
            },
            "location": {
                "vector": ["Chiautempan"],
                "label": "Location",
                "position": 2,
                "readonly": False,
                "default": None,
                "required": True,
                "python_type": "str",
                "primary_key": False,
                "sql_type": "VARCHAR",
                "second_table": False,
                "foreign_key": False,
            },
            "state_id": {
                "vector": [1],
                "label": "State",
                "position": 3,
                "readonly": True,
                "default": None,
                "required": False,
                "python_type": "int",
                "primary_key": False,
                "sql_type": "FOREIGN KEY",
                "second_table": "state",
                "foreign_key": "state_id",
            },
        }
    }


def test_computar_queries_raw_align():
    rows, cols = Department.all().raw(align=True)

    assert rows == [(1,), ("Chiautempan 1",), ("Chiautempan",), (1,)]
    assert cols == [
        "department__department_id",
        "department__name",
        "department__location",
        "department__state_id",
    ]
