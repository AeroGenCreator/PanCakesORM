# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

""" Test Funcion -*- update -*- """

# Modulos Propios
from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype
from pancakes.cook.ingredient import update
from pancakes.cook.furnace import insert

# Modulos Python
from pathlib import Path
from datetime import datetime, date
import sqlite3

# Modulo Desarrollo
import ipdb
import pandas as pd

# RUTA GLOBAL
global_dir = Path.cwd() / 'data' / 'test_env'
global_path = global_dir / 'update.sqlite'

# CREACION DE TABLAS DE PRUEBA
class Country(PanCakesORM):
    _table = 'country'
    # Forzamos la ruta a nuestra carpeta de test
    _db_dir = global_dir
    _db_file = global_path

    name = sql_datatype.Char(comment = 'Name', nls = False, unq = False, size = 250)

class Estudiante(PanCakesORM):

    _table = 'estudiante'
    # Forzamos la ruta a nuestra carpeta de test
    _db_dir = global_dir
    _db_file = global_path

    name = sql_datatype.Char(comment = 'Name', nls = False, unq = False, size = 250)
    age = sql_datatype.Int(comment = 'Age', nls = False, unq = False)
    is_student = sql_datatype.Bool(comment = 'Is Student', nls = True)
    stature = sql_datatype.Int(comment = 'Stature', nls = True)
    country_id = sql_datatype.ForeignKey(
        second_table = 'country',
        column_id = 'country_id',
        comment = 'Country',
        on_del = 'no action',
        on_upd = 'no action')

class Curso(PanCakesORM):
    _table = 'curso'
    # Forzamos la ruta a nuestra carpeta de test
    _db_dir = global_dir
    _db_file = global_path

    name = sql_datatype.Char(comment = 'Curso', nls = False, unq = False, size = 250)
    date = sql_datatype.Text(comment = 'Date', nls = False)

class CursoEstudiante(PanCakesORM):
    _table = 'curso_estudiante'
    # Forzamos la ruta a nuestra carpeta de test
    _db_dir = global_dir
    _db_file = global_path

    curso_id = sql_datatype.ForeignKey(
        second_table = 'curso',
        column_id = 'curso_id',
        comment = 'Curso',
        on_del = 'set null',
        on_upd = 'set null')
    estudiante_id = sql_datatype.ForeignKey(
        second_table = 'estudiante',
        column_id = 'estudiante_id',
        comment = 'Estudiante',
        on_del = 'set null',
        on_upd = 'set null')
    fecha = sql_datatype.Text(comment = 'Fecha', nls = False)

insert(
    db_path=global_path,
    chart=[
        {
        'table':'country',
        'data':[
                (None, 'Mexico'),
                (None, 'Brasil'),
                (None, 'China')
            ]
        },
        {
        'table':'estudiante',
        'data':[
                (None, 'Chanchito', 5, True, 7, 1),
                (None, 'Feliz', 10, False, 7, 1),
                (None, 'Andres', 30, False, 7, 2),
                (None, 'Polar', 6, True, 4, 2),
                (None, 'Malteada', 8, False, 2, 1)
            ]
        },
        {
        'table':'curso',
        'data':[
                (None, 'Matematicas', datetime(2020,6,22).isoformat()),
                (None, 'Ciencias', datetime(2021,8,15).isoformat()),
                (None, 'Quimica', datetime(2020,6,20).isoformat()),
                (None, 'Literatura', datetime(2010,6,20).isoformat()),
                (None, 'Cocina', datetime(2003,3,9).isoformat()),
                (None, 'Fisica', datetime(2008,7,10).isoformat())
            ]
        },
        {
        'table':'curso_estudiante',
        'data':[
                (None, 1, 1,'2018'),
                (None, 2, 1,'2019'),
                (None, 2, 2,'2018'),
                (None, 4, 1,'2014'),
                (None, 4, 2,'2015'),
                (None, 1, 3,'2019')
            ]
        },
    ]
)

# COMIENZO DE LOS TESTS
def test_update_all_one_table():
    update(
        db_path=global_path,
        params=[
            {
            'table':'curso',
            'name':'date',
            'data':'2026-03-24'
            }
        ],
        update_all=True
    )

    res = Curso.return_all()
    
    assert res == [
        (1, 'Matematicas', '2026-03-24'),
        (2, 'Ciencias', '2026-03-24'),
        (3, 'Quimica', '2026-03-24'),
        (4, 'Literatura', '2026-03-24'),
        (5, 'Cocina', '2026-03-24'),
        (6, 'Fisica', '2026-03-24')
    ]

def test_update_all_multiple_tables():
    update(
        db_path=global_path,
        params=[
            {
            'table':'curso',
            'name':'date',
            'data': '2011-Ago-14'
            },
            {
            'table':'curso_estudiante',
            'name':'fecha',
            'data': '2011'
            },
        ],
        update_all=True
    )

    res1 = Curso.return_all()
    res2 = CursoEstudiante.return_all()

    assert res1 == [
        (1, 'Matematicas', '2011-Ago-14'),
        (2, 'Ciencias', '2011-Ago-14'),
        (3, 'Quimica', '2011-Ago-14'),
        (4, 'Literatura', '2011-Ago-14'),
        (5, 'Cocina', '2011-Ago-14'),
        (6, 'Fisica', '2011-Ago-14')
    ]
    assert res2 == [
        (1, 1, 1, '2011'),
        (2, 2, 1, '2011'),
        (3, 2, 2, '2011'),
        (4, 4, 1, '2011'),
        (5, 4, 2, '2011'),
        (6, 1, 3, '2011')
    ]

def test_update_one_register_one_table():
    update(
        db_path=global_path,
        params=[
            {
            'table':'country',
            'name':'name',
            'data':'Canada',
            'condition':[
                    {
                    'column':'name',
                    'operator':'like',
                    'value':'%hina',
                    }
                ]
            }
        ]
    )

    res = Country.return_all()

    assert res == [
        (1, 'Mexico'),
        (2, 'Brasil'),
        (3, 'Canada')
    ]

def test_update_one_register_multiple_tables():
    update(
        db_path=global_path,
        params=[
            {
            'table':'country',
            'name':'name',
            'data':'Espanha',
            'condition':[
                    {
                    'column':'name',
                    'operator':'=',
                    'value':'Canada',
                    }
                ]
            },
            {
            'table':'estudiante',
            'name':'name',
            'data':'Enojado',
            'condition':[
                    {
                    'column':'name',
                    'operator':'in',
                    'value':['Feliz']
                    }
                ]
            }
        ]
    )
    res1 = Country.return_all()
    res2 = Estudiante.return_all()
    
    assert res1 == [(1, 'Mexico'), (2, 'Brasil'), (3, 'Espanha')]
    assert res2 == [
        (1, 'Chanchito', 5, 1, 7, 1),
        (2, 'Enojado', 10, 0, 7, 1),
        (3, 'Andres', 30, 0, 7, 2),
        (4, 'Polar', 6, 1, 4, 2),
        (5, 'Malteada', 8, 0, 2, 1)
    ]

def test_update_one_register_one_table_or():

    update(
        db_path=global_path,
        params=[
            {
            'table':'estudiante',
            'name':'age',
            'data':9,
            'condition':[
                    {
                    'column':'estudiante_id',
                    'operator':'=',
                    'value':10,
                    'logic':'or'
                    },
                    {
                    'column':'estudiante_id',
                    'operator':'=',
                    'value':5,
                    }
                ]
            }
        ]
    )

    res = Estudiante.return_all()

    assert res == [
        (1, 'Chanchito', 5, 1, 7, 1),
        (2, 'Enojado', 10, 0, 7, 1),
        (3, 'Andres', 30, 0, 7, 2),
        (4, 'Polar', 6, 1, 4, 2),
        (5, 'Malteada', 9, 0, 2, 1)
    ]

def test_update_multiple_registers_one_table_and():
    update(
        db_path=global_path,
        params=[
            {
            'table':'curso_estudiante',
            'name':'fecha',
            'data':'2026',
            'condition':[
                    {
                    'column':'fecha',
                    'operator':'=',
                    'value':'2011',
                    'logic':'and'
                    },
                    {
                    'column':'curso_estudiante_id',
                    'operator':'not in',
                    'value':[1,2,3],
                    }
                ]
            }
        ]
    )
    res = CursoEstudiante.return_all()

    assert res == [
        (1, 1, 1, '2011'),
        (2, 2, 1, '2011'),
        (3, 2, 2, '2011'),
        (4, 4, 1, '2026'),
        (5, 4, 2, '2026'),
        (6, 1, 3, '2026')
    ]

def test_update_range_multiples_tables():
    update(
        db_path=global_path,
        params=[
            {
            'table':'curso',
            'name':'date',
            'data':'2010-Enero-01',
            'condition':[
                    {
                    'column':'curso_id',
                    'operator':'between',
                    'value':[2,4],
                    'logic':'and'
                    },
                    {
                    'column':'name',
                    'operator':'=',
                    'value':'Quimica',
                    'logic':''
                    }
                ]
            },
            {
            'table':'estudiante',
            'name':'name',
            'data':'Chancho',
            'condition':[
                    {
                    'column':'estudiante_id',
                    'operator':'=',
                    'value':1,
                    }
                ]
            },
        ]
    )
    res1 = Curso.return_all()
    res2 = Estudiante.return_all()

    assert res1 == [
        (1, 'Matematicas', '2011-Ago-14'),
        (2, 'Ciencias', '2011-Ago-14'),
        (3, 'Quimica', '2010-Enero-01'),
        (4, 'Literatura', '2011-Ago-14'),
        (5, 'Cocina', '2011-Ago-14'),
        (6, 'Fisica', '2011-Ago-14')
    ]
    assert res2 == [
        (1, 'Chancho', 5, 1, 7, 1),
        (2, 'Enojado', 10, 0, 7, 1),
        (3, 'Andres', 30, 0, 7, 2),
        (4, 'Polar', 6, 1, 4, 2),
        (5, 'Malteada', 9, 0, 2, 1)
    ]