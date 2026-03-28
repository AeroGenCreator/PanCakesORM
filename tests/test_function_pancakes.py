# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at 
# http://www.apache.org/licenses/LICENSE-2.0

""" Test Funcion -*- pancakes -*- """

# Modulos Propios
from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype
from pancakes.cook.flavor import pancakes
from pancakes.cook.furnace import insert

# Modulos Python
from pathlib import Path
from datetime import datetime, date
import sqlite3

# Modulo Desarrollo
import ipdb
import pandas as pd

# RUTA GLOBAL
global_path = Path.cwd() / 'data' / 'test_env' / 'pancakes.sqlite'

# CREACION DE TABLAS DE PRUEBA
class Country(PanCakesORM):
    _table = 'country'
    # Forzamos la ruta a nuestra carpeta de test
    _db_dir = Path.cwd() / 'data' / 'test_env'
    _db_file = global_path

    name = sql_datatype.Char(comment = 'Name', nls = False, unq = False, size = 250)

class Estudiante(PanCakesORM):

    _table = 'estudiante'
    # Forzamos la ruta a nuestra carpeta de test
    _db_dir = Path.cwd() / 'data' / 'test_env'
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
    _db_dir = Path.cwd() / 'data' / 'test_env'
    _db_file = global_path

    name = sql_datatype.Char(comment = 'Curso', nls = False, unq = False, size = 250)
    date = sql_datatype.Text(comment = 'Date', nls = False)

class CursoEstudiante(PanCakesORM):
    _table = 'curso_estudiante'
    # Forzamos la ruta a nuestra carpeta de test
    _db_dir = Path.cwd() / 'data' / 'test_env'
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
    params=[
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

# -*- TEST QUERY: SELECT AND SPECIAL SELECT -*-
def test_select_star():
    # -----------------------------------------------
    # (select = *, uniones = 1, condiciones = 1)

    result, cols = pancakes(
        db_path = global_path,
        select = "*",
        from_ = 'country',
        join = [
            {
            'join':'inner',
            'extra':'estudiante',
            'fkey':'country_id',
            'origin':'country',
            'id':'country_id'
            }
        ],
        condition = [
            {
            'table':'estudiante',
            'column':'estudiante_id',
            'operator':'=',
            'value':1
            }
        ]
    )

    # Salida:
    assert result == [(1,'Mexico',1,'Chanchito',5,1,7,1)]

def test_select_str():
    # -----------------------------------------------
    # (select = columns, uniones = 1, condiciones = 1)

    result, cols = pancakes(
        db_path = global_path,
        select = [
            {
            'column':'name'
            }
        ],
        from_ = 'country',
        join = [
            {
            'join':'inner',
            'extra':'estudiante',
            'fkey':'country_id',
            'origin':'country',
            'id':'country_id'
            }
        ],
        condition = [
            {
            'table':'estudiante',
            'column':'estudiante_id',
            'operator':'=',
            'value':1,
            'join':''
            }
        ]
    )

    # Salida:
    # 'Mexico'
    assert result == [('Mexico',)]

def test_select_list():
    # (select = list, uniones = 1, condiciones = 1)
    result, cols = pancakes(
        db_path = global_path,
        select = [
            {
            'column':'name'
            },
            {
            'column':'age'
            },
            {
            'column':'is_student'
            },
        ],
        from_ = 'estudiante',
        join = [
            {
            'join':'inner',
            'extra':'curso_estudiante',
            'fkey':'estudiante_id',
            'origin':'estudiante',
            'id':'estudiante_id'
            },
            {
            'join':'inner',
            'extra':'curso',
            'fkey':'curso_id',
            'origin':'curso_estudiante',
            'id':'curso_id'
            }
        ],
        condition = [
            {
            'table':'estudiante',
            'column':'name',
            'operator':'in',
            'value':['Chanchito','Feliz']
            }
        ]
    )

    # Salida:
    assert result == [
            ('Chanchito', 5, 1),
            ('Chanchito', 5, 1),
            ('Feliz', 10, 0),
            ('Chanchito', 5, 1),
            ('Feliz', 10, 0)
        ]

def test_special_select_str():
    # (special_select = str, special = str, uniones = 1, condiciones = 1)

    result, cols = pancakes(
        db_path = global_path,
        select = [
            {
            'column':'name'
            },
            {
            'column':'age'
            },
            {
            'column':'is_student'
            }
        ],
        special_select = [
            {
            'table':'curso',
            'column':'name'
            }
        ],
        from_ = 'estudiante',
        join = [
            {
            'join':'inner',
            'extra':'curso_estudiante',
            'fkey':'estudiante_id',
            'origin':'estudiante',
            'id':'estudiante_id'
            },
            {
            'join':'inner',
            'extra':'curso',
            'fkey':'curso_id',
            'origin':'curso_estudiante',
            'id':'curso_id'
            }
        ],
        condition = [
            {
            'table':'estudiante',
            'column':'name',
            'operator':'in',
            'value':['Chanchito','Feliz'],
            'join':''
            }
        ]
    )

    # Salida:
    assert result == [
    ('Chanchito', 5, 1, 'Matematicas'),
    ('Chanchito', 5, 1, 'Ciencias'),
    ('Feliz', 10, 0, 'Ciencias'),
    ('Chanchito', 5, 1, 'Literatura'),
    ('Feliz', 10, 0, 'Literatura')
    ]

def test_special_select_list():
    # (special_select = list, special = str, uniones = 1, condiciones = 1)

    result, cols = pancakes(
        db_path = global_path,
        select = [
            {
            'column':'name'
            },
            {
            'column':'age'
            },
            {
            'column':'is_student'
            }
        ],
        special_select = [
            {
            'table':'curso',
            'column':'name',
            },
            {
            'table':'curso',
            'column':'date',
            }
        ],
        from_ = 'estudiante',
        join = [
            {
            'join':'inner',
            'extra':'curso_estudiante',
            'fkey':'estudiante_id',
            'origin':'estudiante',
            'id':'estudiante_id'
            },
            {
            'join':'inner',
            'extra':'curso',
            'fkey':'curso_id',
            'origin':'curso_estudiante',
            'id':'curso_id'
            }
        ],
        condition = [
            {
            'table':'estudiante',
            'column':'name',
            'operator':'in',
            'value':['Chanchito','Feliz']
            }
        ]
    )

    # Salida:
    assert result == [
    ('Chanchito', 5, 1, 'Matematicas', '2020-06-22T00:00:00'),
    ('Chanchito', 5, 1, 'Ciencias', '2021-08-15T00:00:00'),
    ('Feliz', 10, 0, 'Ciencias', '2021-08-15T00:00:00'),
    ('Chanchito', 5, 1, 'Literatura', '2010-06-20T00:00:00'),
    ('Feliz', 10, 0, 'Literatura', '2010-06-20T00:00:00')
    ]

# -*- TEST QUERY: M:1 , 1:M, M:M -*-
def test_many_to_one():
    
    # MANY TO ONE
    # Logica:
    # De la tabla padre (curso)
    # Quien es el padre de...
    # Estudiante "Andres"? en la tabla hija (estudiante)
    
    result, cols = pancakes(
        db_path=global_path,
        select="*",
        from_='country',
        join=[
            {
            'join':'inner',
            'extra':'estudiante',
            'fkey':'country_id',
            'origin':'country',
            'id':'country_id'
            }
        ],
        condition=[
            {
            'table':'estudiante',
            'column':'name',
            'operator':'=',
            'value':'Andres',
            'join':''
            }
        ]
    )
    
    assert result == [(2, 'Brasil', 3, 'Andres', 30, 0, 7, 2)]

def test_one_to_many():

    # MANY TO ONE
    # Logica:
    # De la tabla hija (estudiante)
    # quienes son los hijos del padre...
    # "Mexico" en tabla padre (curso)

    result, cols = pancakes(
        db_path=global_path,
        select="*",
        from_='country',
        join=[
            {
            'join':'inner',
            'extra':'estudiante',
            'fkey':'country_id',
            'origin':'country',
            'id':'country_id'
            }
        ],
        condition=[
            {
            'table':'country',
            'column':'name',
            'operator':'=',
            'value':'Mexico'
            }
        ]
    )
    
    assert result == [
        (1, 'Mexico', 1, 'Chanchito', 5, 1, 7, 1),
        (1, 'Mexico', 2, 'Feliz', 10, 0, 7, 1),
        (1, 'Mexico', 5, 'Malteada', 8, 0, 2, 1)
    ]

def test_many_to_many():

    # MANY TO MANY
    # Logica:
    # Muestra la relacion de
    # Diferentes estudiantes
    # en
    # Diferences cursos

    result, cols = pancakes(
        db_path=global_path,
        select="*",
        from_="estudiante",
        join=[
            {
            'join':'left',
            'extra':'curso_estudiante',
            'fkey':'estudiante_id',
            'origin':'estudiante',
            'id':'estudiante_id'
            },
            {
            'join':'left',
            'extra':'curso',
            'fkey':'curso_id',
            'origin':'curso_estudiante',
            'id':'curso_id'
            }
        ]
    )

    assert result == [
    (1, 'Chanchito', 5, 1, 7, 1, 1, 1, 1, '2018', 1, 'Matematicas', '2020-06-22T00:00:00'),
    (1, 'Chanchito', 5, 1, 7, 1, 2, 2, 1, '2019', 2, 'Ciencias', '2021-08-15T00:00:00'),
    (1, 'Chanchito', 5, 1, 7, 1, 4, 4, 1, '2014', 4, 'Literatura', '2010-06-20T00:00:00'),
    (2, 'Feliz', 10, 0, 7, 1, 3, 2, 2, '2018', 2, 'Ciencias', '2021-08-15T00:00:00'),
    (2, 'Feliz', 10, 0, 7, 1, 5, 4, 2, '2015', 4, 'Literatura', '2010-06-20T00:00:00'),
    (3, 'Andres', 30, 0, 7, 2, 6, 1, 3, '2019', 1, 'Matematicas', '2020-06-22T00:00:00'),
    (4, 'Polar', 6, 1, 4, 2, None, None, None, None, None, None, None),
    (5, 'Malteada', 8, 0, 2, 1, None, None, None, None, None, None, None)]

# -*- TEST NO JOINS | NO CONDITIONS | (SIMPLE QUERY) RETURNS ALL -*-
def test_no_join():
    result, cols = pancakes(
        db_path = global_path,
        select = "*",
        from_ = "country"
    )

    # Salida:
    assert result == [
        (1, 'Mexico'),
        (2, 'Brasil'),
        (3, 'China')
    ]

# -*- TEST CONDITIONS -*-
def test_one_condition():
    # Notar que para este test no paso el argumento join de la condicion:
    result, cols = pancakes(
        db_path=global_path,
        select="*",
        from_="estudiante",
        condition=[
            {
            'table':'estudiante',
            'column':'age',
            'operator':'>=',
            'value':10
            }
        ]
    )

    # Salida:
    assert result == [
        (2, 'Feliz', 10, 0, 7, 1),
        (3, 'Andres', 30, 0, 7, 2)
    ]

def test_multiple_condition_and():
    result, cols = pancakes(
        db_path = global_path,
        select="*",
        from_="estudiante",
        condition = [
            {
            'table':'estudiante',
            'column':'name',
            'operator':'in',
            'value': ['Andres', 'Polar', 'Malteada'],
            'join': 'and'
            },
            {
            'table':'estudiante',
            'column':'age',
            'operator':'>=',
            'value': 8
            }
        ]
    )

    # Salida:
    assert result == [
        (3, 'Andres', 30, 0, 7, 2),
        (5, 'Malteada', 8, 0, 2, 1)
    ]

def test_multiple_condition_and_or():
    result, cols = pancakes(
        db_path = global_path,
        select="*",
        from_="estudiante",
        condition = [
            {
            'table':'estudiante',
            'column':'name',
            'operator':'in',
            'value': ('Andres', 'Polar', 'Malteada'),
            'join': 'and'
            },
            {
            'table': 'estudiante',
            'column': 'age',
            'operator': '>=',
            'value': 8,
            'join': 'or'
            },
            {
            'table': 'estudiante',
            'column': 'name',
            'operator': 'in',
            'value': ['Chanchito']
            }
        ]
    )

    # Salida:
    assert result == [
        (1, 'Chanchito', 5, 1, 7, 1),
        (3, 'Andres', 30, 0, 7, 2),
        (5, 'Malteada', 8, 0, 2, 1)
    ]

# -*- TEST AGREGATION FUNCTIONS -*-
def test_simple_agg_func():
    result, cols = pancakes(
        db_path=global_path,
        select=[{'agg':'count','column':'name'}],
        from_='estudiante'
        )

    assert result == [(5,)]

def test_special_select_agg_func():

    result, cols = pancakes(
        db_path=global_path,
        select=[
            {
            'column':'name'
            }
        ],
        special_select = [
            {
            'agg':'count',
            'table':'country',
            'column':'name'
            }
        ],
        from_='estudiante',
        join=[
                {
                'join':'inner',
                'extra':'country',
                'fkey':'country_id',
                'origin':'estudiante',
                'id':'country_id'
                }
            ]
        )
    
    # Aunque la cuenta esta sobre country: se necesita group by
    # para segmentar el query:
    assert result == [('Chanchito', 5)]

# -*- TEST GROUP BY -*-
def test_group_by():
    # Cuenta los estudiantes
    # Agrupados por pais
    result, cols = pancakes(
        db_path = global_path,
        select = [
            {
            'agg':'count',
            'column':'name'
            }
        ],
        special_select=[
            {
            'table':'country',
            'column':'name'
            }
        ],
        from_ = 'estudiante',
        join = [
            {
            'join':'right',
            'extra':'country',
            'fkey':'country_id',
            'origin':'estudiante',
            'id':'country_id',
            }
        ],
        group_by = 
            [
                {
                'table':'country',
                'column':'name'
                }
            ]
        )

    assert result == [
        (2, 'Brasil'),
        (0, 'China'),
        (3, 'Mexico')
    ]

def test_group_by_multiple():
    result, cols = pancakes(
        db_path = global_path,
        select = [
            {
            'column':'fecha'
            }
        ],
        special_select=[
            {
            'agg':'count',
            'table':'estudiante',
            'column':'name'
            }
        ],
        from_ = 'curso_estudiante',
        join = [
            {
            'join':'right',
            'extra':'estudiante',
            'fkey':'estudiante_id',
            'origin':'curso_estudiante',
            'id':'estudiante_id',
            }
        ],
        group_by = 
            [
                {
                'table':'estudiante',
                'column':'name'
                },
                {
                'table':'curso_estudiante',
                'column':'fecha'
                }
            ]
        )
    
    assert result == [
        ('2019', 1),
        ('2014', 1),
        ('2018', 1),
        ('2019', 1),
        ('2015', 1),
        ('2018', 1),
        (None, 1),
        (None, 1)
    ]

# -*- TEST ORDER BY -*-
def test_group_by():
    # Cuenta los estudiantes
    # Agrupados por pais
    result, cols = pancakes(
        db_path = global_path,
        select = [
            {
            'agg':'count',
            'column':'name'
            }
        ],
        special_select=[
            {
            'table':'country',
            'column':'name'
            }
        ],
        from_ = 'estudiante',
        join = [
            {
            'join':'right',
            'extra':'country',
            'fkey':'country_id',
            'origin':'estudiante',
            'id':'country_id',
            }
        ],
        group_by = 
            [
                {
                'table':'country',
                'column':'name'
                }
            ],
        order_by = [
                {
                'table':'estudiante',
                'column':'name',
                'order':'desc'
                }
            ]
        )

    assert result == [
        (3, 'Mexico'),
        (2, 'Brasil'),
        (0, 'China')
    ]
    # SQL:

    # SELECT count(e.name), c.name
    # FROM estudiante AS e
    # RIGHT JOIN country AS c ON c.country_id = e.country_id
    # GROUP BY c.name 
    # ORDER BY e.name DESC;

# -*- TEST SELECT * AND SEPECIAL SELECT * -*-
def test_s_start_and_sp_star():
    result, cols = pancakes(
        db_path = global_path,
        select = "*",
        special_select = [
            {
            'table':'estudiante',
            'column':'*'
            }
        ],
        from_ = 'country',
        join = [
            {
            'join':'inner',
            'extra':'estudiante',
            'fkey':'country_id',
            'origin':'country',
            'id':'country_id'
            }
        ],
        condition = [
            {
            'table':'estudiante',
            'column':'estudiante_id',
            'operator':'=',
            'value':1
            }
        ]
    )

    # Salida:
    assert result == [(1,'Mexico',1,'Chanchito',5,1,7,1)]

# -*- TEST SELECT T1 * | T2 [COLUMNS]: -*- 

def test_t1_star_and_t2_columns():
    result, cols = pancakes(
        db_path = global_path,
        select = [
            {
            'column':'*',
            'alias': True
            }
        ],
        special_select = [
            {
            'agg':'sum',
            'table':'estudiante',
            'column':'age'
            }
        ],
        from_ = 'country',
        join = [
            {
            'join':'inner',
            'extra':'estudiante',
            'fkey':'country_id',
            'origin':'country',
            'id':'country_id'
            }
        ],
        group_by = [
            {
            'table':'country',
            'column':'name'
            }
        ]
    )

    # SQL
    
    # SELECT [c].*, 
    # SUM([e].[age]) 
    # FROM country AS c 
    # INNER JOIN estudiante AS e ON c.country_id = e.country_id 
    # GROUP BY [c].[name];

    assert result == [
        (2, 'Brasil', 36),
        (1, 'Mexico', 23)
    ]

# -*- TEST LIMIT -*-
def test_limit_in_query():
    result, cols = pancakes(
        db_path = global_path,
        select = [
            {
            'column':'*',
            'alias': True
            }
        ],
        special_select = [
            {
            'agg':'sum',
            'table':'estudiante',
            'column':'age'
            }
        ],
        from_ = 'country',
        join = [
            {
            'join':'inner',
            'extra':'estudiante',
            'fkey':'country_id',
            'origin':'country',
            'id':'country_id'
            }
        ],
        group_by = [
            {
            'table':'country',
            'column':'name'
            }
        ],
        order_by = [
            {
            'table':'estudiante',
            'column':'age',
            'order':'desc'
            }
        ],
        limit = 1
    )

    assert result == [(2, 'Brasil', 36)]

    # SQL
    
    # SELECT [c].*, 
    # SUM([e].[age]) 
    # FROM country AS c 
    # INNER JOIN estudiante AS e ON c.country_id = e.country_id 
    # GROUP BY [c].[name]
    # LIMIT 1;

def test_pancakes_direct_in_class():
    result, cols = Estudiante.pancakes()

    assert result == [
        (1, 'Chanchito', 5, 1, 7, 1),
        (2, 'Feliz', 10, 0, 7, 1),
        (3, 'Andres', 30, 0, 7, 2),
        (4, 'Polar', 6, 1, 4, 2),
        (5, 'Malteada', 8, 0, 2, 1)
    ]

    print()
    # print(pd.DataFrame(data=result, columns=cols))