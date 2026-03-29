# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at 
# http://www.apache.org/licenses/LICENSE-2.0

"""
Test Main Class -*- PanCakesORM -*- 

Este fichero testea las tablas de manera dinamica.
Cada ejecucion primera de un fichero '.py' evalua el atributo de clase
'cls._loop_validation'. A traves del metodo de clase
cls._which_loop(), si el atributo es False, se sincroniza la tabla en
tiempo real, de lo contrario, no se intenta una sincronizacion
manteniendo hacia el estado del esquema optimizado.
"""

from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype
from pathlib import Path

DIR = Path.cwd() / 'data'
FILE = DIR / 'dynamic.sqlite'

# Tabla:
class Product(PanCakesORM):
    _table = 'product'
    _db_dir = DIR
    _db_file = FILE

    date = sql_datatype.Text(comment='Date', nls=False)

# Funcion de test
def test_dynamic_schema():
    res, cols = pancakes(
        db_path=FILE,
        select='*',
        from_='product',
        )

    print(res, cols)

# asser dinamico:
    # assert res == [(1, '')]
    # assert cols == ['product_id', 'date']