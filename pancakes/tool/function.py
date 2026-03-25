# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""
Declaracion De Clase Context Manager: Inyeccion De Dependencia
(Conexion y Cursor) - Es Utilizada Por La Clase ORM Principal.
"""

# Modulos Python
import sqlite3
from contextlib import contextmanager
import logging

logging.basicConfig(
    level=logging.WARNING,  # Captura todo desde WARNING hacia arriba
    format='%(asctime)s [%(levelname)s] '
    '%(name)s.%(funcName)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)


@contextmanager
def db_connection(db_path: str, no_foreign: bool = False, timeout:  int = 10):
    """
    Funcion De Conexion E Inyeccion de datos
    Para La Clase Principal Del ORM

    Parametros: Ruta A La Base De Datos

    Devuelve: Conexion Y Cursor.
    """
    conn = sqlite3.connect(database=db_path, timeout=timeout)
    try:
        if no_foreign:
            conn.execute('PRAGMA foreign_keys = OFF;')
        else:
            conn.execute('PRAGMA foreign_keys = ON;')
        logger.debug(f"Opened Connection: database {db_path}")
        conn.execute('PRAGMA journal_mode = WAL;')
        yield conn, conn.cursor()
        conn.commit()
        logger.debug('SQLite3 succeded commit action.')
    except Exception as e:
        conn.rollback()
        logger.error(
            f"Database error: {db_path} {e}, "
            "something wrong in process. "
            "SQLite3 rolleback completed."
        )
        raise e
    finally:
        conn.close()
        logger.debug(f"Closed Connection: database {db_path}.")

def clean_string(string: str):
    line = [char for char in string if char.isalnum() or char == '_']
    f_line = "".join(line)
    if f_line:
        return f_line
    else:
        msg = f'Invalid string passed for cleaning {string}.'
        logger.critical(msg)
        raise ValueError(msg)