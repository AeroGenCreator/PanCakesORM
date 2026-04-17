# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
"""
1. Declaracion De Clase Context Manager: Inyeccion De Dependencia
(Conexion y Cursor) - Es Utilizada Por La Clase ORM Principal.

2. Funcion de Limpieza de strings
"""

# Modulos Python
import sqlite3
from contextlib import contextmanager
import logging
import os
from pathlib import Path

# Modulos de Terceros
from dotenv import load_dotenv

load_dotenv()

def environment():
    """
    Obtiene las variables de entorno
    -> LOG
    -> DB_DIR
    -> DB_FILE
    """
    
    log_valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    dot_valid = {".sqlite", "sqlite3", "db"}

    # Obtener log; validar log
    log = os.getenv("LOG", "WARNING").upper()
    if log not in log_valid:
        raise ValueError(
            f"Invalid Log value: {log}. "
            f"Valid ones are: {log}"
        )

    # Obtener rutas; validacion de rutas
    path_dir = os.getenv("DB_DIR", "data")
    path_file = os.getenv("DB_FILE", "database.sqlite")

    # Ruta: ("data/database.sqlite")
    dir_path = Path.cwd() / path_dir
    db_file = dir_path / path_file

    if db_file.suffix.lower() not in dot_valid:
        raise ValueError(
            f"Invalid extension {db_file}. "
            f"Expected exyensions are {dot_valid}."
        )

    envs = dict([("log", log), ("dir", dir_path), ("db", db_file)])
    
    return envs

# Configuracion de loggings
log = os.getenv("LOG", "WARNING").upper()
log_level = getattr(logging, log, logging.WARNING)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s [%(levelname)s] '
    '%(name)s.%(funcName)s:%(lineno)d - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)


@contextmanager
def db_connection(
    db_path: str,
    no_foreign: bool = False,
    timeout:  int = 10
):
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
    if string == "*":
        return string
    line = [char for char in string if char.isalnum() or char == '_']
    f_line = "".join(line)
    if f_line:
        return f_line
    else:
        msg = f'Invalid string passed for cleaning {string}.'
        logger.critical(msg)
        raise ValueError(msg)
