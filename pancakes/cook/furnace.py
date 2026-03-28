# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""
Este fichero centraliza la funcion insert()
"""

# Modulos Propios
from ..tool.function import db_connection, clean_string, logger

# Modulos Python
from pathlib import Path


def insert(
    db_path: str,
    params: list
):
    """
    Parametros:

    -*- db_path -*-
    Ruta a la base de datos.

    -*- params -*-
    Lista de diccionarios:
    Debe contener las siguientes llaves "table", "data"

    params = [
        {
        'table':'user',     <- Nombre de tabla
        'data':[(1, 'Omar')]  <- Iterable de tuplas
        }
    ]

    Por cada columna en la tabla, un dato en el iterable:

    tabla: user ("id", "name") data: [(None, "Omar")]

    El id es un dato que SQLite3 maneja por si solo.
    Sin embargo se puede especificar si es necesario.
    """
    MIN_KEYS = {'table', 'data'}

    if not isinstance(db_path, (str, Path)):
        msg = f"Invalid datatype for 'db_path': {db_path}."
        logger.critical(msg)
        raise TypeError(type(db_path))

    if not isinstance(params, list):
        msg = f"Invalid datatype for 'db_path': {params}."
        logger.critical(msg)
        raise TypeError(type(params))

    sentence = []
    data = []

    for i_info in params:

        if not isinstance(i_info, dict):
            msg = f"Invalid datatype for 'db_path': {i_info}."
            logger.critical(msg)
            raise TypeError(type(i_info))

        if set(i_info.keys()) != MIN_KEYS:
            msg = f"Invalid keys for {i_info}."
            logger.critical(i_info)
            raise KeyError(i_info.keys())

        i_tab = clean_string(i_info.get('table', ''))
        i_data = i_info.get('data', '')

        if not i_tab:
            msg = f"Invalid characters for 'table'."
            logger.critical(msg)
            raise ValueError(i_tab)

        if not isinstance(i_data, list):
            msg = f"For key: 'data', INSERT expects a list of tuples."
            logger.critical(msg)
            raise TypeError(type(i_data))

        i_valid = all(isinstance(x, tuple) for x in i_data)
        if not i_valid:
            msg = f"For key 'data', INSERT expects a list of tuples."
            logger.critical(msg)
            raise TypeError(i_data)

        i_line = (
            f"INSERT INTO [{i_tab}] "
            f"VALUES({', '.join(['?'] * len(i_data[0]))});"
        )

        sentence.append(i_line)
        data.append(i_data)

    if not data:
        msg = f"'Command' INSERT contains no data."
        logger.critical(msg)
        raise ValueError(data)

    with db_connection(db_path=db_path) as (conn, cur):
        for sql, val in zip(sentence, data):
            cur.executemany(sql, val)
    return True
