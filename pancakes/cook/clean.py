# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""
Este fichero centraliza la funcion delete()
"""

# Modulos Propios
from ..tool.function import db_connection, clean_string

# Modulos Python
from pathlib import Path
import logging
import os

# Modulos de Terceros
from dotenv import load_dotenv

# Configuracion de loggings; variables de entorno
load_dotenv()
log = os.getenv("LOG", "WARNING").upper()
log_level = getattr(logging, log, logging.WARNING)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s [%(levelname)s] '
    '%(name)s.%(funcName)s:%(lineno)d - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)


def delete(
    db_path: str,
    params: list,
    delete_all: bool = False,
    force: bool = False,
):

    """
    Parametros:

    -*- db_path -*-
    Ruta a la base de datos.

    -*- params -*-
    Lista de diccionarios con las siguientes llaves:
    params = [
        {
        'table':'user',     <- Tabla
        'condition':[       <- Lista de condiciones
                {
                'column',   <- Columna de condicion
                'operator', <- Operador de comparacion
                'value',    <- Valor de comparacion
                'logic'     <- Operador logico
                }
            ]
        }
    ]
    COMPS = [
        "=", "<", "<=", ">", ">=", "<>",
        "IN", "NOT IN",
        "BETWEEN",
        "IS", "IS NOT",
        "LIKE", "NOT LIKE"]
    LOGICS = ['AND', 'OR', ""]

    -*- delete_all -*-
    Unicamente toma la llave "tabla". Fuerza la limpieza completa
    en una tabla. (Borra todas las filas). (Fuerza la eliminacion de datos).

    -*- force -*-
    Fuerza el borrado de filas incluso aquellas con ForeignKey()
    constraint.
    """
    COMPS = [
        "=", "<", "<=", ">", ">=", "<>",
        "IN", "NOT IN",
        "BETWEEN",
        "IS", "IS NOT",
        "LIKE", "NOT LIKE"]
    LOGICS = ['AND', 'OR', ""]

    MIN_SET = {'table'}
    BASIC_SET = MIN_SET | {'condition'}

    MIN_CON = {'column', 'operator', 'value'}
    PLUS_CON = MIN_CON | {'logic'}

    if not isinstance(db_path, (str, Path)):
        msg = f'Argument "db_path" must be a string. {db_path}.'
        logger.error(msg)
        raise TypeError(type(db_path))

    if not all(isinstance(x, bool) for x in [delete_all, force]):
        msg = (
            f'Argument "delete_all" or "force" must be bools. '
            f'{delete_all, force}.'
        )
        logger.error(msg)
        raise TypeError(type(delete_all), type(force))

    if not isinstance(params, list):
        msg = (
            f'Argument params must be a list of dictionaries. {params}.'
        )
        logger.error(msg)
        raise TypeError(type(params))

    sentences = []
    all_data = []

    if not delete_all:

        for b_dict in params:
            b_cache_line = []
            b_cache_data = []

            if not isinstance(b_dict, dict):
                msg = (
                    f'Argument "params" must coint dictionaries. '
                    f'{b_dict}.'
                )
                logger.error(msg)
                raise TypeError(type(b_dict))

            if set(b_dict.keys()) != BASIC_SET:
                msg = f'Invalid passed keys. {b_dict}.'
                logger.error(msg)
                raise KeyError(b_dict)

            b_table = clean_string(b_dict.get('table', ''))
            b_condition = b_dict.get('condition', '')

            if not isinstance(b_condition, list):
                msg = (
                    f'Argument "condition" must be a list of '
                    f'dictionaries. {b_condition}.'
                )
                logger.error(msg)
                raise TypeError(type(b_condition))

            for b_args in b_condition:

                if not isinstance(b_args, dict):
                    msg = (
                        f'Argument "condition" must be a list of '
                        f'dictionaries. {b_condition}.'
                    )
                    logger.error(msg)
                    raise TypeError(type(b_condition))

                if set(b_args.keys()) not in (MIN_CON, PLUS_CON):
                    msg = f'Invalid passed keys. {b_args}.'
                    logger.error(msg)
                    raise KeyError(b_args)

                b_col = clean_string(b_args.get('column', ''))
                b_op = b_args.get('operator', '').upper()
                b_val = b_args.get('value', '')
                b_log = b_args.get('logic', '').upper()

                if b_log not in LOGICS:
                    msg = (
                        f'Invalid value passed for "logic". {b_log}.'
                    )
                    logger.error(msg)
                    raise KeyError(b_log)

                if b_op not in COMPS:
                    msg = (
                        f'Invalid value passed for "operator". {b_op}.'
                    )
                    logger.error(msg)
                    raise KeyError(b_op)

                if b_op == 'BETWEEN' and len(b_val) != 2:
                    msg = (
                        f'Operator "BETWEEN" must be complemented '
                        f'with an iterable of two values. {b_val}.'
                    )
                    logger.error(msg)
                    raise TypeError(type(b_val))

                if b_op == 'BETWEEN':

                    b_mark = '? AND ?'
                    b_line = f"[{b_col}] {b_op} {b_mark} {b_log}"
                    b_line = b_line.replace("  ", " ").strip()
                    b_cache_line.append(b_line)
                    b_cache_data.extend(b_val)
                    continue

                if isinstance(b_val, (str, bool, float, int)):

                    b_line = f"[{b_col}] {b_op} ? {b_log}"
                    b_line = b_line.replace("  ", " ").strip()
                    b_cache_line.append(b_line)
                    b_cache_data.append(b_val)
                    continue

                if b_op in ('IN', 'NOT IN'):
                    if isinstance(b_val, (list, tuple, set)):
                        b_mark = f"({', '.join(['?'] * len(b_val))})"
                        b_line = f"[{b_col}] {b_op} {b_mark} {b_log}"
                        b_line = b_line.replace("  ", " ").strip()
                        b_cache_line.append(b_line)
                        b_cache_data.extend(b_val)
                        continue

                    b_line = f"[{b_col}] {b_op} (?) {b_log}"
                    b_line = b_line.replace("  ", " ").strip()
                    b_cache_line.append(b_line)
                    b_cache_data.append(b_val)
                    continue

            del_clause = f"DELETE FROM [{b_table}] "
            sub_line = " ".join(b_cache_line)
            for logic in LOGICS:
                if logic and sub_line.endswith(logic):
                    sub_line = sub_line.rsplit(logic, 1)[0].strip()
            where_clause = "WHERE " + sub_line + ";"

            query = del_clause + where_clause

            sentences.append(query)
            all_data.append(tuple(b_cache_data))

    else:
        for a_dict in params:

            if set(a_dict.keys()) != MIN_SET:
                msg = (
                    f'Argument "delete_all" needs the following key '
                    f'only: "table". {a_dict}.'
                )
                logger.error(msg)
                raise KeyError(a_dict)

            a_table = clean_string(a_dict.get('table', ''))
            a_line = f"DELETE FROM [{a_table}];"
            sentences.append(a_line)
            all_data.append(None)

    with db_connection(db_path=db_path, no_foreign=force) as (conn, cur):
        for sql, data in zip(sentences, all_data):
            if data:
                cur.execute(sql, data)
            else:
                cur.execute(sql)
    return True
