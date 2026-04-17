# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""
Este fichero centraliza la funcion update()
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
log = os.getenv("LOG").upper()
log_level = getattr(logging, log, logging.WARNING)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s [%(levelname)s] '
    '%(name)s.%(funcName)s:%(lineno)d - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)


def update(
    db_path: str,
    params: list,
    update_all: bool = False
):
    """
    Parametros:

    -*- db_path -*-
    Ruta a la base de datos donde queremos realizar cambios.
    db_path = 'data/my_app_database.sqlite'

    -*- params -*-
    Lista de diccionarios que mapea las condiciones de actualizacion
    de datos SQL.

    params = [{
        'table':'user',     <- Tabla
        'name':'name',      <- Campo donde se realiza un cambio
        'data':'Raul'       <- Nuevos datos
        'condition':[{      <- Condicion (Lista de diccionarios)
            'column':'id',  <- Columna de condicion
            'operator':'=', <- Operador
            'value':'10',   <- Valor de condicion
            'logic':'',     <- Operador logico
        }]
    }]

    COMPS = (
        "=", "<", "<=", ">", ">=", "<>",
        "IN", "NOT IN",
        "BETWEEN",
        "IS", "IS NOT",
        "LIKE", "NOT LIKE"
    )
    LOGICS = ('AND', 'OR', '')

    -*- update_all -*-
    Buscara en el diccionario de 'params' unicamente las llaves:
    'table', 'name', 'data'. Y aplicara los cambios sobre la columna
    especificada en 'name' el valor pasado en 'data' para todas las filas
    de la tabla.

    Se pueden actulizar registros en varias tablas a la vez,
    pero solo se puede actualizar una base de datos a la vez.
    """
    COMPS = (
        "=", "<", "<=", ">", ">=", "<>",
        "IN", "NOT IN",
        "BETWEEN",
        "IS", "IS NOT",
        "LIKE", "NOT LIKE"
    )
    LOGICS = ('AND', 'OR', '')

    MIN_KEYS = {'table', 'name', 'data'}
    BAS_KEYS = MIN_KEYS | {'condition'}
    COMP_KEYS = {'column', 'operator', 'value'}
    LOG_KEYS = COMP_KEYS | {'logic'}

    if not isinstance(db_path, (str, Path)):
        msg = (f'Invalid datatype {db_path}.')
        logger.error(msg)
        raise TypeError(type(db_path))

    if not isinstance(params, list):
        msg = (f'Invalid datatype {params}.')
        logger.error(msg)
        raise TypeError(type(params))

    if not isinstance(update_all, bool):
        msg = (f'Invalid datatype {update_all}.')
        logger.error(msg)
        raise TypeError(type(update_all))

    sentences = []
    raw_data = []

    if not update_all:

        for s_info in params:
            s_cache = []

            if not isinstance(s_info, dict):
                msg = (
                    f'Argument "params" must be a list of '
                    f'dictionaries. {s_info}'
                )
                logger.error(msg)
                raise TypeError(type(s_info))

            if set(s_info.keys()) not in (MIN_KEYS, BAS_KEYS):
                msg = f'Invalid passed keys for argument "params" {params}.'
                raise KeyError(params)

            s_tab = clean_string(s_info.get('table', ''))
            s_name = clean_string(s_info.get('name', ''))
            s_data = s_info.get('data', '')
            s_con = s_info.get('condition', '')

            s_cache.append(s_data)

            if not s_con or not isinstance(s_con, (tuple, list)):
                msg = (
                    f'No condition specify for argument "condition" '
                    f'{s_con}. Or Invalid datatype.'
                )
                logger.error(msg)
                raise TypeError(type(s_con))

            s_condition = []
            for s_args in s_con:

                if not isinstance(s_args, dict):
                    msg = (
                        f'Conditions must be a list '
                        f'of dictionaries. {s_args}.'
                    )
                    logger.error(msg)
                    raise TypeError(type(s_args))

                if set(s_args.keys()) not in (COMP_KEYS, LOG_KEYS):
                    msg = (
                        f'Invalid keys passed for argument '
                        f'"conditions" {s_args}.'
                    )
                    logger.error(msg)
                    raise Exception(s_args)

                s_col = clean_string(s_args.get('column', ''))
                s_oper = s_args.get('operator', '').upper()
                s_val = s_args.get('value', '')
                s_logic = s_args.get('logic', '').upper()

                if s_oper not in COMPS:
                    msg = f'Invalid operator {s_oper}.'
                    logger.error(msg)
                    raise Exception(s_oper)

                if s_logic not in LOGICS:
                    msg = f'Invalid logic operator {s_logic}.'
                    logger.error(msg)
                    raise Exception(s_logic)

                if s_oper == 'BETWEEN' and len(s_val) != 2:
                    msg = (
                        f'Operator {s_oper} must have an iterable '
                        f'of 2 items for key "value" {s_val}.'
                    )
                    logger.error(msg)
                    raise TypeError(type(s_val))

                if s_oper == 'BETWEEN':
                    s_line = f"[{s_col}] BETWEEN ? AND ? {s_logic}"
                    s_condition.append(s_line)
                    s_cache.extend(s_val)
                    continue

                if s_oper in ('IN', 'NOT IN'):
                    if isinstance(s_val, (list, tuple, set)):
                        s_marks = f"({', '.join(['?'] * len(s_val))})"
                        s_line = f"[{s_col}] {s_oper} {s_marks} {s_logic}"
                        s_condition.append(s_line)
                        s_cache.extend(s_val)
                        continue

                    s_line = f"[{s_col}] {s_oper} (?) {s_logic}"
                    s_condition.append(s_line)
                    s_cache.append(s_val)
                    continue

                if isinstance(s_val, (int, float, str, bool)):
                    s_line = f"[{s_col}] {s_oper} ? {s_logic}"
                    s_condition.append(s_line)
                    s_cache.append(s_val)
                    continue

            str_cons = " ".join(s_condition)
            for log in LOGICS:
                if log and str_cons.endswith(log):
                    str_cons = str_cons.rsplit(logic, 1)[0].strip()

            line = (
                f"UPDATE [{s_tab}] "
                f"SET [{s_name}] = ? "
                f"WHERE {str_cons}"
            ).strip() + ";"

            sentences.append(line)
            raw_data.append(tuple(s_cache))

    else:
        for a_info in params:

            if len(a_info.keys()) != 3:
                msg = (
                    f'"update_all" argument needs "table", '
                    f'"name" and "data" keys only. {params}.'
                )
                logger.error(msg)
                raise Exception(params)

            if set(a_info.keys()) != MIN_KEYS:
                msg = (
                    f'Invalid keys passed for argument "params" {a_info}.'
                )
                logger.error(msg)
                raise KeyError(a_info)

            a_tab = clean_string(a_info.get('table', ''))
            a_name = clean_string(a_info.get('name', ''))
            a_data = a_info.get('data', '')

            a_line = (
                f"UPDATE [{a_tab}] "
                f"SET [{a_name}] = ?;"
            )
            sentences.append(a_line)
            raw_data.append(tuple([a_data]))

    with db_connection(db_path=db_path) as (conn, cur):
        for sql, val in zip(sentences, raw_data):
            cur.execute(sql, val)

    return True
