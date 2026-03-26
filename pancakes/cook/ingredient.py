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
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.WARNING,  # Captura todo desde WARNING hacia arriba
    format='%(asctime)s [%(levelname)s] '
    '%(name)s.%(funcName)s:%(lineno)d - %(message)s'
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

    OPERATORS = (
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
    OPERATORS = (
        "=", "<", "<=", ">", ">=", "<>",
        "IN", "NOT IN",
        "BETWEEN",
        "IS", "IS NOT",
        "LIKE", "NOT LIKE"
    )
    LOGICS = ('AND', 'OR', '')
    MIN_VALID_KEYS = ['table', 'name', 'data']
    PLUS_VALID_KEYS = MIN_VALID_KEYS + ['condition']
    S_CONDITION_KEYS = ['column', 'operator', 'value']
    S_CON_OPTIONAL_KEYS = S_CONDITION_KEYS + ['logic']

    if not isinstance(db_path, (str, Path)):
        msg = (f'Invalid datatype {db_path}.')
        logger.critical(msg)
        raise TypeError(msg)

    if not isinstance(params, list):
        msg = (f'Invalid datatype {params}.')
        logger.critical(msg)
        raise TypeError(msg)

    if not isinstance(update_all, bool):
        msg = (f'Invalid datatype {update_all}.')
        logger.critical(msg)
        raise TypeError(msg)

    if not update_all:

        SET_BASIC = set(MIN_VALID_KEYS)
        SET_PLUS = set(PLUS_VALID_KEYS)

        CON_BASIC = set(S_CONDITION_KEYS)
        CON_PLUS = set(S_CON_OPTIONAL_KEYS)

        s_sentences = []
        s_package = []
        for s_info in params:
            s_cache = []
            if not isinstance(s_info, dict):
                msg = f'Argument "params" must be a list of dictionaries.'
                logger.error(msg)
                raise TypeError(msg)
            if set(s_info.keys()) not in (SET_BASIC, SET_PLUS):
                msg = f'Invalid keys passed for argument "params" {params}.'
                raise Exception(msg)

            s_tab = clean_string(s_info.get('table', ''))
            s_name = clean_string(s_info.get('name', ''))
            s_data = s_info.get('data', '')
            s_con = s_info.get('condition', '')

            s_main_line = f"UPDATE [{s_tab}] SET [{s_name}] = ?"
            s_cache.append(s_data)

            if not s_con or not isinstance(s_con, (tuple, list)):
                msg = (
                    f'No condition specify for argument "condition" '
                    f'{s_con}. Or Invalid datatype.'
                )
                logger.error(msg)
                raise Exception(msg)

            s_condition = []
            for s_args in s_con:
                if not isinstance(s_args, dict):
                    msg = (
                        f'Conditions must be a list '
                        f'of dictionaries. {s_args}.'
                    )
                    logger.error(msg)
                    raise TypeError(msg)

                if set(s_args.keys()) not in (CON_BASIC, CON_PLUS):
                    msg = (
                        f'Invalid keys passed for argument '
                        f'"conditions" {s_args}.'
                    )
                    logger.error(msg)
                    raise Exception(msg)

                s_col = clean_string(s_args.get('column', ''))
                s_oper = s_args.get('operator', '').upper()
                s_valu = s_args.get('value', '')
                s_logic = s_args.get('logic', '').upper()

                if s_oper not in OPERATORS:
                    msg = f'Invalid operator {s_oper}.'
                    logger.error(msg)
                    raise Exception(msg)

                if s_logic not in LOGICS:
                    msg = f'Invalid logic operator {s_logic}.'
                    logger.error(msg)
                    raise Exception(msg)

                if s_oper == 'BETWEEN' and len(s_valu) != 2:
                    msg = (
                        f'Operator {s_oper} must have an iterable '
                        f'of 2 items for key "value" {s_valu}.'
                    )
                    logger.error(msg)
                    raise TypeError(msg)

                if s_oper in ('NOT IN', 'IN'):
                    s_marks = ", ".join(["?"] * len(s_valu))
                    s_marks = f"({s_marks})"
                    s_cache.extend(s_valu)
                elif s_oper == 'BETWEEN':
                    s_marks = '? AND ?'
                    s_cache.extend(s_valu)
                else:
                    s_marks = "?"
                    s_cache.append(s_valu)

                s_con_line = (
                    f' [{s_col}] {s_oper} {s_marks} '
                    f' {s_logic}'
                )

                s_condition.append(s_con_line)

            s_package.append(s_cache)
            s_condition = " WHERE" + " ".join(s_condition)
            final_line = s_main_line + s_condition
            final_line = final_line.replace("  ", " ").strip()
            final_line += ";"
            s_sentences.append(final_line)

        with db_connection(db_path=db_path) as (conn, cur):
            for sql, val in zip(s_sentences, s_package):
                cur.execute(sql, tuple(val))

    if update_all:
        MIN_SET = set(MIN_VALID_KEYS)
        a_sentence = []
        a_package = []
        for a_info in params:
            if len(a_info.keys()) != 3:
                msg = (
                    f'"update_all" argument needs "table", '
                    f'"name" and "data" keys only. {params}.'
                )
                logger.error(msg)
                raise Exception(msg)
            if set(a_info.keys()) != MIN_SET:
                msg = (
                    f'Invalid keys passed for argument "params" {a_info}.'
                )
                logger.error(msg)
                raise KeyError(msg)
            a_tab = clean_string(a_info.get('table', ''))
            a_name = clean_string(a_info.get('name', ''))
            a_data = a_info.get('data', '')

            a_line = (
                f"UPDATE [{a_tab}] "
                f"SET [{a_name}] = ?;"
            )
            a_sentence.append(a_line)
            a_package.append([a_data])
        with db_connection(db_path=db_path) as (conn, cur):
            for sql, val in zip(a_sentence, a_package):
                cur.execute(sql, tuple(val))
