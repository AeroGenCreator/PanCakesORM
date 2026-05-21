# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""
Este codigo centraliza la funcion de lectura avanzada query()
"""

# Modulos Propios
import logging

# Modulos Python
from pathlib import Path

from ..tools.functions import clean_string, db_connection, environment

envs = environment()
LOG = envs.get("log", "WARNING")
DEFAULT_DIR = envs.get("dir")
DEFAULT_DB_FILE = envs.get("db")

log_level = getattr(logging, LOG, logging.WARNING)
logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] "
    "%(name)s.%(funcName)s:%(lineno)d - %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)


def query(
    select: list,
    _from: str,
    db_path: str | None = None,
    join: list | None = None,
    condition: list | None = None,
    group_by: list | None = None,
    order_by: list | None = None,
    limit: int | None = None,
    offset: int | None = None,
):
    """
    Ejecuta una consulta SQL avanzada, sanitizada y dinámica.

    Construye sentencias SELECT complejas permitiendo el uso de joins,
    agregaciones y filtrado mediante estructuras de diccionarios.

    No soporta: HAVING, FOR, PARTITION BY, PIVOT, RECURSIVE CTE, CROSS APPLY.

    Params:

    -*- db_path -*- Ruta al archivo de base de datos (.db, .sqlite).
    :type db_path: str | Path

    -*- select -*- Especificación de columnas de la tabla principal.
        Si es '*', selecciona todo. Si es lista, usa diccionarios:
        [{'name': str, 'agg': str, 'all': bool}]
    :type select: str | list

    -*- _from -*- Nombre de la tabla de origen principal (T1).
    :type _from: str

    -*- sp_select -*- Columnas de tablas relacionadas (Joins).
        Estructura: [{'table': str, 'name': str, 'agg': str}]
        Soporta name='*' para traer todas las columnas de dicha tabla.
    :type sp_select: list, optional

    -*- join -*- Lista de uniones entre tablas.
        Estructura: [{'join': str, 'tab1': str, 'id1': str,
        'tab2': str, 'id2': str}]
        Joins permitidos: 'INNER', 'LEFT', 'RIGHT'.
    :type join: list, optional

    -*- condition -*- Filtros WHERE (sanitizados mediante parámetros ?).
        Estructura: [{'table': str, 'column': str, 'operator': str,
        'value': any, 'logic': str}]
        Operadores: '=', '>', 'IN', 'BETWEEN', 'LIKE', 'IS', etc.
    :type condition: list, optional

    -*- group_by -*- Agrupamiento de resultados.
        Estructura: [{'table': str, 'name': str}]
    :type group_by: list, optional

    -*- order_by -*- Ordenamiento de resultados.
        Estructura: [{'table': str, 'name': str, 'order': str}]
        Order: 'ASC', 'DESC' o ''.
    :type order_by: list, optional

    -*- limit -*- Cantidad máxima de registros a retornar.
    :type limit: int, optional

    :return: Una tupla con (lista de filas, lista de nombres de columnas).
    :rtype: tuple(list, list)
    :raises TypeError: Si los tipos de datos de los argumentos son inválidos.
    :raises ValueError: Si se usan operadores o agregaciones no permitidos.
    :raises KeyError: Si faltan llaves obligatorias en los diccionarios.
    """

    # LISTA BLANCA DE TERMINOS SQL
    DIRECTION = {"ASC", "DESC", ""}
    RELATION = {"INNER", "LEFT", "RIGHT"}
    OPERATOR = {
        "=",
        "<",
        "<=",
        ">",
        ">=",
        "<>",
        "IN",
        "NOT IN",
        "BETWEEN",
        "IS",
        "IS NOT",
        "LIKE",
        "NOT LIKE",
    }
    AGGREGS = {
        "MIN",
        "MAX",
        "SUM",
        "COUNT",
        "AVG",
        "DSUM",
        "DAVG",
        "DCOUNT",
        "DISTINCT",
        ""
    }
    DISTINCT = {
        "DSUM": "SUM(DISTINCT",
        "DAVG": "AVG(DISTINCT",
        "DCOUNT": "COUNT(DISTINCT",
        "DISTINCT": "DISTINCT("
    }
    LOGICS = {"AND", "OR", ""}

    # LLAVES VALIDAS EN DICCIONARIOS

    SELMINS = {"table", "name"}
    SELPLUS = SELMINS | {"agg"}

    J_MIN = {"join", "tab1", "id1", "tab2", "id2"}

    C_MIN = {"table", "column", "operator", "value"}
    C_PLUS = C_MIN | {"logic"}

    G_MIN = {"table", "name"}

    O_MIN = {"table", "name", "order"}

    # VALIDAR EL TIPO DE DATO PARA LOS ARGUMENTOS
    if not isinstance(select, (str, list, tuple)):
        msg = f"Invalid datatype: {select}."
        logger.critical(msg)
        raise TypeError(type(select))

    if not isinstance(_from, str):
        msg = f"Invalid datatype: {_from}."
        logger.critical(msg)
        raise TypeError(type(_from))

    # Limpiamos el string de "FROM"
    FROM = clean_string(_from)

    # CONTRUCCION DE SELECT DINAMICA
    SELECT_LINE = []
    if not isinstance(select, list):
        msg = f"Invalid datatype: {select}."
        logger.critical(msg)
        raise TypeError(type(select))

    for dicc in select:
        if not isinstance(dicc, dict):
            msg = f"Argument 'select' must be a list of dictionaries: {dicc}."
            logger.critical(msg)
            raise TypeError(type(dicc))

        if set(dicc.keys()) not in (SELMINS, SELPLUS):
            msg = f"Invalid Keys: {dicc}."
            logger.critical(msg)
            raise KeyError(dicc)

        AGG = dicc.get("agg", "").upper()
        TAB = clean_string(dicc.get("table", ""))
        COL = clean_string(dicc.get("name", ""))
        ALIAS = f"{TAB}__{COL}"

        if (
            not isinstance(AGG, str)
            or not isinstance(TAB, str)
            or not isinstance(COL, str)
            or not isinstance(ALIAS, str)
        ):
            msg = f"Invalid datatype: {AGG}, {TAB} or {COL}."
            logger.critical(msg)
            raise TypeError

        if AGG != "" and AGG not in AGGREGS:
            msg = f"Invalid agregation: {AGG}."
            logger.critical(msg)
            raise ValueError(AGG)

        if AGG:
            if AGG in DISTINCT:
                # Con DISTINCT
                AGG_LINE = (
                    f"{DISTINCT[AGG]} [{TAB}].[{COL}]) "
                    f"AS [{ALIAS}__{AGG.lower()}]"
                )
            else:
                # Para SUM, AVG, COUNT, MIN, MAX normales
                AGG_LINE = f"{AGG}([{TAB}].[{COL}]) AS [{ALIAS}__{AGG.lower()}]"

            SELECT_LINE.append(AGG_LINE)
        else:
            # Si no hay agregación, va la columna limpia
            SELECT_LINE.append(f"[{TAB}].[{COL}] AS [{ALIAS}]")

    # CLAUSULA SELECT
    SELECT = ", ".join(SELECT_LINE)
    select_clause = f"SELECT {SELECT} FROM [{FROM}] "

    # CONTRUCCION DE JOINS
    join_clause = ""

    if join:
        if not isinstance(join, list):
            msg = f"Invalid datatype: {join}."
            logger.critical(msg)
            raise TypeError(type(join))

        j_line = []
        for j_info in join:
            if not isinstance(j_info, dict):
                msg = (
                    f"Argument 'join' must be a list of dictionaries: {j_info}."
                )
                logger.critical(msg)
                raise TypeError(type(j_info))

            if set(j_info.keys()) != J_MIN:
                msg = f"Invalid Keys: {j_info}."
                logger.critical(msg)
                raise KeyError(j_info)

            j_rel = j_info.get("join", "").upper()
            j_tab1 = clean_string(j_info.get("tab1", ""))
            j_id1 = clean_string(j_info.get("id1", ""))
            j_tab2 = clean_string(j_info.get("tab2", ""))
            j_id2 = clean_string(j_info.get("id2", ""))

            if j_rel not in RELATION:
                msg = f"Invalid relation: {j_rel}."
                logger.critical(msg)
                raise ValueError(j_rel)

            j_sub_line = (
                f"{j_rel} JOIN [{j_tab1}] "
                f"ON [{j_tab1}].[{j_id1}] = [{j_tab2}].[{j_id2}] "
            )
            j_line.append(j_sub_line)

        join_clause = " ".join(j_line)

    # Construccion de condiciones "WHERE"
    where_clause = ""
    c_line = []
    c_data = []

    if condition:
        if not isinstance(condition, list):
            msg = f"Invalid datatype: {condition}."
            logger.critical(msg)
            raise TypeError(type(condition))

        for c_info in condition:
            if not isinstance(c_info, dict):
                msg = (
                    f"Argument 'condition' must be a list of "
                    f"dictionaries: {c_info}."
                )
                logger.critical(msg)
                raise TypeError(type(c_info))

            if set(c_info.keys()) not in (C_MIN, C_PLUS):
                msg = f"Invalid Keys: {c_info}."
                logger.critical(msg)
                raise KeyError(c_info)

            c_tab = clean_string(c_info.get("table", ""))
            c_col = clean_string(c_info.get("column", ""))
            c_op = c_info.get("operator", "").upper()
            c_val = c_info.get("value", "")
            c_log = c_info.get("logic", "").upper()

            if c_op not in OPERATOR:
                msg = f"Invalid operator {c_op}."
                logger.error(msg)
                raise ValueError(c_op)

            if c_log not in LOGICS:
                msg = f"Invalid operator {c_log}."
                logger.error(msg)
                raise ValueError(c_log)

            if c_op == "BETWEEN" and len(c_val) != 2:
                msg = (
                    f"Operator {c_op} must have an iterable "
                    f'of 2 items for key "value" {c_val}.'
                )
                logger.error(msg)
                raise TypeError(type(c_val))

            if c_op == "BETWEEN":
                c_line.append(f"[{c_tab}].[{c_col}] BETWEEN ? AND ? {c_log}")
                c_data.extend(c_val)
                continue

            if c_op in ("IN", "NOT IN"):
                if isinstance(c_val, (list, tuple, set)):
                    c_mark = f"({', '.join(['?'] * len(c_val))})"
                    c_line.append(
                        f"[{c_tab}].[{c_col}] {c_op} {c_mark} {c_log}"
                    )
                    c_data.extend(c_val)
                    continue

                c_line.append(f"[{c_tab}].[{c_col}] {c_op} (?) {c_log}")
                c_data.append(c_val)
                continue

            if isinstance(c_val, (int, float, str, bool)):
                c_line.append(f"[{c_tab}].[{c_col}] {c_op} ? {c_log}")
                c_data.append(c_val)
                continue

        str_cons = " ".join(c_line)
        for log in LOGICS:
            if log and str_cons.endswith(log):
                str_cons = str_cons.rsplit(log, 1)[0].strip()

        where_clause = f"WHERE {str_cons} "

    # Construccion de "GROUP BY"
    group_clause = ""
    g_line = []

    if group_by:
        if not isinstance(group_by, list):
            msg = f"Invalid datatype: {group_by}."
            logger.critical(msg)
            raise TypeError(type(group_by))

        for g_info in group_by:
            if not isinstance(g_info, dict):
                msg = (
                    f"Argument 'group_by' must be a list of "
                    f"dictionaries: {g_info}."
                )
                logger.critical(msg)
                raise TypeError(type(g_info))

            if set(g_info.keys()) != G_MIN:
                msg = f"Invalid Keys: {g_info}."
                logger.critical(msg)
                raise KeyError(g_info)

            g_tab = clean_string(g_info.get("table", ""))
            g_name = clean_string(g_info.get("name", ""))

            g_line.append(f"[{g_tab}].[{g_name}]")

        sub_g_line = ", ".join(g_line)
        group_clause = f"GROUP BY {sub_g_line} "

    # Construccion de "ORDER BY"
    order_clause = ""
    o_line = []

    if order_by:
        if not isinstance(order_by, list):
            msg = f"Invalid datatype: {order_by}."
            logger.critical(msg)
            raise TypeError(type(order_by))

        for o_info in order_by:
            if not isinstance(o_info, dict):
                msg = (
                    f"Argument 'order_by' must be a list of "
                    f"dictionaries: {o_info}."
                )
                logger.critical(msg)
                raise TypeError(type(o_info))

            if set(o_info.keys()) != O_MIN:
                msg = f"Invalid Keys: {o_info}."
                logger.critical(msg)
                raise KeyError(o_info)

            o_tab = clean_string(o_info.get("table", ""))
            o_name = clean_string(o_info.get("name", ""))
            o_order = o_info.get("order", "").upper()

            if o_order not in DIRECTION:
                msg = f"Invalid order {o_order}."
                logger.error(msg)
                raise ValueError(o_order)

            o_line.append(f"[{o_tab}].[{o_name}] {o_order}")
        order_clause = f"ORDER BY {', '.join(o_line)}"

    # Construccion de "LIMIT"
    limit_clause = ""

    if limit:
        if not isinstance(limit, int):
            msg = f"Invalid datatype: {limit}."
            logger.critical(msg)
            raise TypeError(type(limit))

        limit_clause = "LIMIT ?"
        c_data.append(limit)

    # CONSTRUCCIÓN 'OFFSET' CLAUSE
    offset_clause = ""

    if offset:
        if not isinstance(offset, int):
            msg = f"Invalid datatype: {offset}."
            logger.critical(msg)
            raise TypeError(type(offset))

        offset_clause = "OFFSET ?"
        c_data.append(offset)

    # Construccion del Query Final
    sql = (
        f"{select_clause} "
        f"{join_clause} "
        f"{where_clause} "
        f"{group_clause} "
        f"{order_clause} "
        f"{limit_clause} "
        f"{offset_clause};"
    )

    sql = " ".join(sql.split()).strip()

    # Validamos la ruta del query
    db_path = db_path if db_path else DEFAULT_DB_FILE

    if not isinstance(db_path, (str, Path)):
        msg = f"Invalid datatype: {db_path}."
        logger.critical(msg)
        raise TypeError(type(db_path))

    # Conexion a la base de datos
    with db_connection(db_path=db_path) as (conn, cur):
        if c_data:
            res = cur.execute(sql, tuple(c_data))
            return res.fetchall(), [d[0] for d in cur.description]
        else:
            res = cur.execute(sql)
            return res.fetchall(), [d[0] for d in cur.description]
