# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""
Este codigo centraliza la funcion de lectura avanzada query()
"""

# Modulos Propios
from ..tool.function import db_connection, clean_string, logger

# Modulos Python
from pathlib import Path


def query(
    db_path: str,
    select: str | list,
    _from: str,
    sp_select: list = None,
    join: list = None,
    condition: list = None,
    group_by: list = None,
    order_by: list = None,
    limit: int = None
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
    DIRECTION = {'ASC', 'DESC', ''}
    RELATION = {'INNER', 'LEFT', 'RIGHT'}
    OPERATOR = {
        "=", "<", "<=", ">", ">=", "<>",
        "IN", "NOT IN",
        "BETWEEN",
        "IS", "IS NOT",
        "LIKE", "NOT LIKE"
    }
    AGGREGS = {'MIN', 'MAX', 'SUM', 'COUNT', 'AVG', ""}
    LOGICS = {'AND', 'OR', ''}

    # LLAVES VALIDAS EN DICCIONARIOS
    S_MIN = {'name'}
    S_PLUS = S_MIN | {'agg'}
    S_WHOLE = {'all'}
    S_ALL_KEYS = S_PLUS | {'all'}

    SP_MIN = {'table', 'name'}
    SP_PLUS = SP_MIN | {'agg'}

    J_MIN = {'join', 'tab1', 'id1', 'tab2', 'id2'}

    C_MIN = {'table', 'column', 'operator', 'value'}
    C_PLUS = C_MIN | {'logic'}

    G_MIN = {'table', 'name'}

    O_MIN = {'table', 'name', 'order'}

    # VALIDAR EL TIPO DE DATO PARA LOS ARGUMENTOS
    if not isinstance(db_path, (str, Path)):
        msg = f"Invalid datatype: {db_path}."
        logger.critical(msg)
        raise TypeError(type(db_path))

    if not isinstance(select, (str, list, tuple)):
        msg = f"Invalid datatype: {select}."
        logger.critical(msg)
        raise TypeError(type(select))

    if not isinstance(_from, str):
        msg = f"Invalid datatype: {_from}."
        logger.critical(msg)
        raise TypeError(type(_from))

    # Limpiamos el string de "FROM"
    _from = clean_string(_from)

    # Contruccion lista del "SELECT" principal.
    s_all_confirm = False
    if isinstance(select, str) and select == "*":
        s_line = select
    else:
        s_line = []
        for s_info in select:

            if not isinstance(s_info, dict):
                msg = (
                    f"Argument 'select' must be a list of "
                    f"dictionaries: {s_info}."
                )
                logger.critical(msg)
                raise TypeError(type(s_info))

            if (
                set(s_info.keys()) not in
                (S_MIN, S_PLUS, S_WHOLE, S_ALL_KEYS)
            ):
                msg = f"Invalid Keys: {s_info}."
                logger.critical(msg)
                raise KeyError(s_info)

            s_all = s_info.get('all', '')
            if isinstance(s_all, bool) and s_all is True:
                s_all_confirm = True
                break

            s_agg = s_info.get('agg', '').upper()
            s_name = clean_string(s_info.get('name', ''))
            s_alias = f"{_from}__{s_name}"

            if s_agg != "" and s_agg not in AGGREGS:
                msg = f"Invalid agregation: {s_agg}."
                logger.critical(msg)
                raise ValueError(s_agg)

            if s_agg:
                s_line.append(
                    f"{s_agg}([{_from}].[{s_name}]) "
                    f"AS [{s_alias}]"
                )
                continue

            if s_name and not s_agg:
                s_line.append(
                    f"[{_from}].[{s_name}] "
                    f"AS [{s_alias}]"
                )

    # Contruccion lista del "SELECT" especial.
    sp_line = []
    if sp_select:

        if not isinstance(sp_select, list):
            msg = f"Invalid datatype: {sp_select}."
            logger.critical(msg)
            raise TypeError(type(sp_select))

        for sp_info in sp_select:

            if not isinstance(sp_info, dict):
                msg = (
                    f"Argument 'sp_select' must be a list of "
                    f"dictionaries: {sp_info}."
                )
                logger.critical(msg)
                raise TypeError(type(sp_info))

            if set(sp_info.keys()) not in (SP_MIN, SP_PLUS):
                msg = f"Invalid Keys: {sp_info}."
                logger.critical(msg)
                raise KeyError(sp_info)

            sp_agg = sp_info.get('agg', '').upper()
            sp_table = clean_string(sp_info.get('table', ''))
            sp_name = clean_string(sp_info.get('name', ''))
            sp_alias = f"{sp_table}__{sp_name}"

            if (
                not isinstance(sp_agg, str) or
                not isinstance(sp_table, str) or
                not isinstance(sp_name, str) or
                not isinstance(sp_alias, str)
            ):
                msg = (
                    f"Invalid datatype: {sp_agg}, "
                    f"{sp_table} or {sp_name}."
                )
                logger.critical(msg)
                raise TypeError

            if sp_name != "*":

                if sp_agg != "" and sp_agg not in AGGREGS:
                    msg = f"Invalid agregation: {sp_agg}."
                    logger.critical(msg)
                    raise ValueError(sp_agg)

                if sp_agg:
                    sp_pre = (
                        f"{sp_agg}([{sp_table}].[{sp_name}]) "
                        f"AS [{sp_alias}]"
                    )
                    sp_line.append(sp_pre)
                    continue

                sp_line.append(
                    f"[{sp_table}].[{sp_name}] "
                    f"AS [{sp_alias}]"
                )

            else:
                sp_line.append(f"[{sp_table}].*")

    # Contruccion sentencia 'SELECT'
    if s_line == "*":
        select_clause = f"SELECT * FROM [{_from}] "
    else:
        if s_all_confirm:
            s_head = ", ".join([f"[{_from}].*"] + sp_line)
        else:
            s_head = ", ".join(s_line + sp_line)
        select_clause = (
            f"SELECT {s_head} FROM [{_from}] "
        )

    # Construcciones de relaciones "JOIN"
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
                    f"Argument 'join' must be a list of "
                    f"dictionaries: {j_info}."
                )
                logger.critical(msg)
                raise TypeError(type(j_info))

            if set(j_info.keys()) != J_MIN:
                msg = f"Invalid Keys: {j_info}."
                logger.critical(msg)
                raise KeyError(j_info)

            j_rel = j_info.get('join', '').upper()
            j_tab1 = clean_string(j_info.get('tab1', ''))
            j_id1 = clean_string(j_info.get('id1', ''))
            j_tab2 = clean_string(j_info.get('tab2', ''))
            j_id2 = clean_string(j_info.get('id2', ''))

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

            c_tab = clean_string(c_info.get('table', ''))
            c_col = clean_string(c_info.get('column', ''))
            c_op = c_info.get('operator', '').upper()
            c_val = c_info.get('value', '')
            c_log = c_info.get('logic', '').upper()

            if c_op not in OPERATOR:
                msg = f'Invalid operator {c_op}.'
                logger.error(msg)
                raise ValueError(c_op)

            if c_log not in LOGICS:
                msg = f'Invalid operator {c_log}.'
                logger.error(msg)
                raise ValueError(c_log)

            if c_op == 'BETWEEN' and len(c_val) != 2:
                msg = (
                    f'Operator {c_op} must have an iterable '
                    f'of 2 items for key "value" {c_val}.'
                )
                logger.error(msg)
                raise TypeError(type(c_val))

            if c_op == 'BETWEEN':
                c_line.append(f"[{c_tab}].[{c_col}] BETWEEN ? AND ? {c_log}")
                c_data.extend(c_val)
                continue

            if c_op in ('IN', 'NOT IN'):
                if isinstance(c_val, (list, tuple, set)):
                    c_mark = f"({', '.join(['?'] * len(c_val))})"
                    c_line.append(
                        f"[{c_tab}].[{c_col}] {c_op} {c_mark} {c_log}"
                    )
                    c_data.extend(c_val)
                    continue

                c_line.append(
                    f"[{c_tab}].[{c_col}] {c_op} (?) {c_log}"
                )
                c_data.append(c_val)
                continue

            if isinstance(c_val, (int, float, str, bool)):
                c_line.append(
                    f"[{c_tab}].[{c_col}] {c_op} ? {c_log}"
                )
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

            g_tab = clean_string(g_info.get('table', ''))
            g_name = clean_string(g_info.get('name', ''))

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

            o_tab = clean_string(o_info.get('table', ''))
            o_name = clean_string(o_info.get('name', ''))
            o_order = o_info.get('order', '').upper()

            if o_order not in DIRECTION:
                msg = f'Invalid order {o_order}.'
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

    # Construccion del Query Final
    sql = (
        f"{select_clause} "
        f"{join_clause} "
        f"{where_clause} "
        f"{group_clause} "
        f"{order_clause} "
        f"{limit_clause};"
    )

    sql = " ".join(sql.split()).strip()

    # Conexion a la base de datos
    with db_connection(db_path=db_path) as (conn, cur):
        if c_data:
            res = cur.execute(sql, tuple(c_data))
            return res.fetchall(), [d[0] for d in cur.description]
        else:
            res = cur.execute(sql)
            return res.fetchall(), [d[0] for d in cur.description]
