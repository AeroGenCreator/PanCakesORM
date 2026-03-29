# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""
Este codigo recopila la funcion de query avanzado.
"""

# Modulos Propios
from ..tool.function import db_connection

# Modulos Python
import logging

logging.basicConfig(
    level=logging.WARNING,  # Captura todo desde WARNING hacia arriba
    format='%(asctime)s [%(levelname)s] '
    '%(name)s.%(funcName)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)


def pancakes(
    db_path: str,
    select: str | list,
    from_: str,
    special_select: list = None,
    join: list = None,
    condition: list = None,
    group_by: list = None,
    order_by: list = None,
    limit: int = None
):
    """
    Esta funcion no soporta:
    HAVING,
    FOR,
    PARTITION BY,
    PIVOT,
    RECURSIVE CTE,
    CROSS APPLY

    PARAMETROS:

    -*- db_path -*- | string
    Ruta a la base de datos donde se desea realizar el query.

    -*- select -*- | list
    Lista de diccionarios que especifica columnas de tabla "main"
    y su funcion de agregacio, la funcion de agregacion puede ser
    omitida.
    O la estrella '*' para seleccionar todas las columnas

    select = '*'
    select = [
        {
        'agg': 'count',         <- Se puede agregar una funcion
        'column': 'name',
        'alias': True   <- Si desea el selector '*' unicamente para Tabla 1
        }
    ]

    ALLOWED_AGGREGATIONS = ('MIN', 'MAX', 'SUM', 'COUNT', 'AVG', "")

    -*- from_ -*- | str
    Tabla "main" que servira como el inicio del query

    from = 'curso'

    -*- special_select -*- | list
    Lista de diccionarios -> selecciona la funcion de agregacion, la tabla
    y el nombre de columna de un query relacional.
    Tambien admite se admite 'column':'*' <- Para todas las columnas.

    special_select = [
        {
        'agg': 'max'            <- Se puede agregar una funcion.
        'table': 'estudiante',
        'column': 'age'         'column':'*' <- Para todas las columnas.
        }
    ]

    ALLOWED_AGGREGATIONS = ('MIN', 'MAX', 'SUM', 'COUNT', 'AVG', "")

    -*- join -*- | list
    Lista de diccionarios, mapeando sentencias de relacion de tablas.
    ALLOWED_RELATIONS = ('INNER', 'LEFT', 'RIGHT')

    join = [
        {
        'join': 'inner',        <- Tipo de union.
        'extra': 'estudiante',  <- Tabla extra.
        'fkey': 'curso_id',     <- "ForeignKey" o columna id.
        'origin': 'curso',      <- Tabla de origin (Tabla Padre).
        'id': 'curso_id',       <- Columna "id" o columna ForeignKey.
        }
    ]

    El join, permite ordenar en ambos sentidos, por comodidad
    recomiendo usar la tabla hija como tabla extra, y la padre
    como tabla main.

    (PanCakesORM) nombra al indice de cualquier tabla como:
    nombre_de_tabla_id

    -*- condition -*- | list
    Lista de diccionarios que agreguen condicion a la consulta:
    ALLOWED_JOINS = ('AND', 'OR', "")
    ALLOWED_OPERATORS = (
        "=", "==", "<", "<=", ">", ">=", "!=", "",
        "IN", "NOT IN", "BETWEEN", "IS", "IS NOT", "LIKE",
    )

    condition = [
        {
        'table': ,              <- Tabla para la condicion
        'column': ,             <- Columna para la condicion
        'operator': ,           <- Operador de comparacion
        'value': ,              <- Valor (str, int, float, bool, list, tuple)
        'join': ,               <- Operador logico
        }
    ]

    # El operador logico se puede omitir si no hay necesidad de uno.

    -*- group_by -*- | list
    Lista de diccionario mapeando por que alias y columna agrupar
    la consulta.

    group_by = [
        {
        'table':'country',
        'column':'name'
        }
    ]

    -*- order_by -*- | list
    Lista de diccionarios para mapear orden:

    order_by = [
        {
        'table':'estudiante', <- Se puede omitir:
        'column':'name',
        'order'DESC'
        }
    ]
    Omitir 'table' cuando la tabla de la cual se toma la columna de
    referencia de orden es la main o sea T1 del argumento from_.

    -*- limit -*-
    Un integer para controlar el numero de filas devueltas por la
    consulta.

    limit = 5
    """

    ALLOWED_AGGREGATIONS = ('MIN', 'MAX', 'SUM', 'COUNT', 'AVG', "")
    ALLOWED_RELATIONS = ('INNER', 'LEFT', 'RIGHT')
    ALLOWED_OPERATORS = (
        "=", "<", "<=", ">", ">=", "<>",
        "IN", "NOT IN",
        "BETWEEN",
        "IS", "IS NOT",
        "LIKE", "NOT LIKE"
    )
    ALLOWED_JOINS = ('AND', 'OR', "")
    ALLOWED_ORDERS = ('ASC', 'DESC', '')

    # -*- MAIN SELECT -*-
    if not isinstance(from_, str):
        msg = (
            f'Table name "from_" {from_} must be a string. '
            f'Given {type(from_)}'
        )
        logger.error(msg)
        raise TypeError(msg)

    if not isinstance(select, (list, tuple, str)):
        msg = (
            f'"Select" argument invalid datatype. {select}. '
            f'Must be (str, list). Given {type(select)}'
        )
        logger.error(msg)
        raise TypeError(msg)

    s_alias_confirm = False
    s_line = [] if isinstance(select, (list, tuple)) else "*"
    if isinstance(s_line, list):
        for data in select:
            try:
                s_alias = data.get('alias')
            except AttributeError:
                continue
            if s_alias:
                s_alias_confirm = True
                break
        for data in select:
            try:
                s_agg = data.get('agg').upper()
            except AttributeError:
                s_agg = ""
            s_col = data.get('column')
            if s_agg:
                if s_agg in ALLOWED_AGGREGATIONS:
                    s_line.append(f"{s_agg}([{from_}].[{s_col}])")
                else:
                    msg = (
                        f'Invalid aggregation function {s_agg}.'
                    )
                    logger.error(msg)
                    raise ValueError(msg)
            else:
                s_line.append(f"[{from_}].[{s_col}]")
    select_head = s_line

    # -*- SPECIAL SELECT -*-
    select_body = []
    if special_select:
        if not isinstance(special_select, list):
            raise Exception(f"""
                Argument "special_select" must be a list of dictionaries
                {special_select}.
            """)
        sp_line = []
        for data in special_select:
            try:
                sp_agg = data.get('agg').upper()
            except AttributeError:
                sp_agg = ""
            sp_tab = data.get('table')
            sp_col = data.get('column')
            if not isinstance(sp_col, (str)):
                raise Exception(f"""
                    Invalid selector for "special_select" argument:
                    {sp_col}.
                """)
            if sp_col != '*':
                if sp_agg:
                    if sp_agg in ALLOWED_AGGREGATIONS:
                        sp_line.append(f"{sp_agg}([{sp_tab}].[{sp_col}])")
                    else:
                        raise Exception(f"""
                            Invalid aggregation function {sp_agg}
                        """)
                else:
                    sp_line.append(f"[{sp_tab}].[{sp_col}]")
            else:
                sp_line.append(f"[{sp_tab}].*")
        select_body.extend(sp_line)

    # -*- SELECT CLAUSE -*-
    partial_star = [True for star in select_body if "*" in star]
    if partial_star or s_alias_confirm:
        select_head = [f"[{from_}].*"]
    if isinstance(select_head, list):
        sub_select = ", ".join(select_head + select_body)
    else:
        sub_select = select_head

    select_clause = (
        f"""SELECT
        {sub_select}
        FROM [{from_}]"""
    )

    # -*- CLAUSULA DE RELACION -*-
    join_clause = ""
    if join:
        try:
            sentences = []
            for data in join:
                rel = data.get('join').upper()
                extra = data.get('extra')
                fkey = data.get('fkey')
                origin = data.get('origin')
                index = data.get('id')
                if rel.upper() not in ALLOWED_RELATIONS:
                    raise Exception(f"""
                        Invalid "join" Relation {rel}.
                        Valid ones: {ALLOWED_RELATIONS}
                    """)
                line = (f""" {rel} JOIN [{extra}]
                ON [{extra}].[{fkey}] = [{origin}].[{index}]
                """)
                sentences.append(line)
            join_clause = " ".join(sentences)
        except Exception as e:
            print(e)
            return

    # -*- CLAUSULA DE CONDICION -*-
    lines = []
    raw_data = []
    if condition and isinstance(condition, list):
        try:
            for data in condition:
                cache_data = []
                con_table = data.get('table')
                column = data.get('column')
                operator = data.get('operator').upper()
                value = data.get('value')
                try:
                    conj = data.get('join').upper()
                except AttributeError:
                    conj = ""
                if operator not in ALLOWED_OPERATORS:
                    raise Exception(
                        f"""Invalid Operator Try:
                        {ALLOWED_OPERATORS}"""
                    )
                elif conj not in ALLOWED_JOINS:
                    raise Exception(f"""
                        Values for 'join' not in allowed joins:
                        {ALLOWED_JOINS}. {conj}
                    """)
                if isinstance(value, (int, str, bool, float)):
                    cache_data.append(value)
                    marks = ", ".join(['?'] * len([value]))
                    raw_data.extend(cache_data)
                elif isinstance(value, (list, tuple)):
                    cache_data.extend(value)
                    marks = ", ".join(['?'] * len(cache_data))
                    raw_data.extend(cache_data)
                else:
                    raise Exception(
                        f""" No Integer or Interable was found in your
                        condition {data}: 'value' statement: {value}
                        """
                    )
                conj = f' {conj}' if conj else conj
                line = (f"""
                    [{con_table}].[{column}] {operator} ({marks}){conj}"""
                )
                lines.append(line)
            lines = " ".join(lines)
        except Exception as e:
            print(e)
            return
    where_clause = f" WHERE {lines}" if lines else ""

    # -*- GROUP BY SENTENCE -*-
    group_clause = ""
    if group_by and isinstance(group_by, list):
        try:
            g_line = []
            for data in group_by:
                g_tab = data.get('table')
                g_col = data.get('column')
                g_line.append(f'[{g_tab}].[{g_col}]')
            group_clause = ", ".join(g_line)
        except Exception as e:
            print(e)
            return
    group_clause = f' GROUP BY {group_clause}' if group_clause else ""

    # -*- ORDER BY SENTENCE -*-
    order_clause = []
    if order_by and isinstance(order_by, list):
        for data in order_by:
            try:
                o_table = data.get('table')
            except AttributeError:
                o_table = from_
            o_column = data.get('column')
            o_order = data.get('order').upper()
            if o_order in ALLOWED_ORDERS:
                order_clause.append(f'[{o_table}].[{o_column}] {o_order}')
            else:
                print(f"""
                    Invalid "order" keyword: {o_order}, allowed ones:
                    {ALLOWED_ORDERS}.
                """)
                return
    if order_clause:
        order_clause = f" ORDER BY {", ".join(order_clause)}"
    else:
        order_clause = ""

    # -*- LIMIT SENTENCE -*-
    limit_clause = ""
    if limit and isinstance(limit, int):
        limit_clause = " LIMIT ?"
        raw_data.append(limit)
    else:
        limit_clause = ""

    # -*- FINAL QUERY -*-
    query = (
        f"""
        {select_clause}
        {join_clause}
        {where_clause}
        {group_clause}
        {order_clause}
        {limit_clause};
        """
    )
    query = query.strip().replace("  ", " ")

    # -*- EXECUTE QUERY -*-
    with db_connection(db_path=db_path) as (conn, cur):
        if raw_data:
            res = cur.execute(query, tuple(raw_data))
            return res.fetchall(), [d[0] for d in cur.description]
        else:
            res = cur.execute(query)
            return res.fetchall(), [d[0] for d in cur.description]
