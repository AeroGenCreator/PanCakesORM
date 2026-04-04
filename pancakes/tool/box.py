# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""
Declaracion De Clase QueryBox() Queries declarativos a traves
de **kwargs y encadenamiento de metodes.
"""

# Modulos Propios
from ..cook.layer import query
from .function import logger

# Modulos Python
import warnings


class QueryBox:

    def __init__(self, model=None):
        self.model = model
        self.reset()

    def reset(self):
        self.join = None
        self.condition = None
        self.group = None
        self.order = None
        self.limit = None
        self.ids = False

    def to_dict(self, rows: list, cols: list):
        """
        Convierte la salida defecto de un query SQLite3:
        listas de tuplas a -> lista de diccionarios.
        """

        # Se evalua la repeticion de nombres:
        # Si existen repeditos se agrega "*N"
        if len(cols) != len(set(cols)):
            warnings.warn(
                f"Repeated names for columns in: {cols}. "
                f"Try passed 'comment' attribute instead. "
                f"Make sure to add different strings for "
                f"'comment' argument when creating a table. "
                f"Special identifier '*N' has been added to "
                f"your column names.",
                UserWarning
            )
            count = 0
            c_col = []
            for c in cols:
                c_col.append(c + f"*{count}")
                count += 1

            dicc = [dict(zip(c_col, r)) for r in rows]

            return dicc

        # Si no hay nombres repetidos creamos la lista
        # de diccionarios
        dicc = [dict(zip(cols, r)) for r in rows]
        return dicc

    def filter(self, **kwargs):
        """
        Declaracion de filtros:
        tabla + __ + columna + __ + operador = data

        Ejemplo:
        client__name__same = "Omar"
        country__name__in = ["Mexico", "France"]

        Complejo:
        filter.(client__name__same__and = "Omar", client__age__btwn = [10, 20])

        SQL:
        WHERE client.name = "Omar"
        AND client.age BETWEEN 10 AND 20;

        Comparadores Validos:
        OPERATOR = {
        same: "=",
        lt: "<",
        ltsm: "<=",
        gt: ">",
        gtsm: ">=",
        diff:"<>",
        in: "IN",
        notin: "NOT IN",
        btwn: "BETWEEN",
        is: "IS",
        isnot: "IS NOT",
        like: "LIKE",
        notlike: "NOT LIKE"
        }

        LOGICS = {'AND', 'OR', ''}
        """

        OPERATOR = {
            "same": "=",
            "lt": "<",
            "ltsm": "<=",
            "gt": ">",
            "gtsm": ">=",
            "diff": "<>",
            "in": "IN",
            "notin": "NOT IN",
            "btwn": "BETWEEN",
            "is": "IS",
            "isnot": "IS NOT",
            "like": "LIKE",
            "notlike": "NOT LIKE"
        }
        LOGICS = {'AND', 'OR', ''}

        if not kwargs:
            return self

        res = []
        keys = kwargs.keys()

        for k in keys:

            if "__" not in k:
                msg = f"Invalid key {k}."
                logger.critical(msg)
                raise KeyError

        for k in keys:

            if not isinstance(
                kwargs[k],
                    (
                    list,
                    tuple,
                    str,
                    float,
                    int,
                    bool
                    )
            ):
                msg = (
                    f"Invalid datatype {kwargs[k]}. "
                    "Allowed datatypes: "
                    "(list, tuple, str, float, int, bool)."
                )
                logger.critical(msg)
                raise TypeError(type(kwargs[k]))

            arg = k.split("__")

            if len(arg) not in [3, 4]:
                msg = (
                    "Invalid statments passed in argument: "
                    f"{arg}. Pay attention to separator "
                    "'__'. At least 3 labels must be in "
                    "the argument."
                )
                logger.critical(msg)
                raise KeyError

            tab = arg[0]
            col = arg[1]
            con = arg[2]

            # Validar si hay comparador logico:
            # Y lista blanca:
            if len(arg) == 4:
                log = arg[3].upper()
                if log not in LOGICS:
                    msg = f"Invalid logic operator {log}."
                    logger.critical(msg)
                    raise KeyError
            else:
                log = ""

            # lista blanca del operador de comparacion:
            if con not in set(OPERATOR.keys()):
                msg = f"Invalid operator {con}."
                logger.critical(msg)
                raise KeyError

            if con == 'btwn':

                if not isinstance(kwargs[k], (list, tuple)):
                    msg = (
                        "Operator 'btwn' "
                        "requires datatypes (list, tuple)."
                        f"Passed: {kwargs[k]}"
                    )
                    logger.critical(msg)
                    raise TypeError(type(kwargs[k]))

                if len(kwargs[k]) != 2:
                    msg = (
                        f"'btwn' condition requires "
                        "a list of 2 values "
                        f"{kwargs[k]}."
                    )
                    logger.critical(msg)
                    raise ValueError

                dicc = {
                    'table': tab,
                    'column': col,
                    'operator': 'between',
                    'value': kwargs[k],
                    'logic': log,
                }
                res.append(dicc)
                continue

            if con in ['in', 'notin']:

                if not isinstance(kwargs[k], (list, tuple)):
                    msg = (
                        "Operador 'in' and 'notin' "
                        "requires datatypes (list, tuple)."
                        f"Passed: {kwargs[k]}"
                    )
                    logger.critical(msg)
                    raise TypeError(type(kwargs[k]))

                dicc = {
                    'table': tab,
                    'column': col,
                    'operator': OPERATOR[con],
                    'value': kwargs[k],
                    'logic': log,
                }
                res.append(dicc)
                continue

            if not isinstance(kwargs[k], (str, int, float, bool)):
                msg = (
                    f"Invalid datatype for argument: "
                    f"{kwargs[k]}."
                )
                logger.critical(msg)
                raise TypeError(type(kwargs[k]))

            dicc = {
                'table': tab,
                'column': col,
                'operator': OPERATOR[con],
                'value': kwargs[k],
                'logic': log,
            }

            res.append(dicc)
            continue

        self.condition = res
        return self

    def add(self, **kwargs):
        """
        Permite union de tabla de manera rapida a traves de **kwargs

        Los argumentos deben ser:
        tipo de union + __ + tabla extra = [id referencia, tabla]

        Uniones validas
        in = INNER
        rg = RIGHT
        lf = LEFT

        Ejemplo de argumento:
        _from = client
        in__country = ['country_id', 'client'] <- Doble Guion como separador

        SQL
        INNER JOIN country
        ON country.country_id = client.country_id

        Por tanto, si no se mantuvo la convencion de:
        Cada llave foranea declarada en alguna tabla (nombrarse como):

        tabla_2_id = sql_datatype.ForeignKey(tabla_2, tabla_2_id)

        Este metodo no funcionara por que la columna no existira.
        Y por tanto tendras que usar el metodo query()

        El cual es mas flexible pero mas verboso.
        """
        VALID_JOIN = {'in', 'rg', 'lf'}
        if not kwargs:
            return self

        res = []
        keys = kwargs.keys()

        for k in keys:
            if "__" not in k:
                msg = f"Invalid key {k}."
                logger.critical(msg)
                raise KeyError

        for k in keys:

            if not isinstance(kwargs[k], list):
                msg = (
                    f"Invalid datatype {kwargs[k]}. "
                    "Arguments must be a list "
                    "[union id, table]"
                )
                logger.critical(msg)
                raise TypeError(type(kwargs[k]))

            arg = k.split('__')
            join = arg[0].lower()
            plus = arg[1]

            if join not in VALID_JOIN:
                msg = (
                    f"Invalid union operator {join}. "
                    f"Valid ones are: {VALID_JOIN}."
                )
                logger.critical(msg)
                raise KeyError

            udid = kwargs[k][0]
            utab = kwargs[k][1]

            if not isinstance(udid, str):
                msg = f"Invalid 'id reference' datatype: {udid}. "
                logger.critical(msg)
                raise TypeError(type(udid))

            if not isinstance(utab, str):
                msg = f"Invalid 'table' datatype: {utab}. "
                logger.critical(msg)
                raise TypeError(type(utab))

            if join == 'in':
                dicc = {
                    'join': 'INNER',
                    'tab1': plus,
                    'id1': udid,
                    'tab2': utab,
                    'id2': udid
                }
                res.append(dicc)
                continue

            if join == 'lf':
                dicc = {
                    'join': 'LEFT',
                    'tab1': plus,
                    'id1': udid,
                    'tab2': utab,
                    'id2': udid
                }
                res.append(dicc)
                continue

            if join == 'rg':
                dicc = {
                    'join': 'RIGHT',
                    'tab1': plus,
                    'id1': udid,
                    'tab2': utab,
                    'id2': udid
                }
                res.append(dicc)
                continue
        self.join = res
        return self

    def gp(self, **kwargs):
        """
        Agrupar pasando el nombre de la tabla como argumento
        y el nombre de la columna como valor:

        Ejemplo

        sale = name
        """
        if not kwargs:
            return self

        res = []
        keys = kwargs.keys()

        for k in keys:

            if not isinstance(kwargs[k], str):
                msg = (
                    f"Invalid datatype: {kwargs[k]}. "
                    "values must be a string"
                )
                logger.critical(msg)
                raise TypeError(type(kwargs[k]))

            tab = k
            col = kwargs[k]

            dicc = {
                "table": tab,
                "name": col
            }

            res.append(dicc)

        self.group = res
        return self

    def sorts(self, **kwargs):
        """
        """
        DIRECTION = {'DESC', 'ASC', ''}

        if not kwargs:
            return self

        res = []
        keys = kwargs.keys()

        for k in keys:
            if "__" not in k:
                msg = f"Invalid key {k}."
                logger.critical(msg)
                raise KeyError

        for k in keys:

            if not isinstance(kwargs[k], str):
                msg = (
                    f"Invalid datatype {kwargs[k]}. "
                    "Arguments must be a string "
                )
                logger.critical(msg)
                raise TypeError(type(kwargs[k]))

            arg = k.split("__")
            tab = arg[0]
            col = arg[1]
            ordn = kwargs[k].upper()

            if ordn not in DIRECTION:
                msg = (
                    f"Invalid order passed {ordn}. "
                    f"Valid ones are {DIRECTION}"
                )
                logger.critical(msg)
                raise ValueError

            dicc = {
                "table": tab,
                "name": col,
                "order": ordn,
            }
            res.append(dicc)
        self.order = res
        return self

    def lim(self, limit):
        """
        Asigna un valor int para limitar el output
        del query.
        """
        if not isinstance(limit, int):
            msg = f"Invalid datatype {limit}."
            logger.critical(msg)
            raise TypeError(type(limit))

        limit = limit if limit else None
        self.limit = limit
        return self

    def id(self):
        """
        Bandera que especifica al query devolver
        unicamente los ids.
        """
        self.ids = True
        return self

    def all(self, db_path=None, _from=None):

        if db_path is None:
            db_path = self.model._db_file

        if _from is None:
            _from = self.model._table

        join = self.join if self.join else None
        condition = self.condition if self.condition else None
        group = self.group if self.group else None
        order = self.order if self.order else None
        limit = self.limit if self.limit else None
        ids = [{"name": f"{_from}_id"}] if self.ids else "*"

        row, col = query(
            db_path=db_path,
            select=ids,
            _from=_from,
            sp_select=None,
            join=join,
            condition=condition,
            group_by=group,
            order_by=order,
            limit=limit
        )

        if ids != "*":
            row = list(zip(*row))[0]

        self.reset()

        return row, col
