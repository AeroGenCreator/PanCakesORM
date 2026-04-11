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
        self.s_label = []
        self.sp_label = []
        self.s_select = []
        self.sp_select = []
        self.join = None
        self.condition = None
        self.group = None
        self.order = None
        self.limit = None
        self.ids = False
        self.row = None
        self.col = None

    def raw(self, line_up: bool = False, label=False):
        
        if not self.row or not self.col:
            return []

        row = self.row
        col = self.col

        comments = self.s_label + self.sp_label

        if label and len(comments) == len(col):
            col = comments

        if len(set(col)) != len(col):
            cache = []
            count = 0
            for c in col:
                line =(
                    f"{c.split('__', 1)[0]}"
                    f"__{c.split('__', 1)[1]}__{count}"
                )
                cache.append(line)
                count += 1
            col = cache

        if line_up:
            row = list(zip(*row))

        self.reset()
        return row, col

    def to_json(self, label=False):

        if not self.row or not self.col:
            return {}

        row = self.row
        col = self.col

        comments = self.s_label + self.sp_label

        if label and len(comments) == len(col):
            col = comments

        if len(row[0]) != len(col):
            msg = (
                f"Length mismatch for query output {row}, {col}"
            )
            logger.critical(msg)
            raise ValueError

        if len(set(col)) != len(col):
            cache = []
            count = 0
            for c in col:
                line =(
                    f"{c.split('__', 1)[0]}"
                    f"__{c.split('__', 1)[1]}__{count}"
                )
                cache.append(line)
                count += 1
            col = cache

        trans = list(zip(*row))
        tabls = [c.split("__", 1)[0] for c in col]
        heads = [c.split("__", 1)[1] for c in col]

        res = {}
        count = 0
        for count, (t, h) in enumerate(zip(tabls, heads)):
            tab_dicc = res.setdefault(t, {})
            tab_dicc[h] = list(trans[count])

        self.reset()
        return res

    def to_dict(self, label=False):
        """
        Convierte la salida defecto de un query SQLite3:
        listas de tuplas a -> lista de diccionarios.
        Forzoso para obtner una salida de datos usando QueryBox.
        """

        # Se evalua la repeticion de nombres:
        # Si existen repeditos se agrega " number"
        if not self.row or not self.col:
            return []

        comments = self.s_label + self.sp_label

        if label and len(comments) == len(col):
            col = comments

        if len(self.col) != len(set(self.col)):
            count = 0
            c_col = []
            for c in self.col:
                c_col.append(c + f" {count}")
                count += 1

            dicc = [dict(zip(c_col, r)) for r in self.row]

            self.reset()
            return dicc

        # Si no hay nombres repetidos creamos la lista
        # de diccionarios
        dicc = [dict(zip(self.col, r)) for r in self.row]

        self.reset()
        return dicc

    def _if_no_select(self) -> None:

        s_res = []
        sp_res = None
        
        # Siempre: select simple
        for col in self.model._fields:

            # Obtenemos todas las etiquetas de tabla main:
            if f"{self.model._table.title()} ID" not in self.s_label:
                self.s_label.append(f"{self.model._table.title()} ID")
            lab = col.comment
            self.s_label.append(lab)

            # Insertamos dicc de query simple (todas las columnas):
            dicc = {
                "name": col._name
            }
            s_res.append(dicc)
        s_res = [{"name": f"{self.model._table}_id"}] + s_res

        # Nombres compuestos de la tabla main:
        names = []
        for dicc in s_res:
            line = f"{self.model._table}__{dicc["name"]}"
            names.append(line)

        # 1. Evaluar si hay data en join
        # 2. Si hay: Select complejo
        if self.join:

            e_tabs = []  # Tablas con join
            for e in self.join:
                t1 = e.get("tab1", "")
                t2 = e.get("tab2", "")
                e_tabs.extend([t1, t2])

            # Validar que las tablas existan en las tablas de la
            # base de datos.
            e_tabs = set(e_tabs)
            t_tabs = set(self.model._family.keys())

            if e_tabs.issubset(t_tabs):

                sp_res = []
                for e in e_tabs:
                    sp_cache = []

                    # Clase completa de PanCakesORM identificada por su
                    # nombre de tabla.
                    obj = self.model._family[e]

                    for col in obj._fields:

                        # Etiquetas de comment
                        exp = f"{e.title()} ID"
                        if (
                            exp not in self.sp_label and
                            exp not in (self.s_label)
                        ):
                            self.sp_label.append(exp)
                        lab = col.comment
                        if lab not in self.s_label:
                            self.sp_label.append(lab)

                        # Si la tabla es main, skip
                        if f"{e}__{col._name}" in names:
                            continue

                        # Nombre compuesto de tablas "join"
                        dicc = {
                            "table": e,
                            "name": col._name,
                        }
                        sp_cache.append(dicc)
                        names.append(f"{e}__{col._name}")
                        if f"{e}__{e}_id" not in names:
                            sp_cache = (
                                [{"table": e, "name": f"{e}_id"}] + sp_cache
                            )
                            names.append(f"{e}__{e}_id")
                    sp_res.extend(sp_cache)

        self.s_select = s_res
        self.sp_select = sp_res

        return

    def select(self, *columns):
        AGGS = {
            "min": 'MIN',
            "max": 'MAX',
            "sum": 'SUM',
            "count": 'COUNT',
            "avg": 'AVG',
            "": ""
        }

        for c in columns:

            if not isinstance(c, str):
                msg = (
                    f"Invalid datatype: {c}. "
                    "Column names must be strings; sintax: "
                    "table__column__aggregation"
                )
                logger.critical(msg)
                raise TypeError(type(c))

            if "__" not in c:
                msg = (
                    f"Invalid sintax; {c}. "
                    "Valid separator: '__'."
                )
                logger.critical(msg)
                raise ValueError(c)

            arg = c.split("__")

            if len(arg) not in (2, 3):
                msg = (
                    f"Invalid quantity of arguments passed in {c}. "
                    "At least you must specify table + __ + column. "
                    "Or table + __ + column + __ + aggregation."
                )
                logger.critical(msg)
                raise ValueError(len(arg))

            agg = ""  # <- Por defecto no agregacion.
            if len(arg) != 2:
                agg = arg[2]  # <- Se extrae agregacion.
                if agg not in AGGS.keys():
                    msg = (
                        f"Invalid aggregation {agg}. "
                        f"Valid ones are {AGGS}."
                    )
                    logger.critical(msg)
                    raise ValueError(agg)

                agg = AGGS[agg]  # Indexacion por la agregacion valida.

            tab = arg[0]  # <- Se extrae tabla.
            col = arg[1]  # <- Se extrae columna.

            m_tab = self.model._table  # <- tabla main.

            if tab != m_tab:  # <- Si estamos seleccionando de union
                
                obj = self.model._family[tab]  # <- Buscamos la Tabla
                # Obtenemos el comment:
                if f"{tab.title()} ID" not in self.sp_label:
                    self.sp_label.append(f"{tab.title()} ID")
                lab = [c.comment for c in obj._fields if c._name == col]
                self.sp_label.extend(lab)

                dicc = {
                        "table": tab,
                        "name": col,
                        "agg": agg,
                    }
                self.sp_select.append(dicc)
                continue

            # Obtenemos el comment desde la tabla main
            if f"{m_tab.title()} ID" not in self.s_label:
                self.s_label.append(f"{m_tab.title()} ID")
            lab = [c.comment for c in self.model._fields if c._name == col]
            self.s_label.extend(lab)

            dicc = {
                "name": col,
                "agg": agg
            }
            self.s_select.append(dicc)

        return self

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

    def sort(self, **kwargs):
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

    def lim(self, limit: int):
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

        ids = f"{_from}_id" if self.ids else None
        s_select = self.s_select if self.s_select else None
        sp_select = self.sp_select if self.sp_select else None
        join = self.join if self.join else None
        condition = self.condition if self.condition else None
        group = self.group if self.group else None
        order = self.order if self.order else None
        limit = self.limit if self.limit else None

        ambiguous = (
            ids and s_select,
            ids and sp_select,
            ids and sp_select and s_select)
        if any(ambiguous):
            warnings.warn(
                "Ambiguous structure. "
                "requested; 'id' but specific columns "
                f"were passed: {s_select} : {sp_select}. "
                "Your query will continue; return 'ids' only. "
                "otherwise do not use .id() method.",UserWarning
            )
            s_select = [{"name":f"{_from}_id"}]
            sp_select = None

        if ids and not sp_select and not s_select:
            s_select = [{"name":f"{_from}_id"}]
            sp_select = None

        # Evaluar cuando .all() no contiene select():

        if not s_select:
            self._if_no_select()
            self.all()
            return self

        row, col = query(
            db_path=db_path,
            select=s_select,
            _from=_from,
            sp_select=sp_select,
            join=join,
            condition=condition,
            group_by=group,
            order_by=order,
            limit=limit
        )

        self.row = row
        self.col = col

        return self

    def link(self, *relation):

        # Si no hay datos, salir
        if not relation:
            return self

        # Obtener las relaciones en una lista de diccionarios
        # Imitan el **kwargs de .add()
        # tipo de union + __ + tabla extra = [id referencia, tabla]

        kwargs = {}

        # Iteracion sobre los nombre de tablas dadas
        for rel in relation:

            # validar que se hayan pasado strings
            if not isinstance(rel, str):
                msg = (
                    f"Passed table name {rel} "
                    f"must be a string."
                )
                logger.critical(msg)
                raise TypeError(type(rel))

            # Obtenecion de campo relacional
            field = None

            # Iteramos los "objetos" tipo "campos" <- o sea columnas
            # "_fields"
            for f in self.model._fields:

                # Evaluamos que el campo tenga el atributo:
                # "second_table".
                # Que el nombre dado y que el nombre del campo
                # sean iguales
                #
                # Se guarda y salimos de bucle
                if (
                    hasattr(f, "second_table") and
                    rel == f.second_table
                ):

                    field = f._name
                    break

            # Validamos que la operacion directa no encontro campos
            # Pasamos a la operacion inversa (de tabla padre a hija).
            if not field:
                
                # Buscamos relacion del PRIMARY KEY de la tabla
                # A la FOREIGN KEY de la tabla hija
                c_id = f"{self.model._table}_id"
                # Buscamos que la tabla exista en la base de datos
                if rel not in self.model._family:
                    msg = (
                        f"Passed {rel} tables does not exists. "
                        "Make sure of the spelling and "
                        "make sure of relation before "
                        "trying again."
                    )
                    logger.critical(msg)
                    raise KeyError(rel)
                
                # Si existe accedemos a la tabla entera
                # e iteramos sus campos
                tab2 = self.model._family[rel]

                for f in tab2._fields:
                    if c_id == f._name:
                        field = f._name
                        break

            # Agregamos la relacion al dicc "kwargs"
            # Por defecto INNER JOIN
            kwargs[f"in__{rel}"] = [field, self.model._table]

        # Ejecutamos la union
        # Desempaquetamos el diccionario:
        self.add(**kwargs)

        return self

    def count(self):
        self.select(
            f"{self.model._table}__{self.model._table}_id__count").all()
        
        return self