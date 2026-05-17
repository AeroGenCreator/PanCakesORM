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
import logging

# Modulos Python
import warnings

from ..orm.query import query
from ..tools.functions import environment

# .envs; log, dir, db
envs = environment()
LOG = envs.get("log", "WARNING")

log_level = getattr(logging, LOG, logging.WARNING)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s [%(levelname)s] '
    '%(name)s.%(funcName)s:%(lineno)d - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)


class QueryBox:

    def __init__(self, model=None):
        self.model = model
        self.reset()

    def reset(self):
        self.label_dicc = {}
        self.s_label = []
        self.sp_label = []
        self.s_select = []
        self.sp_select = []
        self.join = None
        self.condition = None
        self.group = None
        self.order = None
        self.limit = None
        self.offset = None
        self.ids = False
        self.row = None
        self.col = None

    def raw(self, line_up: bool = False, label=False):
        """
        Devuelve una tupla de dos elementos; la data en crudo de un query:

        row, col = ...raw()

        Forzoso para obtner una salida de datos usando QueryBox.

        Paramestros:

        line_up: bool = False -> Alinea la salida de las filas;

        en lugar de una lista de filas:
        [(fila), (fila), (fila)]

        devuelve una lista de data de columna:
        [(columna), (columna), (columna)]

        label: bool = False -> Si es True; se devuelven las columnas
        con las etiquetas de frontend.
        """

        if not self.row and not self.col:
            return self.row, self.col

        row = self.row
        col = self.col

        # Obtencion de etiquetas frontend
        if label:
            comments = self.s_label + self.sp_label
            if comments:
                lab = comments
            if self.label_dicc:
                lab = self.label_dicc
            if isinstance(lab, list):
                col = comments
            if isinstance(lab, dict):
                chart = [lab.get(c) for c in col]
                col = chart

        # Validar nombres únicos, sino; numerar.
        if len(set(col)) != len(col):
            warnings.warn(
                f"Possible duplicate column names. {col}"
                "If you are using label=True, "
                "ensure you are not repeating "
                "the same name for the 'comment' argument "
                "when declaring table.",
                UserWarning
            )
            cache = []
            count = 0
            if not label:
                for c in col:
                    line = (
                        f"{c.split('__', 1)[0]}"
                        f"__{c.split('__', 1)[1]}__{count}"
                    )
                    cache.append(line)
                    count += 1
                col = cache
            for et in col:
                cache.append(f"{et} {count}")
                count += 1
            col = cache

        if line_up:
            row = list(zip(*row))

        self.reset()
        return row, col

    def to_json(self, label=False):
        """
        Convierte la salida defecto de un query SQLite3:

        listas de tuplas a -> JSON
        dict 'query': dict 'tabla': dict 'column': list[any]

        Forzoso para obtner una salida de datos usando QueryBox.

        Paramestros:

        label: bool = False -> Si es True; se devuelve el JSON
        con las etiquetas de frontend.
        """

        # Validacion de data
        if not self.row and not self.col:
            return {}

        # Extraccion de data, extraccion de respaldo 'columnas'
        row = self.row
        col = self.col
        backup = self.col

        # Obtencion de etiquetas frontend
        if label:
            comments = self.s_label + self.sp_label
            if comments:
                lab = comments
            if self.label_dicc:
                lab = self.label_dicc
            if isinstance(lab, list):
                col = comments
            if isinstance(lab, dict):
                chart = [lab.get(c) for c in col]
                col = chart

        # Validar nombres únicos, sino; numerar.
        if len(set(col)) != len(col):
            warnings.warn(
                f"Possible duplicate column names. {col}"
                "If you are using label=True, "
                "ensure you are not repeating "
                "the same name for the 'comment' argument "
                "when declaring table.",
                UserWarning
            )
            cache = []
            count = 0
            if not label:
                for c in col:
                    line = (
                        f"{c.split('__', 1)[0]}"
                        f"__{c.split('__', 1)[1]}__{count}"
                    )
                    cache.append(line)
                    count += 1
                col = cache
            for et in col:
                cache.append(f"{et} {count}")
                count += 1
            col = cache

        # Cabeceras de tabla
        tabls = [b.split("__", 1)[0] for b in backup]
        if label:
            heads = col
        # Sino etiquetas frontend; iterar sobre el respaldo
        else:
            heads = [b.split("__", 1)[1] for b in backup]

        # Si hay columnas y filas
        if row:

            # Validar alineación antes de la transpocisión:
            if len(row[0]) != len(col):
                msg = (
                    f"Length mismatch for query output {row}, {col}"
                )
                logger.critical(msg)
                raise ValueError

            # Transposción; obtener (tabla, columna)
            trans = list(zip(*row))

            # Fabricación de diccionario
            res = {}
            count = 0
            for count, (t, h) in enumerate(zip(tabls, heads)):
                tab_dicc = res.setdefault(t, {})
                tab_dicc[h] = list(trans[count])

            self.reset()
            return res

        else:
            res = {}
            count = 0
            for count, (t, h) in enumerate(zip(tabls, heads)):
                tab_dicc = res.setdefault(t, {})
                tab_dicc[h] = row

            self.reset()
            return res

    def to_dict(self, label=False):
        """
        Convierte la salida defecto de un query SQLite3:

        listas de tuplas a -> llave: valor; para cada celda de la tabla.
        list 'query': dict 'fila': llave 'columna': valor 'celda'

        Forzoso para obtner una salida de datos usando QueryBox.

        Paramestros:

        label: bool = False -> Si es True; se devuelve el JSON
        con las etiquetas de frontend.
        """

        if not self.row and not self.col:
            return []

        col = self.col
        row = self.row

        # Obtencion de etiquetas frontend
        if label:
            comments = self.s_label + self.sp_label
            if comments:
                lab = comments
            if self.label_dicc:
                lab = self.label_dicc
            if isinstance(lab, list):
                col = comments
            if isinstance(lab, dict):
                chart = [lab.get(c) for c in col]
                col = chart

        # Validar nombres únicos, sino; numerar.
        if len(set(col)) != len(col):
            warnings.warn(
                f"Possible duplicate column names. {col}"
                "If you are using label=True, "
                "ensure you are not repeating "
                "the same name for the 'comment' argument "
                "when declaring table.",
                UserWarning
            )
            cache = []
            count = 0
            if not label:
                for c in col:
                    line = (
                        f"{c.split('__', 1)[0]}"
                        f"__{c.split('__', 1)[1]}__{count}"
                    )
                    cache.append(line)
                    count += 1
                col = cache
            for et in col:
                cache.append(f"{et} {count}")
                count += 1
            col = cache

        # Lista diccionarios si: 'filas' y 'nombres unicos'
        if row:
            dicc = [dict(zip(col, r)) for r in row]

            self.reset()
            return dicc

        # Listar diccionarios si: 'nombres unicos'
        else:
            nones = []
            for c in col:
                nones.append(None)
            dicc = [dict(zip(col, nones))]

            self.reset()
            return dicc

    def _if_no_select(self) -> None:

        # Lista de kwargs para 'seleccion' y 'seleccion especial'
        s_res = []
        sp_res = None

        # Siempre: select simple
        for col in self.model._fields:

            # Mapeamos todas las etiquetas de tabla 'main' e 'id'
            id_key = f"{self.model._table}__{self.model._table}_id"
            if id_key not in self.label_dicc.keys():
                self.label_dicc[id_key] = f"{self.model._table.upper()} ID"
            lab = col.comment
            s_lab_key = f"{self.model._table}__{col._name}"
            self.label_dicc[s_lab_key] = lab

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

        # Validar uniones; selección de columnas 'join'.
        if self.join:
            # Obtencion de tablas 'join'.
            e_tabs = []
            for e in self.join:
                t1 = e.get("tab1", "")
                t2 = e.get("tab2", "")
                # Limpiamos manual repetidos.
                if t1 not in e_tabs and t1 != self.model._table:
                    e_tabs.append(t1)
                if t2 not in e_tabs and t2 != self.model._table:
                    e_tabs.append(t2)

            # Forzamos orden constante de aparición
            e_tabs.sort(reverse=False)

            # Validar existencia de tablas en la base de datos.
            s_tabs = set(e_tabs)
            t_tabs = set(self.model._family.keys())

            if s_tabs.issubset(t_tabs):

                sp_res = []
                for e in e_tabs:
                    # Cache fuerza a que columna 'id' siempre sea 1ra.
                    sp_cache = []

                    # Objeto PanCakesORM identificada por 'nombre tabla'
                    obj = self.model._family[e]

                    # Itera cada objeto columna de la tabla 'e'
                    for col in obj._fields:

                        # Etiquetas de comment mapeadas
                        sp_id_key = f"{e}__{e}_id"
                        if sp_id_key not in self.label_dicc.keys():
                            self.label_dicc[sp_id_key] = f"{e.upper()} ID"
                        lab = col.comment
                        sp_lab_key = f"{e}__{col._name}"
                        if sp_lab_key not in self.label_dicc.keys():
                            self.label_dicc[sp_lab_key] = lab

                        # Usamos 'names' para validar únicos
                        if f"{e}__{col._name}" in names:
                            continue

                        # Nombre compuesto de tablas "join"
                        if f"{e}__{e}_id" not in names:
                            sp_cache = (
                                [{"table": e, "name": f"{e}_id"}] + sp_cache
                            )
                            names.append(f"{e}__{e}_id")
                        dicc = {
                            "table": e,
                            "name": col._name,
                        }
                        sp_cache.append(dicc)
                        names.append(f"{e}__{col._name}")
                    sp_res.extend(sp_cache)

        self.s_select = s_res
        self.sp_select = sp_res

        return

    def select(self, *columns):

        if not columns:
            return self

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

                # Label; si seleccion "id"
                if col == f"{tab}_id":
                    if not agg:
                        lab = f"{tab} id".upper()
                    else:
                        lab = f"{tab} id {agg}".upper()
                else:
                    obj = self.model._family[tab]  # <- Buscamos la Tabla
                    # Obtenemos el comment:
                    lab = [c.comment for c in obj._fields if c._name == col]
                    lab = f"{"".join(lab)} {agg.upper()}"
                    lab = " ".join(lab.split(" ")).strip()

                self.sp_label.append(lab)

                dicc = {
                        "table": tab,
                        "name": col,
                        "agg": agg,
                    }
                self.sp_select.append(dicc)
                continue

            elif tab == m_tab:

                # Label; si seleccion "id"
                if col == f"{tab}_id":
                    if not agg:
                        lab = f"{tab} id".upper()
                    else:
                        lab = f"{tab} id {agg}".upper()

                # Iterar campos; si no selecciona id
                else:
                    # Obtenemos el comment desde la tabla main
                    lab = []
                    for c in self.model._fields:
                        if c._name == col:
                            lab.append(c.comment)
                    lab = f"{"".join(lab)} {agg.upper()}"
                    lab = " ".join(lab.split(" ")).strip()

                self.s_label.append(lab)

                dicc = {
                    "name": col,
                    "agg": agg
                }
                self.s_select.append(dicc)
                continue

        # Validar; no s_select, si sp_select
        if not self.s_select:
            m_tab = self.model._table
            name = self.model._table
            if "_" in name:
                name = name.split("_")
                name = " ".join(name)

            self.s_label.append(f"{name} id".upper())
            self.s_select = [
                {
                    "name": f"{m_tab}_id",
                    "agg": ""
                }
            ]

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
        #import ipdb; ipdb.set_trace()

        for k in keys:

            if "__" not in k:
                msg = f"Invalid key {k}."
                logger.critical(msg)
                raise KeyError

            if not isinstance(kwargs[k], (tuple, list)):
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
                msg = (
                    f"Invalid 'id reference' datatype: {udid}. "
                    "No relationship found. "
                    "Ensure you have related your tables "
                    "before using methods like .link(), .add(), "
                    "or the .query() function."
                )
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

        Ej: category="name"

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
        Equivalente a ORDER BY

        Ej: tabla__columna = "DESC"
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

    def off(self, offset: int):
        """
        Asigna un valor integer para seleccionar
        el comienzo del query.
        """
        if not isinstance(offset, int):
            msg = f"Invalid datatype: {offset}."
            logger.critical(msg)
            raise TypeError(type(offset))

        offset = offset if offset else None
        self.offset = offset
        return self

    def id(self):
        """
        Bandera que especifica al query devolver
        unicamente los ids.
        """
        self.ids = True
        return self

    def all(self, db_path=None, _from=None):

        # Validar ruta
        if db_path is None:
            path = self.model._db_file

        # Validar tabla; query
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
        offset = self.offset if self.offset else None

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
                "otherwise do not use .id() method.", UserWarning
            )
            s_select = [{"name": f"{_from}_id"}]
            sp_select = None

        if ids and not sp_select and not s_select:
            s_select = [{"name": f"{_from}_id"}]
            sp_select = None

        # Evaluar cuando .all() no contiene select():
        if s_select is None and sp_select is None:
            self._if_no_select()
            self.all()
            return self

        row, col = query(
            db_path=path,
            select=s_select,
            _from=_from,
            sp_select=sp_select,
            join=join,
            condition=condition,
            group_by=group,
            order_by=order,
            limit=limit,
            offset=offset
        )

        self.row = row
        self.col = col

        return self

    def link(self, *relation):
        """
        * Esta función imita **kwargs de add()
        tipo de union + __ + tabla extra = [id referencia, tabla]

        * Recibe; list[tabla1, tabla2, etc...]
        """

        # Si no hay datos, salir
        if not relation:
            return self

        by_dependency = False
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

        dicc_values = kwargs.values()
        for v in dicc_values:
            if v[0] is None or v[1] is None:
                by_dependency = True
                break

        if by_dependency:
            copy = list(relation)

            # Orden de relacion por dependencia:
            DEP = reversed(self.model._order)
            copy.insert(0, self.model._table)
            REL = [d for d in DEP if d in copy]
            winner = self.model

            # Iterar esquema de relación
            kwargs = {}
            for t in REL:

                # Bandera Validación
                flag = True

                TABS = REL
                MODEL = self.model._family[t]
                TABS.remove(MODEL._table)

                # Iteracion sobre los nombre de tablas dadas
                for rel in TABS:

                    # validar que se hayan pasado strings
                    if not isinstance(rel, str):
                        msg = (
                            f"Passed table name {rel} "
                            f"must be a string."
                        )
                        logger.critical(msg)
                        raise TypeError(type(rel))

                    field = None

                    # Iteramos los "objetos" tipo "campos" <- o sea columnas
                    # "_fields"
                    for f in MODEL._fields:

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
                            winner = MODEL
                            break

                    # Validamos que la operacion directa no encontro campos
                    # Pasamos a la operacion inversa (de tabla padre a hija).
                    if field is None:

                        # Buscamos relacion del PRIMARY KEY de la tabla
                        # A la FOREIGN KEY de la tabla hija
                        c_id = f"{MODEL._table}_id"
                        # Buscamos que la tabla exista en la base de datos
                        if rel not in MODEL._family:
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
                        tab2 = MODEL._family[rel]

                        for f in tab2._fields:
                            if c_id == f._name:
                                field = f._name
                                winner = MODEL
                                break

                    # Si no relación absoluta; reportar cambio de esquema
                    if field is None:
                        flag = False
                        break

                    # Agregamos la relacion al dicc "kwargs"
                    # Por defecto INNER JOIN
                    kwargs[f"in__{rel}"] = [field, MODEL._table]

                # Cambiamos el esquema de busqueda
                if flag is False:
                    continue
                else:
                    break

            # Si se encontraron nulos:
            self.model = winner
            self.add(**kwargs)
            return self

        # Si no hay nulos retornamos
        self.add(**kwargs)
        return self

    def count(self):
        self.select(
            f"{self.model._table}__{self.model._table}_id__count").all()

        return self
