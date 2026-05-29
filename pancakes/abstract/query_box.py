# ==============================================================================
# PROJECT:       PanCakesORM
# VERSION:       5.0.0
# AUTHOR:        AeroGenCreator
# GITHUB:        https://github.com/AeroGenCreator
# LICENSE:       Apache License 2.0
# ==============================================================================
# Copyright (c) 2026 AeroGenCreator. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
# DESCRIPTION:
# [Abtraccion de queries a traves de encadenamiento de metodos.]
# ==============================================================================

# Modulos Propios
import logging

from ..orm.query import query
from ..sql.datatype import ForeignKey
from ..tools.functions import environment

# .envs; LOGS, Directory, Database file
envs = environment()
LOG = envs.get("log", "WARNING")

log_level = getattr(logging, LOG, logging.WARNING)
logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] "
    "%(name)s.%(funcName)s:%(lineno)d - %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

def _NOT_DUPLICATED_LABELS_(labels: list, columns: list):

    # VALIDAR ETIQUETAS UNICAS
    if len(set(labels)) != len(labels):
        logger.warning(
            f"Possible duplicate column names. {labels}"
            "If you are using label=True, "
            "ensure you are not repeating "
            "the same name for the 'comment' argument "
            "when declaring table."
        )
        FIXED = []
        COUNT = 0
        for COL in columns:
            FIXED.append(
                f"{COL.split('__', 1)[0]}__{COL.split('__', 1)[1]}__{COUNT}"
            )
            COUNT += 1
        columns = FIXED
        return columns
    else:
        columns = labels
        return columns


class QueryBox:
    """
    QueryBox v2.
    Mejora en la declaracion de queries.
    Ahora se permite DISTINCT
    Filtros declarativos
    Joins declarativos
    Manejo de NO SELECT
    Manejo dinamico de GROUP BY
    Una sola función para LIMIT y OFFSET
    Ids obtenidos desde el ejecutable
    SALIDAS:
    - raw
    - dictionary
    - container (Perfecta integracion con flet)
    """

    # Pasar MODELO "instanciado" es obligatorio
    def __init__(self, model):
        self.model = model
        self.reset()

    def reset(self):
        self.SE_LABEL = []
        self.SE_SELECT = []
        self.FROM = None
        self.JOIN = []
        self.FILTER = []
        self.GROUP = []
        self.ORDER = []
        self.LIMIT = None
        self.OFFSET = None
        self.ROW = []
        self.COL = []

    def select(self, *select):
        """
        Sintaxis de seleccion:

        1. 'tabla__columna', ...
        2. 'tabla__columna__Agregacion', ...

        Agregaciones validas:

        {
            "min": "MIN",
            "max": "MAX",
            "sum": "SUM",
            "count": "COUNT",
            "avg": "AVG",
            "dsum": "DSUM",
            "davg": "DAVG",
            "dcount": "DCOUNT",
            "distinct": "DISTINCT",
            "": ""
        }
        """

        # Agregaciones validas
        AGGS = {
            "min": "MIN",
            "max": "MAX",
            "sum": "SUM",
            "count": "COUNT",
            "avg": "AVG",
            "dsum": "DSUM",
            "davg": "DAVG",
            "dcount": "DCOUNT",
            "distinct": "DISTINCT",
            "": "",
        }

        # Modelo
        MODEL = self.model

        # Validar select:
        if not select:
            return self

        # Tablas en base de datos
        DB_TABLES = list(MODEL._family.keys())
        ACTUAL_TB = MODEL._table

        # Iterar select
        for col in select:
            col = col.lower()

            # Validar estructura de select
            validate = (not isinstance(col, str), ("__" not in col))
            if any(validate):
                logger.critical(
                    "Invalid syntax passed in selection. "
                    "To declara a selection you must specify: "
                    "table__column__aggregation. "
                    f"Error found in {col}"
                )
                raise ValueError

            # Partir select en segmentos
            PARTS = col.split("__")

            # Validar cantidad de partes
            if len(PARTS) not in (2, 3):
                logger.critical(
                    "Invalid quantity of arguments passed in select. "
                    "You must separate arguments with '__'. "
                    "Syntax: TABLE__COLUMN__AGGREGATION or "
                    "TABLE__COLUMN"
                )
                raise ValueError

            # Extraer y validar agregacion, sino ""
            AGG = ""
            if len(PARTS) == 3:
                AGG = PARTS[2]
                if AGG not in AGGS:
                    logger.critical(
                        "Invalid aggregation function passed in select. "
                        f"Column: {col}, agregation: {AGG}"
                    )
                    raise ValueError
                AGG = AGGS[AGG]

            # Extraccion de TABLA y COLUMNA
            TAB = PARTS[0]
            COL = PARTS[1]

            # SELECCION SIMPLE
            if TAB == ACTUAL_TB:
                # Obtener etiquetas frontend
                MAPPED = dict(
                    zip(
                        MODEL._family[TAB]._metadata[TAB]["columns"],
                        MODEL._family[TAB]._metadata[TAB]["comments"],
                    )
                )
                LAB = MAPPED[COL]
                self.SE_LABEL.append(" ".join(f"{LAB} {AGG.upper()}".split()))

                # Seleccion simple resultado
                self.SE_SELECT.append({"table": TAB, "name": COL, "agg": AGG})

            # SELECCION ESPECIAL
            elif TAB in DB_TABLES:
                # Obtener etiquetas frontend
                MAPPED = dict(
                    zip(
                        MODEL._family[TAB]._metadata[TAB]["columns"],
                        MODEL._family[TAB]._metadata[TAB]["comments"],
                    )
                )
                LAB = MAPPED[COL]
                self.SE_LABEL.append(" ".join(f"{LAB} {AGG.upper()}".split()))

                # Seleccion especial resultado
                self.SE_SELECT.append({"table": TAB, "name": COL, "agg": AGG})

            # TAB no existe en la base de datos
            else:
                logger.critical(
                    f"The following table: {TAB} does not exist in Database. "
                    "Make sure passed arguments exist and proper syntax "
                    "using '__' as separator."
                )
                raise ValueError

        return self

    def _IDS_(self, main_table) -> None:

        COLUMN_ID = f"{main_table}_id".lower()
        LABELS_LIST = (
            self.model._family[main_table]
            ._metadata[main_table]["comments"]
            .copy()
        )
        INDEX = LABELS_LIST.index(f"{main_table} id".upper())
        self.SE_SELECT = [
            {"table": main_table, "name": COLUMN_ID, "agg": "DISTINCT"}
        ]
        self.ORDER = [{"table": main_table, "name": COLUMN_ID, "order": "ASC"}]
        self.SE_LABEL = [LABELS_LIST[INDEX]]

        return

    def _NO_SELECT_(self):

        MODEL = self.model
        MAINT = self.model._table

        # OBTENER TODAS LAS ETIQUETAS Y COLUMNAS SELECT SIMPLE
        SE_MAPPED = dict(
            zip(
                MODEL._family[MAINT]._metadata[MAINT]["columns"],
                MODEL._family[MAINT]._metadata[MAINT]["comments"],
            )
        )

        COLUMNS = list(SE_MAPPED.keys())
        COMMENTS = list(SE_MAPPED.values())

        self.SE_LABEL.extend(COMMENTS)

        for COL in COLUMNS:
            SQL = MODEL._metadata[MAINT]["schema"][COL]["metadata"]["sql_type"]
            if SQL != "1:N":
                self.SE_SELECT.append({"table": MAINT, "name": COL})

        # OBTENER TODO DE UN JOIN
        CACHE = []
        if self.JOIN:
            # TABLAS EXTRAS EN ARGUMENTOS DE JOIN
            for join in self.JOIN:
                TAB1 = join.get("tab1", "")
                TAB2 = join.get("tab2", "")

                if TAB1 not in CACHE and TAB1 != MAINT:
                    CACHE.append(TAB1)

                    CHART1 = dict(
                        zip(
                            MODEL._family[TAB1]._metadata[TAB1]["columns"],
                            MODEL._family[TAB1]._metadata[TAB1]["comments"],
                        )
                    )

                    COLS1 = list(CHART1.keys())
                    LABS1 = list(CHART1.values())

                    listado1 = []
                    for COL in COLS1:
                        SQL = MODEL._metadata[TAB1]["schema"][COL]["metadata"]["sql_type"]
                        if SQL != "1:N":
                            listado1.append({"table": TAB1, "name": COL})

                    self.SE_LABEL.extend(LABS1)
                    self.SE_SELECT.extend(listado1)

                if TAB2 not in CACHE and TAB2 != MAINT:
                    CACHE.append(TAB2)

                    CHART2 = dict(
                        zip(
                            MODEL._family[TAB2]._metadata[TAB2]["columns"],
                            MODEL._family[TAB2]._metadata[TAB2]["comments"],
                        )
                    )

                    COLS2 = list(CHART2.keys())
                    LABS2 = list(CHART2.values())

                    listado2 = []
                    for COL in COLS2:
                        SQL = MODEL._metadata[TAB2]["schema"][COL]["metadata"]["sql_type"]
                        if SQL != "1:N":
                            listado1.append({"table": TAB1, "name": COL})
                        listado2.append({"table": TAB2, "name": COL})

                    self.SE_LABEL.extend(LABS2)
                    self.SE_SELECT.extend(listado2)

    def _DYNAMIC_GROUP_(self):

        SELECT = self.SE_SELECT.copy()

        # VALIDACION COLUMNAS DE SELECT
        GROUP = []
        for dicc in SELECT:
            AGG = dicc.get("agg", "")
            if not AGG:
                TAB = dicc.get("table")
                COL = dicc.get("name")

                GROUP.append({"table": TAB, "name": COL})

        self.GROUP = GROUP
        return

    def add(self, **kwargs):
        """
        SINTAXIS:

        EL modelo desde el cual se invoca el metodo add() NO SERA TOMADO,
        por tanto es necesario especificar union usando una sintaxis SQL

        EJ;
        SELECT * FROM sale INNER JOIN client ... requiere de;
        modeloX.add(sale__inner__client=client_id).

        De esta manera de izquierda a derecha la primera tabla pasada
        se tomara como el FROM.

        Metodo add()
        User(user__left__sale=sale_id, ...)

        SQL
        SELECT * FROM user LEFT JOIN sale ON user.sale_id = sale.sale_id

        'tablaOrigen__unionType__secondTable=sharedColumn', ...
        'client__inner__country=country_id'

        UNIONES VALIDAS:

        inner = INNER
        right = RIGHT
        left = LEFT
        """

        # UNIONES VALIDAS
        UNIONES = {"inner": "INNER", "right": "RIGHT", "left": "LEFT"}
        VALIDUN = list(UNIONES.keys())

        DB_TABLES = list(self.model._family.keys())

        RESULT = []
        # Iteracion de argumentos
        for llave, valor in kwargs.items():
            # Validar sintaxis:
            validate = (("__" not in llave), (not isinstance(valor, str)))
            if any(validate):
                logger.critical(
                    "Make sure to separate clauses using '__'. "
                    "Also consider 'string' as the only valid argument. "
                    f"Passed data; {llave}={valor}."
                )
                raise ValueError

            # Separar y validar
            PARTS = llave.split("__")
            if len(PARTS) != 3:
                logger.critical(
                    "Invalid quantity of clauses passed. "
                    "Valid syntax: "
                    "tablaOrigen__unionType__secondTable=sharedColumn. "
                    f"Passed {llave}"
                )
                raise ValueError

            # EXTRACCION DE ARGUMENTOS
            ORIGEN = PARTS[0].lower()
            UNIONT = PARTS[1].lower()
            SECONT = PARTS[2].lower()
            SHARED = valor.lower()

            # VALIDAR DATOS
            valid_data = (
                (ORIGEN not in DB_TABLES),
                (SECONT not in DB_TABLES),
                (UNIONT not in VALIDUN),
            )

            if any(valid_data):
                logger.critical(
                    f"Make sure tables exist in DATABASE. Valid {DB_TABLES}. "
                    f"Passed tables: {ORIGEN}, {SECONT}. "
                    f"Make sure passed a valid union type. "
                    f"Passed union {UNIONT}, valid ones are {VALIDUN}."
                )

            # Diccionario de uniones
            dicc = {
                "join": UNIONT,
                "tab1": SECONT,
                "id1": SHARED,
                "tab2": ORIGEN,
                "id2": SHARED,
            }
            RESULT.append(dicc)

        try:
            self.FROM = RESULT[0]["tab2"]
        except IndexError:
            self.FROM = self.model._table
        self.JOIN = RESULT

        return self

    def link(self, *tables):
        MODEL = self.model
        DB_TABLES = list(MODEL._family.keys())
        MAIN = MODEL._table

        if not tables:
            return self

        # 1. Validar que todas las tablas solicitadas existan
        RELATIONS = list(tables)
        for TAB in RELATIONS:
            if TAB not in DB_TABLES:
                logger.critical(f"La tabla {TAB} no existe en el modelo.")
                raise ValueError(f"Tabla inválida: {TAB}")

        # Incluimos MAIN para analizar sus relaciones
        ALL_TABLES = RELATIONS + [MAIN]

        # 2. Extraer TODAS las relaciones posibles entre estas tablas
        possible_edges = []
        for TAB in ALL_TABLES:
            FIELDS = MODEL._family[TAB]._metadata[TAB]["fields"]
            for field in FIELDS:
                if isinstance(field, ForeignKey):
                    second_table = getattr(field, "second_table")
                    if second_table in ALL_TABLES:
                        name = field._name
                        possible_edges.append((TAB, second_table, name))

        # 3. Ordenar las relaciones evitando caminos redundantes
        available_tables = {MAIN}
        ordered_kwargs = {}

        while possible_edges:
            match_found = False
            for edge in list(possible_edges):
                tab_a, tab_b, field_name = edge

                # Caso A: tab_a ya es conocida, pero tab_b es NUEVA
                if tab_a in available_tables and tab_b not in available_tables:
                    arg = f"{tab_a}__inner__{tab_b}"
                    ordered_kwargs[arg] = field_name
                    available_tables.add(tab_b)
                    possible_edges.remove(edge)
                    match_found = True
                    break

                # Caso B: tab_b ya es conocida, pero tab_a es NUEVA - Invertir
                elif (
                    tab_b in available_tables and tab_a not in available_tables
                ):
                    arg = f"{tab_b}__inner__{tab_a}"
                    ordered_kwargs[arg] = field_name
                    available_tables.add(tab_a)
                    possible_edges.remove(edge)
                    match_found = True
                    break

                # Caso C: Ambas tablas ya están en la consulta. (Redundancia)
                elif tab_a in available_tables and tab_b in available_tables:
                    possible_edges.remove(edge)
                    match_found = (
                        True  # Marcamos progreso para que continúe el bucle
                    )
                    break

            if not match_found:
                break

        # 4. Enviar los kwargs limpios y ordenados
        self.add(**ordered_kwargs)
        return self

    def filter(self, **kwargs):
        """
        SINTAXIS:

        tabla__columna__operador = data
        tabla__columna__operador__logico = data

        EJEMPLO:
        client__name__same = "Omar"
        country__name__in = ["Mexico", "France"]

        COMPLEJO:
        filter.(client__name__same__and = "Omar", client__age__btwn = [10, 20])

        SQL:
        WHERE client.name = "Omar" AND client.age BETWEEN 10 AND 20;

        Comparadores Validos:

        OPERATOR = {same: "=", lt: "<", ltsm: "<=", gt: ">", gtsm: ">=",
        diff:"<>", in: "IN", notin: "NOT IN", btwn: "BETWEEN",
        is: "IS", isnot: "IS NOT", like: "LIKE", notlike: "NOT LIKE"}

        LOGICS = {'AND', 'OR', ''}
        """

        MODEL = self.model

        DB_TABLES = list(MODEL._family.keys())

        OPERATORS = {
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
            "notlike": "NOT LIKE",
        }

        LOGICS = {"AND", "OR", ""}

        DATATYPES = (list, tuple, str, int, float, bool)

        if not kwargs:
            return self

        RESULT = []

        for ARGUMENT, VALUE in kwargs.items():
            if "__" not in ARGUMENT:
                logger.critical(
                    f"Invalid separator in argument {ARGUMENT}. "
                    "Valid separator '__'."
                )
                raise ValueError

            if not isinstance(VALUE, DATATYPES):
                logger.critical(
                    f"Invalid datatype passed {VALUE}. "
                    f"Valid datatypes are {DATATYPES}."
                )
                raise TypeError(type(VALUE))

            PARTS = ARGUMENT.split("__")

            if len(PARTS) not in (3, 4):
                logger.critical(
                    "Invalid parts passed in argument. "
                    "Valid lenght: '3' or '4' elements. "
                    f"Passed parts are {PARTS}."
                )

            # EXTRACCION DE ARGUMENTOS
            TAB = PARTS[0]
            COL = PARTS[1]
            OPR = PARTS[2]
            try:
                LOG = PARTS[3].upper()
            except IndexError:
                LOG = ""

            # VALIDAR ARGUMENTOS
            if TAB not in DB_TABLES:
                logger.critical(
                    f"Invalid table passed in argument {ARGUMENT}. "
                    f"Valid tables are: {DB_TABLES}"
                )
                raise ValueError

            COLUMNS = MODEL._family[TAB]._metadata[TAB]["columns"]
            validate = (
                (COL not in COLUMNS),
                (OPR not in OPERATORS),
                (LOG not in LOGICS),
            )

            if any(validate):
                logger.critical(
                    "Make sure passed arguments are validated. "
                    f"Passed column: {COL}, valid ones: {COLUMNS}. "
                    f"Passed operator: {OPR}, valid ones: {OPERATORS}. "
                    f"Passed logic: {LOG}, valid ones: {LOGICS}."
                )
                raise ValueError

            iterables = (
                (OPR in {"in", "notin", "btwn"}),
                (isinstance(VALUE, (list, tuple))),
            )

            if all(iterables):
                if OPR == "btwn" and len(VALUE) == 2:
                    RESULT.append(
                        {
                            "table": TAB,
                            "column": COL,
                            "operator": OPERATORS[OPR],
                            "value": VALUE,
                            "logic": LOG,
                        }
                    )
                    continue
                elif OPR in {"in", "notin"}:
                    RESULT.append(
                        {
                            "table": TAB,
                            "column": COL,
                            "operator": OPERATORS[OPR],
                            "value": VALUE,
                            "logic": LOG,
                        }
                    )
                    continue
                else:
                    logger.critical(
                        f"Passed argument: {ARGUMENT} "
                        "requires an iterable of 2 elements for data. "
                        f"Passed data: {VALUE}"
                    )
            else:
                RESULT.append(
                    {
                        "table": TAB,
                        "column": COL,
                        "operator": OPERATORS[OPR],
                        "value": VALUE,
                        "logic": LOG,
                    }
                )

        self.FILTER = RESULT
        return self

    def group(self, **kwargs):
        """
        Agrupar pasando el nombre de la tabla como argumento
        y el nombre de la columna como valor:

        Ej: category="name"

        """

        MODEL = self.model
        DB_TABLES = list(MODEL._family.keys())
        DB_COLUMNS = []
        for TAB in DB_TABLES:
            columns = MODEL._family[TAB]._metadata[TAB]["columns"]
            DB_COLUMNS.extend(columns)

        if not kwargs:
            return self

        RESULT = []

        for TAB, COL in kwargs.items():
            validate = (
                (TAB not in DB_TABLES),
                (not isinstance(COL, str)),
                (COL not in DB_COLUMNS),
            )

            if any(validate):
                logger.critical(
                    "Make sure the following contidions are True: "
                    f"Valid tables {DB_TABLES}, you passed {TAB}. "
                    f"Valid columns {DB_COLUMNS}, you passed {COL}"
                )
                raise ValueError

            dicc = {"table": TAB, "name": COL}

            RESULT.append(dicc)

        self.group = RESULT
        return self

    def sort(self, *sort):
        """
        Ordenar pasando strings, ...

        Sintaxis
        tabla__columna__order

        Ordenes:
        {"desc": "DESC", "asc": "ASC"}
        """

        VALID_ORDERS = {"desc": "DESC", "asc": "ASC"}

        MODEL = self.model
        DB_TABLES = list(MODEL._family.keys())

        if not sort:
            return self

        RESULT = []
        # VALIDANDO ARGUMENTOS
        for ARG in sort:
            validate = ((not isinstance(ARG, str)), ("__" not in ARG))

            if any(validate):
                logger.critical(
                    f"Make sure passed arguments are strings {ARG} "
                    f"and the usage of proper separator '__'."
                )
                raise ValueError(type(ARG))

            PARTS = ARG.split("__")
            if len(PARTS) != 3:
                logger.critical(
                    "You must specify: "
                    f"table__column__order. Passed pattern: {PARTS}"
                )
                raise ValueError

            TAB = PARTS[0]
            COL = PARTS[1]
            DIR = PARTS[2]

            if TAB not in DB_TABLES:
                logger.critical(
                    f"The following passed table {TAB} does not exist "
                    f"in DATABASE. Valid tables are: {DB_TABLES}"
                )
                raise ValueError

            COLUMNS = MODEL._family[TAB]._metadata[TAB]["columns"]

            purge = ((COL not in COLUMNS), (DIR not in VALID_ORDERS))
            if any(purge):
                logger.critical(
                    f"Make sure column {COL} exists. "
                    f"Make sure order value is valid {DIR}. "
                    f"Valid columns are: {COLUMNS}. Valid orders are: "
                    f"{VALID_ORDERS}."
                )

            RESULT.append({"table": TAB, "name": COL, "order": DIR})

        self.ORDER = RESULT
        return self

    def chunk(self, offset=None, limit=None):

        if not offset and not limit:
            return self

        if limit and not offset:
            if not isinstance(limit, int):
                logger.critical("Passed argument 'limit' must be an integer.")
                raise TypeError(type(limit))

            self.LIMIT = limit
            self.OFFSET = None

        elif offset and not limit:
            logger.critical(
                "Invalid structure. When invokin a segmented query "
                "you can pass only 'limit' argument but it is not valid"
                "passin 'offset' without 'limit'."
            )
            raise ValueError

        else:
            if not isinstance(limit, int) or not isinstance(offset, int):
                logger.critical(
                    "Make sure both passed arguments are integers. "
                    f"Limit {limit}. Offset {offset}."
                )
                raise TypeError
            self.LIMIT = limit
            self.OFFSET = offset

        return self

    def count(self):
        QUERY = f"{self.model._table}__{self.model._table}_id__count"
        row, col = self.select(QUERY).all().raw()
        return row[0][0]

    def all(self, ids=False):

        # RUTA DB
        PATH = self.model._db_file

        # FROM
        FROM = self.model._table if not self.FROM else self.FROM

        # VALIDAR SELECCION
        if not self.SE_SELECT:
            self._NO_SELECT_()

        # OBTNER IDS DE MAIN TABLE DEL QUERY (Prioridad de select)
        if ids:
            self._IDS_(main_table=FROM)

        # OBTENCION DE AGRUPACIÓN
        self._DYNAMIC_GROUP_()

        # VALIDAR OPCIONALES
        JOIN = None if not self.JOIN else self.JOIN
        FILTER = None if not self.FILTER else self.FILTER
        GROUP = None if not self.GROUP else self.GROUP
        ORDER = None if not self.ORDER else self.ORDER
        LIMIT = None if not self.LIMIT else self.LIMIT
        OFFSET = None if not self.OFFSET else self.OFFSET

        row, col = query(
            db_path=PATH,
            select=self.SE_SELECT,
            _from=FROM,
            join=JOIN,
            condition=FILTER,
            group_by=GROUP,
            order_by=ORDER,
            limit=LIMIT,
            offset=OFFSET,
        )

        self.ROW = row
        self.COL = col

        return self

    def raw(self, label=False, align=False):

        if not self.ROW and not self.COL:
            return self.ROW, self.COL

        ROWS = self.ROW.copy()
        COLS = self.COL.copy()

        if label:
            LABELS = self.SE_LABEL.copy()

            COLS = _NOT_DUPLICATED_LABELS_(labels=LABELS, columns=COLS)

        if align:
            ROWS = list(zip(*ROWS))

        self.reset()
        return ROWS, COLS

    def dictionary(self, label=False):

        if not self.ROW and not self.COL:
            return []

        ROWS = self.ROW.copy()
        COLS = self.COL.copy()

        if label:
            LABELS = self.SE_LABEL.copy()

            COLS = _NOT_DUPLICATED_LABELS_(labels=LABELS, columns=COLS)

        if ROWS:
            dicc = [dict(zip(COLS, r)) for r in ROWS]
            self.reset()
            return dicc

        else:
            NONES = [None for C in COLS]
            dicc = [dict(zip(COLS, NONES))]
            self.reset()
            return dicc

    def container(self):

        if not self.ROW and not self.COL:
            return {}

        MAIN = self.model._table
        ROWS = self.ROW
        COLS = self.COL
        META = self.model._metadata

        TABLES = []
        for col in COLS:
            PARTS = col.split("__")
            TAB = PARTS[0]
            if TAB not in TABLES:
                TABLES.append(TAB)

        if ROWS:
            VECTORS = list(zip(*ROWS))
            VECTORS = [list(v) for v in VECTORS]
        else:
            VECTORS = [[None] for c in COLS]
        CONTAINER = {}
        for ITER_TAB in TABLES:
            CONTAINER[ITER_TAB] = {}

            # MAPAEAR TABLA FORM
            if ITER_TAB == MAIN:
                CONTAINER[ITER_TAB]["@main_table@"] = True
            else:
                CONTAINER[ITER_TAB]["@main_table@"] = False

            # MAPEAR DEPENDENCIAS DE TABLA ACTUAL
            CONTAINER[ITER_TAB]["@depends@"] = META[ITER_TAB]["depends"]

            for col, vector in zip(COLS, VECTORS):

                PARTS = col.split("__")

                TAB = PARTS[0]
                COL = PARTS[1]
                AGG = ""

                if TAB != ITER_TAB:
                    continue

                if len(PARTS) == 3:
                    AGG = PARTS[2]

                CONTAINER[TAB][COL] = {}

                TYPE = META[TAB]["schema"][COL]["type"]
                REQUIRED = META[TAB]["schema"][COL]["required"]
                DEFAULT = META[TAB]["schema"][COL]["default"]
                READONLY = META[TAB]["schema"][COL]["readonly"]
                LABEL = META[TAB]["schema"][COL]["metadata"]["comment"]
                POSITION = META[TAB]["schema"][COL]["metadata"]["position"]
                PK = META[TAB]["schema"][COL]["metadata"].get(
                    "primary_key", False
                )
                FK = META[TAB]["schema"][COL]["metadata"].get(
                    "foreign_key", False
                )
                SQL = META[TAB]["schema"][COL]["metadata"].get(
                    "sql_type", ""
                )
                SEC_TAB = False
                SEC_COL = False

                if FK:
                    SEC_TAB = META[TAB]["schema"][COL]["metadata"][
                        "foreign_key"
                    ]["second_table"]
                    SEC_COL = META[TAB]["schema"][COL]["metadata"][
                        "foreign_key"
                    ]["column_id"]

                if AGG:
                    LABEL = f"{LABEL}" + " " + f"{AGG}".capitalize().strip()
                    POSITION = None

                if SQL == "BOOLEAN":
                    vector = [bool(item) for item in vector]

                if any(((SQL == "TIMESTAMP"), (SQL == "DATE"))):
                    vector = [item.isoformat() for item in vector]

                CONTAINER[TAB][COL]["vector"] = vector
                CONTAINER[TAB][COL]["label"] = LABEL
                CONTAINER[TAB][COL]["position"] = POSITION
                CONTAINER[TAB][COL]["readonly"] = READONLY
                CONTAINER[TAB][COL]["default"] = DEFAULT
                CONTAINER[TAB][COL]["required"] = REQUIRED
                CONTAINER[TAB][COL]["python_type"] = TYPE.__name__
                CONTAINER[TAB][COL]["primary_key"] = PK
                CONTAINER[TAB][COL]["sql_type"] = SQL
                CONTAINER[TAB][COL]["second_table"] = SEC_TAB
                CONTAINER[TAB][COL]["foreign_key"] = SEC_COL

        return CONTAINER
