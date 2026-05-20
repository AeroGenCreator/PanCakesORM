"""
Declaracion De Clase QueryBox() Queries declarativos a traves
de **kwargs y encadenamiento de metodos.
"""

import ast

# Modulos Propios
import logging

from ..orm.consulta import query
from ..tools.functions import environment

# .envs; log, dir, db
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


class QueryBox:
    # Pasar MODELO "instanciado" es obligatorio
    def __init__(self, model):
        self.model = model
        self.reset()

    def reset(self):
        self.DC_LABEL = {}
        self.SE_LABEL = []
        self.SE_SELECT = []
        self.FROM = None
        self.JOIN = None
        self.FILTER = None
        self.GROUP = None
        self.ORDER = None
        self.LIMIT = None
        self.OFF = None
        self.IDS = False
        self.ROW = None
        self.COL = None

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
            "": ""
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

    def _NO_SELECT_(self) -> None:

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
                        listado2.append({"table": TAB2, "name": COL})

                    self.SE_LABEL.extend(LABS2)
                    self.SE_SELECT.extend(listado2)

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

        self.FROM = RESULT[0]["tab2"]
        self.JOIN = RESULT

        return self

    def filter(self, string):
        """
        Un solo string que permite pasar;

        1. Operadores comparativos
        2. Operadores logicos

        COMPARATIVOS
        {
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
        "NOT LIKE"
        }

        LOGICOS
        {'&&': 'AND', '||', 'OR'}

        Filtro basico:

        Valores string deben llevar comillas;

        - Si usas "" entonces string ''
        - Si usas '' entonces string ""

        Esto porque se valida a traves de la libreria 'ast'

        sale__name__=__'omar'

        Filtro con iterables:
        sale__sale_id__in__[1, 2, 3]

        Filtro con logicos
        user__name__=__'Omar'@||@sale__name__=__'snoopy'

        Filtro logico complejo
        user__name__like__'%mar'
        @||@
        sale__sale_id__in__[1, 2, 3, 5]
        @&&@
        client__age__>__18

        Sintaxis:
        Separar con '__' tabla__columna__comparador__valor
        Separar con '@' (clausula ||/&& clausula)

        METODO-> CASE SENSITIVE
        """

        # WHITE LISTS
        DB_TABLES = list(self.model._family.keys())
        OPERATORS = {
            "=",
            "<",
            "<=",
            ">",
            ">=",
            "<>",
            "in",
            "not in",
            "between",
            "is",
            "is not",
            "like",
            "not like",
        }
        LOGICALS = {"||", "&&", ""}
        PARSE_LOGICS = {"||": "or", "&&": "and", "": ""}

        # Preparar el string
        if "__" not in string or not isinstance(string, str):
            logger.critical(
                "Make sure to passed a valid separator '__'. "
                "Make sure you passed only one string. "
                f"Passed string: {string}"
            )
            raise ValueError(type(string))
        CLAUSES = string.split("__")

        SENTENCE = []
        for DATO in CLAUSES:
            if "@" not in DATO:
                SENTENCE.append(DATO)
            else:
                COMPRESS = DATO.split("@")
                SENTENCE.extend(COMPRESS)

        # Anidar grupos de condicion
        SEGMENTOS = []
        COPIA = SENTENCE.copy()
        while COPIA:
            if len(COPIA) >= 5:
                segmento = COPIA[:5]
                if segmento[-1] in LOGICALS:
                    SEGMENTOS.append(segmento)
                    for element in segmento:
                        COPIA.remove(element)
                else:
                    segmento = COPIA[:4]
                    for element in segmento:
                        COPIA.remove(element)
            elif len(COPIA) >= 4:
                segmento = COPIA[:4]
                SEGMENTOS.append(segmento)
                for element in segmento:
                    COPIA.remove(element)
            else:
                logger.critical(
                    "Invalid quantity of elements passed. "
                    f"error found in {COPIA}."
                )
                raise ValueError

        # Armar diccionario | validar data
        RESULT = []
        for condition in SEGMENTOS:
            if len(condition) == 5:
                TAB = condition[0]
                COL = condition[1]
                OPR = condition[2]
                DAT = condition[3]
                LOG = condition[4]

                try:
                    VAL = ast.literal_eval(DAT)
                except Exception as e:
                    logger.critical(e)
                    raise ValueError

                if TAB not in DB_TABLES:
                    logger.critical(
                        f"Following table: {TAB} does not esist. "
                        f"Passed condition: {condition}."
                    )
                    raise ValueError

                COLUMNS = self.model._family[TAB]._metadata[TAB]["columns"]

                if COL not in COLUMNS:
                    logger.critical(
                        f"Following column {COL} does not exist in {TAB}. "
                        f"Passed condition: {condition}"
                    )
                    raise ValueError

                if OPR not in OPERATORS:
                    logger.critical(
                        f"Invalid operator: {OPR}. "
                        f"Passed condition: {condition}"
                    )
                    raise ValueError

                if LOG not in LOGICALS:
                    logger.critical(
                        f"Invalid logical operator found in {LOG}. "
                        f"Passed condition {condition}"
                    )

                validdate_iters = (
                    (OPR in {"in", "not in", "between"}),
                    (not isinstance(VAL, list)),
                )
                if all(validdate_iters):
                    logger.critical(
                        f"Following operators {'in', 'not in', 'between'} "
                        "must receive an iterable. 'List'. "
                        f"Passed value: {VAL}, operator: {OPR}. "
                        f"Passed condition: {condition}"
                    )
                    raise ValueError(type(VAL))

                RESULT.append(
                    {
                        "table": TAB,
                        "column": COL,
                        "operator": OPR,
                        "value": VAL,
                        "logic": PARSE_LOGICS[LOG],
                    }
                )

            elif len(condition) == 4:
                TAB = condition[0]
                COL = condition[1]
                OPR = condition[2]
                DAT = condition[3]
                LOG = ""

                try:
                    VAL = ast.literal_eval(DAT)
                except Exception as e:
                    logger.critical(e)
                    raise ValueError

                if TAB not in DB_TABLES:
                    logger.critical(
                        f"Following table: {TAB} does not esist. "
                        f"Passed condition: {condition}."
                    )
                    raise ValueError

                COLUMNS = self.model._family[TAB]._metadata[TAB]["columns"]

                if COL not in COLUMNS:
                    logger.critical(
                        f"Following column {COL} does not exist in {TAB}. "
                        f"Passed condition: {condition}"
                    )
                    raise ValueError

                if OPR not in OPERATORS:
                    logger.critical(
                        f"Invalid operator: {OPR}. "
                        f"Passed condition: {condition}"
                    )
                    raise ValueError

                if LOG not in LOGICALS:
                    logger.critical(
                        f"Invalid logical operator found in {LOG}. "
                        f"Passed condition {condition}"
                    )

                validdate_iters = (
                    (OPR in {"in", "not in", "between"}),
                    (not isinstance(VAL, list)),
                )
                if all(validdate_iters):
                    logger.critical(
                        f"Following operators {'in', 'not in', 'between'} "
                        "must receive an iterable. 'List'. "
                        f"Passed value: {VAL}, operator: {OPR}. "
                        f"Passed condition: {condition}"
                    )
                    raise ValueError(type(VAL))

                RESULT.append(
                    {
                        "table": TAB,
                        "column": COL,
                        "operator": OPR,
                        "value": VAL,
                        "logic": PARSE_LOGICS[LOG],
                    }
                )

            else:
                logger.critical(
                    "Dimension of expression passed does not match. "
                    "To create a filter you must specify (5 or 4) "
                    "arguments. Following the next syntax."
                    "table__column__operator__value / "
                    "table__column__operator__value@logical@"
                )
        self.FILTER = RESULT
        return self

    def all(self):

        # RUTA DB
        PATH = self.model._db_file

        # FROM
        FROM = self.model._table if not self.FROM else self.FROM

        # VALIDAR SELECCION
        if not self.SE_SELECT:
            self._NO_SELECT_()

        # VALIDAR OPCIONALES
        JOIN = None if not self.JOIN else self.JOIN
        FILTER = None if not self.FILTER else self.FILTER

        row, col = query(
            db_path=PATH,
            select=self.SE_SELECT,
            _from=FROM,
            join=JOIN,
            condition=FILTER
        )

        self.reset()

        return row, col
