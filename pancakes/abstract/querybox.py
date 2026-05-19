"""
Declaracion De Clase QueryBox() Queries declarativos a traves
de **kwargs y encadenamiento de metodos.
"""

# Modulos Propios
import logging

from ..orm.query import query
from ..sql.datatype import ForeignKey
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
        self.SP_LABEL = []
        self.SE_SELECT = []
        self.SP_SELECT = []
        self.FROM = None
        self.JOIN = None
        self.CONDITION = None
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
            "": "",
        }
        """

        # Agregaciones validas
        AGGS = {
            "min": "MIN",
            "max": "MAX",
            "sum": "SUM",
            "count": "COUNT",
            "avg": "AVG",
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
                self.SE_SELECT.append({"name": COL, "agg": AGG})
                continue

            # SELECCION ESPECIAL
            if TAB in DB_TABLES:
                # Obtener etiquetas frontend
                MAPPED = dict(
                    zip(
                        MODEL._family[TAB]._metadata[TAB]["columns"],
                        MODEL._family[TAB]._metadata[TAB]["comments"],
                    )
                )
                LAB = MAPPED[COL]
                self.SP_LABEL.append(" ".join(f"{LAB} {AGG.upper()}".split()))

                # Seleccion especial resultado
                self.SP_SELECT.append({"table": TAB, "name": COL, "agg": AGG})

            # TAB no existe en la base de datos
            else:
                logger.critical(
                    f"The following table: {TAB} does not exist in Database. "
                    "Make sure passed arguments exist and proper syntax "
                    "using '__' as separator."
                )
                raise ValueError

        # Validar: NO SE_SELECT, SI SP_SELECT
        if not self.SE_SELECT:
            FIRST_COL = MODEL._family[ACTUAL_TB]._metadata[ACTUAL_TB][
                "columns"
            ][0]
            self.SE_SELECT.append({"name": FIRST_COL, "agg": ""})

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
            self.SE_SELECT.append({"name": COL})

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

                    self.SP_LABEL.extend(LABS1)
                    self.SP_SELECT.extend(listado1)

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

                    self.SP_LABEL.extend(LABS2)
                    self.SP_SELECT.extend(listado2)

    def link(self, *unions):
        """
        SINTAXIS:

        'UNION__TABLE', ...

        UNIONES VALIDAS:

        in = INNER
        rg = RIGHT
        lf = LEFT
        """

        # UNIONES VALIDAS
        JOINS = {"in": "INNER", "rg": "RIGHT", "lf": "LEFT"}

        MODEL = self.model
        MAINT = self.model._table

        # CACHEAR TABLAS, LINKERS y DEPENDENCIAS
        TABLAS = [MAINT]
        LINKERS = ["in"]
        DEPENDENCIES = [MODEL._family[MAINT]._metadata[MAINT]["depends"]]

        # ITERACION DE UNIONES
        for union in unions:
            union = union.lower()

            # Validar oracion de union
            validate = (not isinstance(union, str), ("__" not in union))
            if any(validate):
                logger.critical(
                    "Invalid syntax, every union declaration depends "
                    "on specifying union nature and separator '__' "
                    "', example: in__table; INNER JOIN table. "
                    "Valid datatype: Only strings."
                )
                raise ValueError

            # Extraccion de datos de la union
            PARTS = union.split("__")

            LINKER = PARTS[0]
            EXTRAT = PARTS[1]

            # Validar LINKER
            if LINKER not in list(JOINS.keys()):
                logger.critical(
                    f"Invalid type of join: {LINKER}. Valid ones are {JOINS}."
                )
                raise ValueError
            # Validar que exista la tabla
            DB_TABLES = list(MODEL._family.keys())
            if EXTRAT not in DB_TABLES:
                logger.critical(
                    "Passed table does not exist in DATABASE. "
                    f"Passed tables: {EXTRAT}. "
                    f"DATABSE tables: {DB_TABLES}"
                )

            if EXTRAT in TABLAS:
                logger.critical(
                    "Duplicated table passed. Your model is itself a table. "
                    "Any repeated table in your sentence will raise this error."
                )
                raise ValueError

            TABLAS.append(EXTRAT)
            LINKERS.append(LINKER)
            DEPENDENCIES.append(
                MODEL._family[EXTRAT]._metadata[EXTRAT]["depends"]
            )

        # MAP TABLAS Y SUS DEPENDENCIAS Y UNIONES
        MAP_DEPN = {}
        for t, dep in zip(TABLAS, DEPENDENCIES):
            MAP_DEPN.update({t: dep})
        MAP_LINK = {}
        for t, link in zip(TABLAS, LINKERS):
            MAP_LINK.update({t: link})

        # Si A == self -> B.add(A)
        # Si C subconjunto de B -> B.(C)
        # Si A != self and A is not subjconjunto de B -> B.(A)
        ORDEN = []
        TABLS = list(MAP_DEPN.keys())
        COPIA = list(MAP_DEPN.values())
        while len(COPIA) > 0:
            for tabla, depend in MAP_DEPN.items():
                if depend == ["self"]:
                    ORDEN.append(tabla)
                    COPIA.remove(depend)
                elif set(depend).issubset(set(ORDEN)):
                    ORDEN.append(tabla)
                    COPIA.remove(depend)
                else:
                    ORDEN.append(tabla)
                    COPIA.remove(depend)

        RESULT = []
        ORDEN.sort(reverse=False)
        FROM = ORDEN.pop()

        for TABLA in ORDEN:

            CAMPOS = MODEL._family[TABLA]._metadata[TABLA]["fields"]

            for CAMP in CAMPOS:
                if isinstance(CAMP, ForeignKey):
                    ORIGEN = TABLA
                    COLLINKS = CAMP._name
                    SECTABLE=getattr(CAMP, "second_table")
                    REFERENS=getattr(CAMP, "column_id")

                    RESULT.append(
                        {
                        "join": JOINS[MAP_LINK[SECTABLE]],
                        "tab1": SECTABLE,
                        "id1": REFERENS,
                        "tab2": ORIGEN,
                        "id2": COLLINKS
                        }
                    )

        import ipdb; ipdb.set_trace()
        self.FROM = FROM
        self.JOIN = RESULT

        return self

    def all(self):

        # REASIGNAR VALOR FROM -> SEGUN JOINS
        if self.FROM:
            FROM = self.FROM
            self.model = self.model._family[self.FROM]
            self._NO_SELECT_()
        else:
            FROM = self.model._table

        # VALIDAR NO SELECCION
        if not self.SE_SELECT and not self.SP_SELECT:
            self._NO_SELECT_()

        # Obtencion de argumentos
        PATH = self.model._db_file
        SPECIAL = None if not self.SP_SELECT else self.SP_SELECT
        JOIN = None if not self.JOIN else self.JOIN

        row, col = query(
            db_path=PATH,
            select=self.SE_SELECT,
            _from=FROM,
            sp_select=SPECIAL,
            join=JOIN
        )

        self.reset()

        return row, col
