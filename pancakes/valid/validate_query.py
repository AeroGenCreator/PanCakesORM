# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""
Validación Pydantic de QueryBox()

Metodos Modo Basico:
    * select() <- ✅
    * add() <- ✅
    * link() <- ✅
    * filter() <- ✅
    * gp() <- ✅
    * sort() <- ✅
    * chunk(lim, off) <- ✅
"""

# Modulos Python
from typing import (
    Annotated,
    ClassVar,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

# Modulos de Terceros
from pydantic import BaseModel, Field, model_validator

# --*-- METODOS MODO BASICO --*--


# SELECT
class ValidateSelect(BaseModel):
    MODEL: ClassVar[Optional[Type]] = None

    VALID_AGGRETATION: ClassVar[Set[str]] = {
        "min",
        "max",
        "sum",
        "count",
        "avg",
        "",
    }

    select: Optional[List[str]] = []

    @model_validator(mode="before")
    @classmethod
    def _validate_select_(cls, data: list):

        if cls.MODEL is None:
            raise RuntimeError(
                "ValidateLink.MODEL must be "
                "injected before using this method."
                "To achive this is suggested "
                "to make injection after all models "
                "are loaded in memory."
            )

        if not data:
            return data

        # Tablas | Columnas -> Base de datos
        DB_TABLES = [t for t, m in cls.MODEL._family.items()]
        DB_COLUMNS = []
        for t in DB_TABLES:
            DB_COLUMNS.extend(cls.MODEL._metadata[t]["columns"])

        list_strings = data.get("select", [])

        for string in list_strings:
            if "__" not in string:
                raise ValueError(
                    f"Valid separator '__' not found in passed string: {string}"
                )

            parts = string.split("__")

            if len(parts) == 2:
                tab = parts[0]
                col = parts[1]

                if tab not in DB_TABLES:
                    raise ValueError(
                        f"Passed table '{tab}' "
                        f"does not exist in DATABASE "
                        f"Valid tables are {DB_TABLES}"
                    )
                if col not in DB_COLUMNS:
                    raise ValueError(
                        f"Passed column '{col}' "
                        f"does not exist in DATABASE "
                        f"Valid columns are {DB_COLUMNS}"
                    )

            elif len(parts) == 3:
                tab = parts[0]
                col = parts[1]
                agg = parts[2]

                if tab not in DB_TABLES:
                    raise ValueError(
                        f"Passed table '{tab}' "
                        f"does not exist in DATABASE "
                        f"Valid tables are {DB_TABLES}"
                    )
                if col not in DB_COLUMNS:
                    raise ValueError(
                        f"Passed column name '{col}' "
                        f"does not exist in DATABASE "
                        f"Valid columns are {DB_COLUMNS}"
                    )
                if agg not in cls.VALID_AGGRETATION:
                    raise ValueError(
                        f"Invalid aggregation function "
                        f"passed to selected column "
                        f"'{agg}'. Valid ones are "
                        f"{cls.VALID_AGGRETATION}"
                    )
                pass

            else:
                raise ValueError(
                    f"Invalid length of select declaration. "
                    f"Syntax; table__column__aggregation. "
                    f"Passed string: {string}. "
                    f"Parts {parts}"
                )

        return data


# ADD
class ValidateAdd(BaseModel):
    """
    Valida; model.add()

    Estructura del kwarg (Sintaxis):

    tablaOrigen__tipoUnion__tablaExtra = foreignKey

    Tipos datos: {str: str}

    Tipos union:

    inner = IN
    left = LEFT
    right = RIGHT
    """

    MODEL: ClassVar[Optional[Type]] = None

    JOINS: ClassVar[Set[str]] = {"inner", "left", "right"}

    added: Optional[Dict[str, str]] = {}

    @model_validator(mode="before")
    @classmethod
    def _validate_add_(cls, data: dict):

        # Validar data
        if not data:
            return data

        # Tablas | Columnas -> Base de datos
        DB_TABLES = list(cls.MODEL._family.keys())
        DB_COLUMNS = []
        for t in DB_TABLES:
            DB_COLUMNS.extend(cls.MODEL._metadata[t]["columns"])

        # Argumentos
        KWARGS = data.get("added", {})

        # Validar argumentos
        for key, value in KWARGS.items():
            # Validar tipo de dato
            if not isinstance(value, str):
                raise TypeError(
                    f"Make sure passed value {value} is a 'string'. "
                    f"Passed datatype {type(value)}"
                )

            # Validar sintaxis "__"
            if "__" not in key:
                msg = (
                    f"Valid separator '__' not found in passed argument: {key}"
                )
                raise ValueError(msg)

            # Separar argumento
            PARTS = key.split("__")

            if len(PARTS) != 3:
                raise ValueError(
                    f"Invalid length passed for argument {PARTS}. "
                    "Valid length of 3 parts are: "
                    "originTable_joinType__extraTable = 'foreignKey'"
                )

            TAB1 = PARTS[0]
            JOIN = PARTS[1]
            TAB2 = PARTS[2]

            validate = (
                (TAB1 not in DB_TABLES),
                (JOIN not in cls.JOINS),
                (TAB2 not in DB_TABLES),
                (value not in DB_COLUMNS),
            )

            if any(validate):
                raise ValueError(
                    "Make sure following conditions are True: "
                    f"1. Valid tables are: {DB_TABLES}, passed ones are: "
                    f"{TAB1}, {TAB2}."
                    f"2. Valid columns are: {DB_COLUMNS}, passed one: {value}."
                    f"3. Valid joins are {cls.JOINS}, passed one: {JOIN}."
                )

        return data


# FILTER
class ValidateFilter(BaseModel):
    MODEL: ClassVar[Optional[Type]] = None

    OPERATOR: ClassVar[Set[str]] = {
        "same",
        "lt",
        "ltsm",
        "gt",
        "gtsm",
        "diff",
        "in",
        "notin",
        "btwn",
        "is",
        "isnot",
        "like",
        "notlike",
    }

    LOGIC: ClassVar[Set[str]] = {"and", "or"}

    VALID_VALUES: ClassVar[Union[str, int, float, bool, List, Tuple]] = (
        str,
        int,
        float,
        bool,
        List,
        Tuple,
    )

    filters: Optional[
        Dict[
            str,
            Union[
                str,
                int,
                float,
                bool,
                List[Union[str, int, float, bool]],
                Tuple[Union[str, int, float, bool]],
            ],
        ]
    ] = {}

    @model_validator(mode="before")
    @classmethod
    def _validate_filter_(cls, data: dict):

        if cls.MODEL is None:
            raise RuntimeError(
                "ValidateLink.MODEL must be "
                "injected before using this method."
                "To achive this is suggested "
                "to make injection after all models "
                "are loaded in memory."
            )

        # Dicc vacio, retornar
        if not data:
            return data

        # Tablas | Columnas -> Base de datos
        DB_TABLES = [t for t, m in cls.MODEL._family.items()]
        DB_COLUMNS = []
        for t in DB_TABLES:
            DB_COLUMNS.extend(cls.MODEL._metadata[t]["columns"])

        kwargs = data.get("filters", {})

        # Iterar Kwargs:
        for key, value in kwargs.items():
            # Validar llaves:
            if "__" not in key:
                raise ValueError(
                    f"Valid separator '__' not found in passed string: {key}"
                )

            # Separar Kwargs, Validar extension:
            parts = key.split("__")

            # Validar 'logic':
            if len(parts) == 4:
                tab = parts[0]
                col = parts[1]
                ope = parts[2]
                log = parts[3]

                if log not in cls.LOGIC:
                    raise ValueError(
                        f"Invalid LOGIC passed; "
                        f"'{log}'. Valid ones are "
                        f"{cls.LOGIC}"
                    )

            elif len(parts) == 3:
                tab = parts[0]
                col = parts[1]
                ope = parts[2]

            else:
                raise ValueError(
                    f"Invalid length of filter declaration. "
                    f"Syntax; table__column__operator. "
                    f"or; table__column__operator__logic. "
                    f"Parts {parts}"
                )

            # Validar Tabla | Columna | Operador
            if tab not in DB_TABLES:
                raise ValueError(
                    f"Passed table '{tab}' "
                    f"does not exist in DATABASE "
                    f"Valid tables are {DB_TABLES}"
                )
            if col not in DB_COLUMNS:
                raise ValueError(
                    f"Passed column name '{col}' "
                    f"does not exist in DATABASE "
                    f"Valid columns are {DB_COLUMNS}"
                )
            if ope not in cls.OPERATOR:
                raise ValueError(
                    f"Invalid OPERATOR passed; "
                    f"'{ope}'. Valid ones are "
                    f"{cls.OPERATOR}"
                )

            # Valida: Data
            if not isinstance(value, (cls.VALID_VALUES)):
                raise TypeError(
                    f"Invalid datatype value; {value} "
                    f"{type(value)} "
                    f"Valid formats are; "
                    f"{cls.VALID_VALUES}"
                )

        return data


# LINK
class ValidateLink(BaseModel):
    MODEL: ClassVar[Optional[Type]] = None

    links: Optional[List[str]] = []

    @model_validator(mode="before")
    @classmethod
    def _validate_link_(cls, data: list):

        if cls.MODEL is None:
            raise RuntimeError(
                "ValidateLink.MODEL must be "
                "injected before using this method."
                "To achive this is suggested "
                "to make injection after all models "
                "are loaded in memory."
            )

        if not data:
            return data

        # Tablas -> Base de datos
        DB_TABLES = [t for t, m in cls.MODEL._family.items()]

        link = data.get("links", [])

        for tab in link:
            if tab not in DB_TABLES:
                raise ValueError(
                    f"Passed tables name; {tab} "
                    f"does not exist in DATABASE. "
                    f"Valid tables are {DB_TABLES}"
                )

        return data


# GROUP
class ValidateGroupBy(BaseModel):
    MODEL: ClassVar[Optional[Type]] = None

    groups: Optional[dict[str, str]] = {}

    @model_validator(mode="before")
    @classmethod
    def _validate_group_by_(cls, data: dict):

        # No GROUP BY; retornar
        if not data:
            return data

        # Tablas | Columnas -> Base de datos
        DB_TABLES = [t for t, m in cls.MODEL._family.items()]
        DB_COLUMNS = []
        for t in DB_TABLES:
            DB_COLUMNS.extend(cls.MODEL._metadata[t]["columns"])

        # Iterar kwargs
        kwargs = data.get("groups", {})

        for tab, col in kwargs.items():
            # Validar Tabla | Columna
            if tab not in DB_TABLES:
                raise ValueError(
                    f"Passed table '{tab}' "
                    f"does not exist in DATABASE "
                    f"Valid tables are {DB_TABLES}"
                )
            if col not in DB_COLUMNS:
                raise ValueError(
                    f"Passed column name '{col}' "
                    f"does not exist in DATABASE "
                    f"Valid columns are {DB_COLUMNS}"
                )

        return data


# SORT
class ValidateOrderBy(BaseModel):
    MODEL: ClassVar[Optional[Type]] = None

    DIRECTION: ClassVar[Dict[str, str]] = {"desc": "DESC", "asc": "ASC"}

    orders: Optional[List[str]] = []

    @model_validator(mode="before")
    @classmethod
    def _validate_order_by_(cls, data: list):

        # No ORDER BY; retornar
        if not data:
            return data

        # Tablas | Columnas -> Base de datos
        DB_TABLES = [t for t, m in cls.MODEL._family.items()]
        DB_COLUMNS = []
        for t in DB_TABLES:
            DB_COLUMNS.extend(cls.MODEL._metadata[t]["columns"])

        # Iterar kwargs:
        LISTA = data.get("orders", [])

        for ARG in LISTA.items():
            if "__" not in ARG:
                raise ValueError(
                    f"Valid separator '__' not found in passed argument: {ARG}"
                )

            PARTS = ARG.split("__")

            if len(PARTS) != 3:
                raise ValueError(
                    "Invalid length of passed arguments. "
                    "You must specify table__column__order"
                    f"Passed parts: {PARTS}"
                )
            TAB = PARTS[0]
            COL = PARTS[1]
            DIR = PARTS[2]

            validate = (
                (TAB not in DB_TABLES),
                (COL not in DB_COLUMNS),
                (DIR not in cls.DIRECTION),
            )

            if any(validate):
                raise ValueError(
                    "Make sure following statements are True. "
                    f"Valid tables: {DB_TABLES}, you passed {TAB}. "
                    f"Valid columns: {DB_COLUMNS}, you passed {COL}. "
                    f"Valid direction: {cls.DIRECTION}, you passed {DIR}"
                )

        return data


# LIMIT | OFFSET
class ValidateLimitOffset(BaseModel):
    offset: Annotated[Optional[int], Field(ge=0)] = None
    limit: Annotated[Optional[int], Field(gt=0)] = None
