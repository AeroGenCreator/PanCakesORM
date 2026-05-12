# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""

Basado en Pydantic;

Construccion de 'Validacion De Filtro'

-> Valida Metodos de Clase -> .d(), .u():

-> Siempre al usar: "QueryBox" & FastAPI -> "routers"

-> delete | update

"""

# Modulos Python
from typing import (
    Annotated,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

# Modulos de Terceros
from pydantic import BaseModel, Field, model_validator


class DeleteFilterValidator(BaseModel):

    MODEL: ClassVar[Optional[Type]] = None

    VALID_OPERATORS: ClassVar[set[str]] = {
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
        "notlike"
    }

    filters: Annotated[
        dict[
            str, Union[
                str, int, float, bool,
                Tuple[Union[str, int, float]],
                List[Union[str, int, float]]
            ]
        ], Field(...)
    ]

    @model_validator(mode="before")
    @classmethod
    def _validate_filter_(cls, params: dict):

        # Validar None
        if not params:
            raise TypeError(
                f"{type(params)}. "
                f"Delete Method .d() does not accept "
                f"NoneType values."
            )

        # Tablas | Columnas <- Base de datos
        DB_TABLES = [t for t, m in cls.MODEL._family.items()]
        DB_COLUMNS = []
        for t in DB_TABLES:
            DB_COLUMNS.extend(
                cls.MODEL._metadata[t]["columns"]
            )

        filters = params.get("filters", {})

        for k, v in filters.items():

            # Validar Sintaxis Kwargs
            if "__" not in k:
                raise ValueError(
                    f"Separator '__' was not found in {k}"
                )

            parts = k.split("__")

            # Filtro Delete 3 partes
            if len(parts) == 3:

                tab = parts[0]
                col = parts[1]
                ope = parts[2]

            else:
                raise ValueError(
                    f"Invalid length of kwargs. "
                    f"Passed: {parts}"
                )

            # Validar Tabla
            if tab not in DB_TABLES:
                raise ValueError(
                    f"Passed table '{tab}' does not exist. "
                    f"Valid tables are: {DB_TABLES}"
                )

            # Validar Columna
            if col not in DB_COLUMNS:
                raise ValueError(
                    f"Passed column '{col}' does not exist. "
                    f"Valid columns for '{tab}' "
                    f"are: {DB_COLUMNS}."
                )

            if ope in {"in", "notin", "btwn"} and not v:
                raise ValueError(
                    f"Empty values are not accepted: {v}"
                )

            # Validar Operador
            if ope not in cls.VALID_OPERATORS:
                raise ValueError(
                    f"Invalid Operator Passed {ope}. "
                    f"Valid ones are {cls.VALID_OPERATORS}"
                )

            # Validar Datos Iterables len()
            if ope in {"in", "notin"}:
                if not isinstance(v, (list, tuple)):
                    raise TypeError(
                        f"Passed value {v} must be a list "
                        f"or a tuple of values"
                    )

            # Validar Datos Iterables len(2)
            elif ope == "btwn":
                if not isinstance(v, (list, tuple)):
                    raise TypeError(
                        f"Passed value {v} must be a list "
                        f"or a tuple of values"
                    )

                if len(v) != 2:
                    raise ValueError(
                        f"Invalid length of values {v} passed "
                        f"with operator {ope}."
                        f"It requires only two values"
                    )

                for i in v:
                    if not isinstance(i, (int, float)):
                        raise TypeError(
                            f"Operator btwn only accepts numeric "
                            f"values. You passed: {v}"
                        )

            # Validar Datos No Iterables
            else:
                if not isinstance(v, (str, int, float, bool)):
                    raise TypeError(
                        "Invalid datatype passed to filter. "
                        "Only operators: 'in', 'notin', 'btwn' "
                        "accept (list, tuple). "
                        "Datatype dict is an invalid syntax."
                    )

        return params


class UpdateFilterValidator(BaseModel):

    MODEL: ClassVar[Optional[Type]] = None

    TYPES: ClassVar[tuple[any]] = (
        str,
        int,
        float,
        bool
    )
    VALID_OPERATORS: ClassVar[set[str]] = {
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
        "notlike"
    }

    update_all: Annotated[bool, Field(False)]
    filters: Dict[
        str,
        Union[str, int, float, bool, List[Any], Tuple[Any, ...]]
    ] = Field(...)

    @model_validator(mode="before")
    @classmethod
    def _validate_filter_(
        cls,
        params: dict
    ):

        # Valid None
        if not params:
            raise TypeError(
                f"{type(params)}. "
                f"Update Method .u() does not accept "
                f"NoneType values."
            )

        # Tablas | Columnas <- Base de datos
        DB_TABLES = [t for t, m in cls.MODEL._family.items()]
        DB_COLUMNS = []
        for t in DB_TABLES:
            DB_COLUMNS.extend(
                cls.MODEL._metadata[t]["columns"]
            )

        # Argumentos .u()
        filters = params.get("filters", {})
        update_all = params.get("update_all", False)

        for k, v in filters.items():

            # Validar Sintaxis
            if "__" not in k:
                raise ValueError(
                    f"Separator '__' was not found in {k}"
                )

            parts = k.split("__")

            # Validar Update All
            if len(parts) == 2 and update_all is True:

                tab = parts[0]
                col = parts[1]

                # Validar Tabla
                if tab not in DB_TABLES:
                    raise ValueError(
                        f"Passed table '{tab}' does not exist. "
                        f"Valid tables are: {DB_TABLES}"
                    )

                # Validar Columna
                if col not in DB_COLUMNS:
                    raise ValueError(
                        f"Passed column '{col}' does not exist. "
                        f"Valid columns for '{tab}' "
                        f"are: {DB_COLUMNS}."
                    )

                continue

            # Validar Update
            elif len(parts) == 4 and update_all is False:

                tab = parts[0]
                col = parts[1]
                ccl = parts[2]
                ope = parts[3]

                val = v[0]
                con = v[1]

                # Validar Tabla
                if tab not in DB_TABLES:
                    raise ValueError(
                        f"Passed table '{tab}' does not exist. "
                        f"Valid tables are: {DB_TABLES}"
                    )

                # Validar Columnas "col" & "ccl"
                if (
                    col not in DB_COLUMNS or
                    ccl not in DB_COLUMNS
                ):
                    raise ValueError(
                        f"Passed column '{col}' does not exist. "
                        f"Valid columns for '{tab}' "
                        f"are: {DB_COLUMNS}."
                    )

            else:
                raise ValueError(
                    f"Invalid length of kwargs passed. "
                    f"Valid 2 and 4. Passed {parts}"
                )

            if ope in {"in", "notin", "btwn"} and not con:
                raise ValueError(
                    f"Empty values are not accepted: {con}"
                )

            if ope not in cls.VALID_OPERATORS:
                raise ValueError(
                    f"Invalid Operator Passed {ope}. "
                    f"Valid ones are {cls.VALID_OPERATORS}"
                )

            if ope in {"in", "notin"}:
                if not isinstance(con, (list, tuple)):
                    raise TypeError(
                        f"Passed value {con} must be a list "
                        f"or a tuple of values"
                    )

            elif ope == "btwn":
                if not isinstance(con, (list, tuple)):
                    raise TypeError(
                        f"Passed value {con} must be a list "
                        f"or a tuple of values"
                    )

                if len(con) != 2:
                    raise ValueError(
                        f"Invalid length of values {con} passed "
                        f"with operator {ope}."
                        f"It requires only two values"
                    )

                for i in con:
                    if not isinstance(i, (int, float)):
                        raise TypeError(
                            f"Operator btwn only accepts numeric "
                            f"values. You passed: {con}"
                        )

            else:
                if not isinstance(con, cls.TYPES):
                    raise TypeError(
                        "Invalid datatype passed to filter. "
                        "Only operators: 'in', 'notin', 'btwn' "
                        "accept (list, tuple). "
                        "Datatype dict is an invalid syntax."
                    )

            if not isinstance(val, cls.TYPES):
                raise ValueError(
                    "Invalid datatype passed to be updated. "
                    f"Valid datatypes: {cls.TYPES}"
                )

        return params
