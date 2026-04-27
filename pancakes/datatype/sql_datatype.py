# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
"""
Definiendo Todos Los Tipos De Datos SQLite3 A Traves De Clases
"""


class DataTypeSQL:
    """
    Base De Construccion Para Los Tipos de Datos SQL
    Valores por defecto "pydantic":
    comment: str
    nls: bools -> NOT NULL en SQL
    required: bool -> Requerido Para la API
    """
    _data_type = ''

    def __init__(
        self,
        comment: str,
        required: bool | None = None
    ):

        self.comment = comment
        self.required = required
        self._dtype = None
        self._name = None
        self._schema = {}
        # atributo '_name' se asigna cuando se declara modelo
        # Revisar: '_get_fields', mold.py

    def _sentence(self):
        self.nls = "NOT NULL" if bool(self.required) else ""
        self._dtype = f'{self._data_type} {self.nls}'.strip()

    def _pydantic(self):
        self._schema = {
            "type": None,
            "required": None,
            "default": None,
            "constraints": {},
            "metadata": {}
        }


class Text(DataTypeSQL):
    """
    Almacena texto sin restricción de caracteres
    """
    _data_type = 'TEXT'
    _sql_default = "DEFAULT ''"
    _python = str

    def __init__(
        self,
        comment: str,
        required: bool | None = None,
        default: str | None = None
    ):

        super().__init__(comment=comment, required=required)
        self.default = default
        self._sentence()
        self._pydantic()

    def _sentence(self):
        super()._sentence()
        self._dtype = f"{self._data_type} {self.nls} {self._sql_default}"
        self._dtype = self._dtype.replace("  ", " ").strip()

    def _pydantic(self):
        super()._pydantic()
        self._schema["type"] = self._python
        self._schema["required"] = bool(self.required)
        self._schema["default"] = self.default
        self._schema["metadata"].update({"comment": self.comment})


class Char(DataTypeSQL):
    """
    Almacena texto delimitado por 'max_length'.
    """

    _data_type = 'VARCHAR'
    _sql_default = "DEFAULT ''"
    _python = str

    def __init__(
        self,
        comment: str,
        max_length: int = 250,
        min_length: int | None = None,
        required: bool | None = None,
        unique: bool | None = None,
        default: str | None = None
    ):
        super().__init__(comment=comment, required=required)
        self.max_length = max_length
        self.min_length = min_length
        self.unique = unique
        self.default = default
        self._sentence()
        self._pydantic()

    def _sentence(self):
        super()._sentence()
        sql_unique = 'UNIQUE COLLATE NOCASE' if self.unique else ""
        self._dtype = (
            f"{self._data_type}"
            f"({self.max_length}) {self.nls} {self._sql_default} {sql_unique}"
        )
        self._dtype = self._dtype.replace("  ", " ").strip()

    def _pydantic(self):
        super()._pydantic()
        self._schema["type"] = self._python
        self._schema["required"] = bool(self.required)
        self._schema["default"] = self.default
        self._schema["constraints"].update({"max_length": self.max_length})
        self._schema["constraints"].update({"min_length": self.min_length})
        if self.unique:
            self._schema["constraints"].update({"unique": bool(self.unique)})
        self._schema["metadata"].update({"comment": self.comment})


class Int(DataTypeSQL):
    """
    Parametros:
    -> comment; Etiquetas Frontend
    -> nls; Not Null
    -> unq; UNIQUE
    -> lt; less than
    -> le; less equal
    -> gt; greater than
    -> ge; greater equal
    """

    _data_type = 'INTEGER'
    _sql_default = 'DEFAULT 0'
    _python = int

    def __init__(
        self,
        comment: str,
        lt: int | None = None,
        le: int | None = None,
        gt: int | None = None,
        ge: int | None = None,
        default: int | None = None,
        required: bool | None = None,
        unique: bool | None = None
    ):
        super().__init__(comment=comment, required=required)
        self.lt = lt
        self.le = le
        self.gt = gt
        self.ge = ge
        self.required = required
        self.default = default
        self.unique = unique
        self._sentence()
        self._pydantic()

    def _sentence(self):
        super()._sentence()
        sql_unique = "UNIQUE" if self.unique else ""
        self._dtype = (
            f"{self._data_type} {self.nls} {self._sql_default} {sql_unique}"
        )
        self._dtype = self._dtype.replace("  ", " ").strip()

    def _pydantic(self):
        super()._pydantic()
        self._schema["type"] = self._python
        self._schema["required"] = bool(self.required)
        self._schema["default"] = self.default
        if self.unique:
            self._schema["constraints"].update({"unique": bool(self.unique)})
        if self.lt:
            self._schema["constraints"].update({"lt": self.lt})
        if self.le:
            self._schema["constraints"].update({"le": self.le})
        if self.gt:
            self._schema["constraints"].update({"gt": self.gt})
        if self.ge:
            self._schema["constraints"].update({"ge": self.ge})
        self._schema["metadata"].update({"comment": self.comment})


class Float(DataTypeSQL):
    """
    Parametros:
    -> comment; Etiquetas Frontend
    -> nls; Not Null
    -> unq; UNIQUE
    -> lt; less than
    -> le; less equal
    -> gt; greater than
    -> ge; greater equal
    """

    _data_type = 'FLOAT'
    _sql_default = 'DEFAULT 0'
    _python = float

    def __init__(
        self,
        comment: str,
        lt: int | None = None,
        le: int | None = None,
        gt: int | None = None,
        ge: int | None = None,
        default: int | None = None,
        required: bool | None = None,
        unique: bool | None = None
    ):
        super().__init__(comment=comment, required=required)
        self.lt = lt
        self.le = le
        self.gt = gt
        self.ge = ge
        self.required = required
        self.default = default
        self.unique = unique
        self._sentence()
        self._pydantic()

    def _sentence(self):
        super()._sentence()
        sql_unique = "UNIQUE" if self.unique else ""
        self._dtype = (
            f"{self._data_type} {self.nls} {self._sql_default} {sql_unique}"
        )
        self._dtype = self._dtype.replace("  ", " ").strip()

    def _pydantic(self):
        super()._pydantic()
        self._schema["type"] = self._python
        self._schema["required"] = bool(self.required)
        self._schema["default"] = self.default
        if self.unique:
            self._schema["constraints"].update({"unique": bool(self.unique)})
        if self.lt:
            self._schema["constraints"].update({"lt": self.lt})
        if self.le:
            self._schema["constraints"].update({"le": self.le})
        if self.gt:
            self._schema["constraints"].update({"gt": self.gt})
        if self.ge:
            self._schema["constraints"].update({"ge": self.ge})
        self._schema["metadata"].update({"comment": self.comment})


class Bool(DataTypeSQL):
    """
    Almacena; True | False
    Solo aceptamos bools en 'default'
    Evitamos pasar otro numero que no sea 1 | 0
    """

    _data_type = 'BOOLEAN'
    _sql_default = 'DEFAULT 1'
    _python = bool

    def __init__(
        self,
        comment: str,
        default: bool | None = None,
        required: bool | None = None
    ):

        super().__init__(comment=comment, required=required)
        self.default = default
        self._sentence()
        self._pydantic()

    def _sentence(self):
        super()._sentence()
        self._dtype = f'{self._data_type} {self.nls} {self._sql_default}'

    def _pydantic(self):
        super()._pydantic()
        self._schema["type"] = self._python
        self._schema["required"] = bool(self.required)
        self._schema["default"] = self.default
        self._schema["metadata"].update({"comment": self.comment})


class ForeignKey(DataTypeSQL):
    """
    Recomendacion: Hacer coincidir el nombre de la instancia
    creada a partir de ForeignKey con la columna de referencia en la
    tabla externa.

    1. Poque el nombre de la instancia es: (nombre de columna en esta tabla),
    2. Para facilidad de query y lectura: Tener en ambas tablas involucradas
    el mismo nombre de columna. (Join's explicitos)

    EJEMPLO:

    client_id = sql.datatype.ForeignKey(
    second_table = 'client',
    column_id = 'client_id',
    comment:str = 'Client Relation',
    on_del = 'set null',
    on_upd = 'cascade')

    -> Notar como ambos compartiran el nombre "client_id"
    """

    ACTIONS = {'no action', 'restrict', 'set null', 'cascade'}
    _data_type = 'INTEGER'
    _not_null = False
    _python = int
    # UNIQUE no se define porque en las relaciones de tablas pueden haber
    # ids repetidos

    def __init__(
        self,
        second_table: str,
        column_id: str,
        comment: str,
        on_del: str = 'set null',
        on_upd: str = 'cascade',
        default: int | None = None
    ):

        super().__init__(
            comment=comment,
            required=self._not_null
        )

        self.second_table = second_table
        self.column_id = column_id
        self.on_del = on_del.upper() if on_del in self.ACTIONS else 'SET NULL'
        self.on_upd = on_upd.upper() if on_upd in self.ACTIONS else 'CASCADE'
        self.default = default
        self._sentence()
        self._pydantic()

    def _sentence(self):
        super()._sentence()

        self._dtype = f"""
        {self._data_type} {self.nls} REFERENCES {self.second_table}
        ({self.column_id}) ON DELETE {self.on_del} ON UPDATE {self.on_upd}
        """
        self._dtype = self._dtype.replace("  ", " ").strip()

    def _pydantic(self):
        super()._pydantic()
        self._schema["type"] = self._python
        self._schema["required"] = False
        self._schema["default"] = self.default
        self._schema["metadata"].update({"comment": self.comment})
        self._schema["metadata"].update(
            {"foreign_key": {
                "second_table": self.second_table,
                "column_id": self.column_id
            }
        })

