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
    column: str
    comment: str
    required: bool
    default: str | None
    """
    _data_type = ''

    def __init__(
        self,
        comment: str,
        nls: bool | None = None,
        default: str | None = None
    ):

        self.comment = comment
        self.nls = 'NOT NULL' if nls else ''
        self.default = default
        self._dtype = None
        # atributo '_name' se asigna cuando se declara modelo
        # Revisar: '_get_fields', mold.py
        self._name = None
        self._schema = {}

    def _sentence(self):
        self._dtype = f'{self._data_type} {self.nls}'.strip()

    def _pydantic(self):
        self.nls = None if self.nls == "" else True
        self._schema.update({"required": (self.nls, None)})
        self._schema.update({"default": (self.default, None)})


class Text(DataTypeSQL):
    """
    Almacena texto sin restricción de caracteres
    """
    _data_type = 'TEXT'
    _default = "DEFAULT ''"

    def __init__(
        self,
        comment: str,
        nls: bool | None = None,
        default: str | None = None
    ):
        
        super().__init__(comment=comment, nls=nls, default=default)
        self._sentence()
        self._pydantic()

    def _sentence(self):
        self._dtype = (
            f"{self._data_type} {self.nls} {self._default}"
        )
        self._dtype = self._dtype.replace("  ", " ").strip()

    def _pydantic(self):
        self._schema.update({"required": (self.nls, None)})
        self._schema.update({"default": (self.default, None)})


class Char(DataTypeSQL):
    """
    Almacena texto delimitado por 'size'.
    """

    _data_type = 'VARCHAR'
    _default = "DEFAULT ''"

    def __init__(
        self,
        comment: str,
        nls: bool | None = None,
        size: int = 250,
        unq: bool = False,
        default: str | None = None
    ):
        super().__init__(comment=comment, nls=nls, default=default)
        self.unq = 'UNIQUE COLLATE NOCASE' if unq else ''
        self.size = size
        self._sentence()
        self._pydantic()

    def _sentence(self):
        self._dtype = (
            f"{self._data_type}"
            f"({self.size}) {self.nls} {self._default} {self.unq}"
        )
        self._dtype = self._dtype.replace("  ", " ").strip()

    def _pydantic(self):
        self._schema.update({"size": (self.size, 250)})
        self._schema.update({"required": (self.nls, None)})
        self._schema.update({"default": (self.default, None)})


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
    _default = 'DEFAULT 0'

    def __init__(
        self,
        comment: str,
        nls: bool | None = None,
        unq: bool = False,
        lt: int | None = None,
        le: int | None = None,
        gt: int | None = None,
        ge: int | None = None,
        default: int | None = None
    ):
        super().__init__(comment=comment, nls=nls, default=default)
        self.unq = 'UNIQUE' if unq else ''
        self.lt = lt
        self.le = le
        self.gt = gt
        self.ge = ge
        self._sentence()
        self._pydantic()

    def _sentence(self):
        self._dtype = (
            f"{self._data_type} {self.nls} {self._default} {self.unq}"
        )
        self._dtype = self._dtype.replace("  ", " ").strip()

    def _pydantic(self):
        self._schema.update({"required": (self.nls, None)})
        self._schema.update({"default": (self.default, None)})
        self._schema.update({"lt": (self.lt, None)})
        self._schema.update({"le": (self.le, None)})
        self._schema.update({"gt": (self.gt, None)})
        self._schema.update({"ge": (self.ge, None)})


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
    _default = 'DEFAULT 0'

    def __init__(
        self,
        comment: str,
        nls: bool | None = None,
        unq: bool = False,
        lt: int | None = None,
        le: int | None = None,
        gt: int | None = None,
        ge: int | None = None,
        default: int | None = None
    ):
        super().__init__(comment=comment, nls=nls, default=default)
        self.unq = 'UNIQUE' if unq else ''
        self.lt = lt
        self.le = le
        self.gt = gt
        self.ge = ge
        self._sentence()
        self._pydantic()

    def _sentence(self):
        self._dtype = (
            f"{self._data_type} {self.nls} {self._default} {self.unq}"
        )
        self._dtype = self._dtype.replace("  ", " ").strip()

    def _pydantic(self):
        self._schema.update({"required": (self.nls, None)})
        self._schema.update({"default": (self.default, None)})
        self._schema.update({"lt": (self.lt, None)})
        self._schema.update({"le": (self.le, None)})
        self._schema.update({"gt": (self.gt, None)})
        self._schema.update({"ge": (self.ge, None)})


class Bool(DataTypeSQL):
    """
    Almacena; True | False
    Solo aceptamos bools en 'default'
    Evitamos pasar otro numero que no sea 1 | 0
    """

    _data_type = 'BOOLEAN'
    _default = 'DEFAULT 1'

    def __init__(
        self,
        comment: str,
        nls: bool | None = None,
        default: bool | None = None
    ):

        super().__init__(comment=comment, nls=nls, default=default)
        self._sentence()

    def _sentence(self):
        self._dtype = f'{self._data_type} {self.nls} {self._default}'

    def _pydantic(self):
        self._schema.update({"required": (self.nls, None)})
        self._schema.update({"default": (self.default, None)})


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

    ACTIONS = {'no action','restrict','set null', 'cascade'}
    _data_type = 'INTEGER'
    _not_null = False
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
            nls=ForeignKey._not_null,
            default=default
        )

        self.second_table = second_table
        self.column_id = column_id
        self.on_del = on_del.upper() if on_del in self.ACTIONS else 'SET NULL'
        self.on_upd = on_upd.upper() if on_upd in self.ACTIONS else 'CASCADE' 
        self._sentence()
        self._pydantic()

    def _sentence(self):
        self._dtype = f"""
        {self._data_type} {self.nls} REFERENCES {self.second_table}
        ({self.column_id}) ON DELETE {self.on_del} ON UPDATE {self.on_upd}
        """
        self._dtype = self._dtype.replace("  "," ").strip()

    def _pydantic(self):
        self._schema.update({"default": (self.default, None)})
