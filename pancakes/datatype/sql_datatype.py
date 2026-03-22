# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""
Definiendo Todos Los Tipos De Datos SQLite3 A Traves De Clases
"""
# Modulo Desarrollo
import ipdb


class DataTypeSQL:
    """Base De Construccion Para Los Tipos de Datos SQL"""
    _data_type = ''

    def __init__(self, comment: str = '', nls: bool = False):
        self.comment = comment
        self.nls = 'NOT NULL' if nls else ''
        self._dtype = None
        # _name, se asigna como nombre de columna una vez
        # se deaclare una instancia de esta clase sobre una tabla.
        self._name = None

    def _construct(self):
        self._dtype = f'{self._data_type} {self.nls}'.strip()


class Text(DataTypeSQL):
    _data_type = 'TEXT'
    _default = "DEFAULT ''"

    def __init__(self, comment: str = '', nls: bool = False):
        super().__init__(comment=comment, nls=nls)
        self._construct()

    def _construct(self):
        self._dtype = (
            f"{self._data_type} {self.nls} {self._default}"
        )
        self._dtype = self._dtype.replace("  ", " ").strip()


class Char(DataTypeSQL):
    _data_type = 'VARCHAR'
    _default = "DEFAULT ''"

    def __init__(
        self,
        comment: str = '',
        nls: bool = False,
        unq: bool = False,
        size: int = 250
    ):
        super().__init__(comment=comment, nls=nls)
        self.unq = 'UNIQUE COLLATE NOCASE' if unq else ''
        self.size = size
        self._construct()

    def _construct(self):
        self._dtype = (
            f"{self._data_type}"
            f"({self.size}) {self.nls} {self._default} {self.unq}"
        )
        self._dtype = self._dtype.replace("  ", " ").strip()


class Int(DataTypeSQL):
    _data_type = 'INTEGER'
    _default = 'DEFAULT 0'

    def __init__(
        self,
        comment: str = '',
        nls: bool = False,
        unq: bool = False
    ):
        super().__init__(comment=comment, nls=nls)
        self.unq = 'UNIQUE' if unq else ''
        self._construct()

    def _construct(self):
        self._dtype = (
            f"{self._data_type} {self.nls} {self._default} {self.unq}"
        )
        self._dtype = self._dtype.replace("  ", " ").strip()


class Float(DataTypeSQL):
    _data_type = 'FLOAT'
    _default = 'DEFAULT 0'

    def __init__(
        self,
        comment: str = '',
        nls: bool = False,
        unq: bool = False
    ):
        super().__init__(comment=comment, nls=nls)
        self.unq = 'UNIQUE' if unq else ''
        self._construct()

    def _construct(self):
        self._dtype = (
            f"{self._data_type} {self.nls} {self._default} {self.unq}"
        )
        self._dtype = self._dtype.replace("  ", " ").strip()


class Bool(DataTypeSQL):
    _data_type = 'BOOLEAN'
    _default = 'DEFAULT 1'

    def __init__(self, comment: str = '', nls: bool = False):
        super().__init__(comment=comment, nls=nls)
        self._construct()

    def _construct(self):
        self._dtype = f'{self._data_type} {self.nls} {self._default}'


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
    on_del = 'no action',
    on_upd = 'no action')

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
        comment: str = '',
        on_del: str = 'no action',
        on_upd: str = 'no action'
    ):
        super().__init__(comment=comment, nls=ForeignKey._not_null)
        self.second_table = second_table
        self.column_id = column_id
        self.on_del = on_del.upper() if on_del in self.ACTIONS else ''
        self.on_upd = on_upd.upper() if on_upd in self.ACTIONS else '' 
        self._construct()

    def _construct(self):
        self._dtype = f"""
        {self._data_type} {self.nls} REFERENCES {self.second_table}
        ({self.column_id}) ON DELETE {self.on_del} ON UPDATE {self.on_upd}
        """
        self._dtype = self._dtype.replace("  "," ").strip()