# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""
Validación Pydantic de QueryBox()

Metodos Modo Basico:
	* select()
	* filter()
	* link()
	* gp()
	* sort()
	* lim()
	* off()

Metodos Modo Ids:
	* id()

Metodos Modo Add:
	* add()

"""


# Modulos Python
from typing import Annotated, Union, Dict, Tuple, Any, ClassVar, List, Optional, Set, Type, TYPE_CHECKING

# Modulos de Terceros
from pydantic import BaseModel, model_validator, Field

# Mis Modulos (Solo importa, si estamos pasando data al validador)
if TYPE_CHECKING:
	from ..cook.mold import PanCakesORM

# --*-- METODOS MODO BASICO --*--


class ValidateSelect(BaseModel):

	MODEL: ClassVar[Optional[Type]] = None

	VALID_AGGRETATION: ClassVar[Set[str]] = {
		"min",
		"max",
		"sum",
		"count",
		"avg",
		""
	}

	select: Optional[List[str]] = []

	@model_validator(mode="before")
	@classmethod
	def _validate_select_(cls, data: list):

		if cls.MODEL is None:
			raise RuntimeError(
				f"ValidateLink.MODEL must be "
				f"injected before using this method."
				f"To achive this is suggested "
				f"to make injection after all models "
				f"are loaded in memory."
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
					f"Valid separator not found "
					f"in passed string: {string}"
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

			elif len(parts) ==3:
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
			"notlike"
			}

	LOGIC: ClassVar[Set[str]] = {"and", "or"}

	VALID_VALUES: ClassVar[
		Union[str, int, float, bool, List, Tuple]
	] = (str, int, float, bool, List, Tuple)

	filters: Optional[
		Dict[
			str, Union[str, int, float, bool,
				List[Union[str, int, float, bool]],
				Tuple[Union[str, int, float, bool]]
			]
		]
	] = {}

	@model_validator(mode="before")
	@classmethod
	def _validate_filter_(cls, data: dict):

		if cls.MODEL is None:
			raise RuntimeError(
				f"ValidateLink.MODEL must be "
				f"injected before using this method."
				f"To achive this is suggested "
				f"to make injection after all models "
				f"are loaded in memory."
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
					f"Valid separator not found "
					f"in passed string: {key}"
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


class ValidateLink(BaseModel):

	MODEL: ClassVar[Optional[Type]] = None

	links: Optional[List[str]] = []

	@model_validator(mode="before")
	@classmethod
	def _validate_link_(cls ,data: list):

		if cls.MODEL is None:
			raise RuntimeError(
				f"ValidateLink.MODEL must be "
				f"injected before using this method."
				f"To achive this is suggested "
				f"to make injection after all models "
				f"are loaded in memory."
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
