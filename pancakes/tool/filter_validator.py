# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""

Basado en Pydantic; Construccion de un 'Validacion De Filtro'
-> Valida -> .d(), .u(), select():
	
	kwargs: tabla__columna__operador = valor
	1. Llave del kwargs
	2. Coherencia con el valor

"""

# Mis Modulos

# Modulos Python
from typing import Annotated, Union, Dict, Tuple, Any

# Modulos de Terceros
from pydantic import BaseModel, model_validator, Field


class DeleteFilterValidator(BaseModel):

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
	
	filters: Annotated[dict[str, object], Field(...)]

	@model_validator(mode="before")
	@classmethod
	def _validate_filter_(cls, params: dict):

		filters = params.get("filters", {})

		for k, v in filters.items():

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

			if ope in {"in", "notin", "btwn"} and not v:
				raise ValueError(
					f"Empty values are not accepted: {v}"
				)

			if ope not in cls.VALID_OPERATORS:
				raise ValueError(
					f"Invalid Operator Passed {ope}. "
					f"Valid ones are {cls.VALID_OPERATORS}"
				)

			if ope in {"in", "notin"}:
				if not isinstance(v, (list, tuple)):
					raise TypeError(
						f"Passed value {v} must be a list "
						f"or a tuple of values"
					)

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

			else: 
				if not isinstance(v, (str, int, float, bool)):
					raise TypeError(
						f"Invalid datatype passed to filter. "
						f"Only operators: 'in', 'notin', 'btwn' "
						f"accept (list, tuple). "
						"Datatype dict is an invalid syntax."
					)

		return params

class UpdateFilterValidator(BaseModel):

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
	filters: Annotated[
		Dict[
			str, Tuple[
				Union[str, int, float, bool],
				Union[str, int, float, bool, list, tuple]
			]
		], Field(...)
	]

	@model_validator(mode="before")
	@classmethod
	def _validate_filter_(
		cls,
		params: dict
	):

		filters = params.get("filters", {})
		update_all = params.get("update_all", False)

		for k, v in filters.items():

			if "__" not in k:
				raise ValueError(
					f"Separator '__' was not found in {k}"
				)

			parts = k.split("__")

			if len(parts) == 2 and update_all is True:

				tab = parts[0]
				col = parts[1]

				continue

			elif len(parts) == 4 and update_all is False:

				tab = parts[0]
				col = parts[1]
				ccl = parts[2]
				ope = parts[3]

				val = v[0]
				con = v[1]

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
						f"Invalid datatype passed to filter. "
						f"Only operators: 'in', 'notin', 'btwn' "
						f"accept (list, tuple). "
						"Datatype dict is an invalid syntax."
					)

			if not isinstance(val, cls.TYPES):
				raise ValueError(
					"Invalid datatype passed to be updated. "
					f"Valid datatypes: {cls.TYPES}"
				)

		return params