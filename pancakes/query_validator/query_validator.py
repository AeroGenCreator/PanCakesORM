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

# Mis Modulos

# Modulos Python
from typing import Annotated, Union, Dict, Tuple, Any, ClassVar, List, Optional, Set

# Modulos de Terceros
from pydantic import BaseModel, model_validator, Field

# --*-- METODOS MODO BASICO --*--


class ValidateSelect(BaseModel):

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

		if not data:
			return data
		
		list_strings = data.get("select", [])

		for string in list_strings:

			if "__" not in string:
				raise ValueError(
					f"Valid separator not found "
					f"in passed string: {string}"
				)

			parts = string.split("__")

			if len(parts) == 2:
				pass

			elif len(parts) ==3:
				agg = parts[2]
				if agg not in cls.VALID_AGGRETATION:
					raise ValueError(
						f"Invalid aggregation function "
						f"passed to selected column "
						f"{agg}"
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


















