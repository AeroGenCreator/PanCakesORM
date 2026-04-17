# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

""" Test QueryBox Class -*- PanCakesORM -*- """

# Modulos Propios
from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype

# Modulos Desarrollo
import ipdb
import pandas as pd

class User(PanCakesORM):
    _table = "user"

    name = sql_datatype.Char(comment="User Name")