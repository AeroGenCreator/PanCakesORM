# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

""" Tipado Para Pydantic -*- PanCakesORM -*- """

# Modulos PanCakesORM
from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype

# Modulos Python
from pathlib import Path

path_dir = Path.cwd() / "data" / "test_env"
path_db = path_dir / "pydantic.sqlite"


class Category(PanCakesORM):
	_table = "category"
	_depends = "self"
	_db_dir = path_dir
	_db_file = path_db

	category_name = sql_datatype.Char(
		comment="Category Name",
		size=200,
		required=True,
		unique=True,
		default="Unknown"
	)

class Inventory(PanCakesORM):

	# Configuracion Modelo
	_table = "inventory"
	_depends = ["category"]
	_db_dir = path_dir
	_db_file = path_db

	# Campos
	mk_date = sql_datatype.Text(
		comment="C. Date",
		required=True,
		default="2026-28-04"
	)
	product_name = sql_datatype.Char(
		comment="Product Name",
		size=100,
		required=True,
		unique=True,
		default="No Product"
	)
	stock_quantity = sql_datatype.Int(
		comment="Stock Quantity",
        le=100,
        ge=0,
        default=1,
        required=True,
        unique=True
	)
	product_price = sql_datatype.Float(
		comment="Product Price",
		lt=1000,
        gt=0,
        default=0.1,
        required=True,
        unique=True
	)
	saleable = sql_datatype.Bool(
		comment="Saleable Product",
		default=True,
		required=True
	)
	category_id = sql_datatype.ForeignKey(
		second_table="category",
        column_id="category_id",
        comment="Prod. Cate. Rel",
        on_del='set null',
        on_upd='cascade',
        default= 1
	)