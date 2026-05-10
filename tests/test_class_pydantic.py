# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

""" Tipado Para Pydantic -*- PanCakesORM -*- """

# Modulos PanCakesORM
from pancakes.models.model import PanCakesORM
from pancakes.sql import datatype

# Modulos Python
from pathlib import Path

path_dir = Path.cwd() / "data" / "test_env"
path_db = path_dir / "pydantic.sqlite"

class Category(PanCakesORM):
	_table = "category"
	_depends = "self"
	_db_dir = path_dir
	_db_file = path_db

	category_name = datatype.Char(
		comment="Category Name",
		max_length=200,
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
	mk_date = datatype.Text(
		comment="C. Date",
		required=True,
		default="2026-28-04"
	)
	product_name = datatype.Char(
		comment="Product Name",
		max_length=100,
		min_length=10,
		required=True,
		unique=True
	)
	stock_quantity = datatype.Int(
		comment="Stock Quantity",
        le=100,
        ge=1,
        default=10,
        required=True,
	)
	product_price = datatype.Float(
		comment="Product Price",
		lt=1000,
        gt=1,
        default=10,
        required=True,
	)
	saleable = datatype.Bool(
		comment="Saleable Product",
		default=True,
		required=True
	)
	category_id = datatype.ForeignKey(
		second_table="category",
        column_id="category_id",
        comment="Prod. Cate. Rel",
        on_del='set null',
        on_upd='cascade'
	)

create_schema = Inventory.CREATE

# Pydantic Validation Creation:
def test_create_schema():
	create_schema(
		mk_date="2026-29-04",
		product_name="Simple Shampoo",
		stock_quantity=20,
		product_price=10,
		saleable=True
	)
	pass

"""print()
print(Inventory.CREATE)
print()
print(Inventory.ADAPTER)
print()
print(Inventory.ANNOTATED)

print()
print(Category.CREATE)
print()
print(Category.ADAPTER)
print()
print(Category.ANNOTATED)"""

def test_i():
	Inventory.i(
		category=[
			(None, "Categoria Numero 1")
		],
		inventory=[
			(None,"2026-28-04","SuperProducto",10,10,True, 1),
			(None,"2026-28-05","SuperProducto2",12,11,True,None),
			(None,"-miss","SuperProducto3","-miss","-miss","-miss","-miss")
		]
	)

	data = Inventory.return_all()
	assert data == [
		(1, '2026-28-04', 'SuperProducto', 10, 10.0, 1, 1),
		(2, '2026-28-05', 'SuperProducto2', 12, 11.0, 1, None),
		(3, '2026-28-04', 'SuperProducto3', 10, 10.0, 1, None)
	]


