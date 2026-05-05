# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

""" -*- Test Self oriented link -*- """
from pancakes.models.model import PanCakesORM
from pancakes.sql import datatype
import pandas as pd
from pathlib import Path

# GLOBAL PATH para pruebas
dir_ = Path.cwd() / 'data' / 'test_env'
file_ =  dir_ / 'optimization.sqlite'

class Category(PanCakesORM):
	_table = "category"
	_depends = "self"
	_db_dir = dir_
	_db_file = file_

	category = datatype.Char(comment="Category", unique=True)
	
class Product(PanCakesORM):
	_table = "product"
	_depends = ["category"]
	_group_constraint=("product_name", "category_id")
	_db_dir = dir_
	_db_file = file_
	
	product_name = datatype.Char(comment="Product Name", unique=True)
	category_id = datatype.ForeignKey(
		comment="Product Category Rel",
		second_table="category",
		column_id="category_id"
	)

class Recipe(PanCakesORM):
	_table = "recipe"
	_depends = ["self"]
	_db_dir = dir_
	_db_file = file_
	
	recipe_ingredient = datatype.Char(comment="Recipe Ingredient", unique=True)

class RecipeProductLine(PanCakesORM):
	_table = "recipe_product_line"
	_depends = ["recipe", "product", "category"]
	_group_constraint = ("recipe_id", "product_id", "category_id")
	_db_dir = dir_
	_db_file = file_
	
	recipe_id = datatype.ForeignKey(
		comment="Recipe Line Rel",
		second_table="recipe",
		column_id="recipe_id"
	)
	product_id = datatype.ForeignKey(
		comment="Product Line Rel",
		second_table="product",
		column_id="product_id"
	)
	category_id = datatype.ForeignKey(
		comment="Category Line Rel",
		second_table="category",
		column_id="category_id"
	)

Category.i(category=[(None, "Dessert")])
Product.i(product=[(None, "Concha", 1)])
Recipe.i(recipe=[(None, "Sugar"),(None, "Milk"), (None, "Salt")])
RecipeProductLine.i(
	recipe_product_line=[(None, 1, 1, 1), (None, 2, 1, 1), (None, 3, 1, 1)])

def test_real_1():
    dicc = Category.all().to_dict(label=True)
    assert dicc == [{'CATEGORY ID': 1, 'Category': 'Dessert'}]
def test_real_2():
    dicc2 = Product.select(
	"product__product_name", "category__category").link(
	"category").all().to_dict(label=True)
    assert dicc2 == [{'Product Name': 'Concha', 'Category': 'Dessert'}]
def test_real_3():
    dicc3 = Recipe.all().to_dict(label=True)
    assert dicc3 == [
    {'RECIPE ID': 1, 'Recipe Ingredient': 'Sugar'},
    {'RECIPE ID': 2, 'Recipe Ingredient': 'Milk'},
    {'RECIPE ID': 3, 'Recipe Ingredient': 'Salt'}
    ]
def test_real_4():
    dicc4 = RecipeProductLine.link("product", "category", "recipe").select(
	"category__category", "product__product_name", "recipe__recipe_ingredient").all().to_dict(label=True)
    assert dicc4 == [
    {'RECIPE PRODUCT LINE ID': 1, 'Category': 'Dessert', 'Product Name': 'Concha', 'Recipe Ingredient': 'Sugar'},
    {'RECIPE PRODUCT LINE ID': 2, 'Category': 'Dessert', 'Product Name': 'Concha', 'Recipe Ingredient': 'Milk'},
    {'RECIPE PRODUCT LINE ID': 3, 'Category': 'Dessert', 'Product Name': 'Concha', 'Recipe Ingredient': 'Salt'}
    ]
def test_final_link():
	dicc5 = Product.link("recipe", "category", "recipe_product_line").select(
	"category__category", "product__product_name", "recipe__recipe_ingredient").all().to_dict(label=True)
	[
	{'RECIPE PRODUCT LINE ID': 1, 'Category': 'Dessert', 'Product Name': 'Concha', 'Recipe Ingredient': 'Sugar'},
	{'RECIPE PRODUCT LINE ID': 2, 'Category': 'Dessert', 'Product Name': 'Concha', 'Recipe Ingredient': 'Milk'}, 
	{'RECIPE PRODUCT LINE ID': 3, 'Category': 'Dessert', 'Product Name': 'Concha', 'Recipe Ingredient': 'Salt'}
	]
