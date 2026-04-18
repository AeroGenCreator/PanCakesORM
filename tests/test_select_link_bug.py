from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype
import pandas as pd
import ipdb
from pathlib import Path

# GLOBAL PATH para pruebas
dir_ = Path.cwd() / 'data' / 'test_env'
file_ =  dir_ / 'optimization.sqlite'

class Category(PanCakesORM):
	_table = "category"
	_depends = "self"
	_db_dir = dir_
	_db_file = file_

	category = sql_datatype.Char(comment="Category", unq=True)
	
class Product(PanCakesORM):
	_table = "product"
	_depends = ["category"]
	_group_constraint=("product_name", "category_id")
	_db_dir = dir_
	_db_file = file_
	
	product_name = sql_datatype.Char(comment="Product Name", unq=True)
	category_id = sql_datatype.ForeignKey(
		comment="Product Category Rel",
		second_table="category",
		column_id="category_id"
	)

class Recipe(PanCakesORM):
	_table = "recipe"
	_depends = ["self"]
	_db_dir = dir_
	_db_file = file_
	
	recipe_ingredient = sql_datatype.Char(comment="Recipe Ingredient", unq=True)

class RecipeProductLine(PanCakesORM):
	_table = "recipe_product_line"
	_depends = ["recipe", "product"]
	_group_constraint = ("recipe_id", "product_id")
	_db_dir = dir_
	_db_file = file_
	
	recipe_id = sql_datatype.ForeignKey(
		comment="Recipe Line Rel",
		second_table="recipe",
		column_id="recipe_id"
	)
	product_id = sql_datatype.ForeignKey(
		comment="Product Line Rel",
		second_table="product",
		column_id="product_id"
	)
	category_id = sql_datatype.ForeignKey(
		comment="Category Line Rel",
		second_table="category",
		column_id="category_id"
	)

Category.i(category=[(None, "Dessert")])
Product.i(product=[(None, "Concha", 1)])
Recipe.i(recipe=[(None, "Sugar"),(None, "Milk"), (None, "Salt")])
RecipeProductLine.i(
	recipe_product_line=[(None, 1, 1, 1), (None, 2, 1, 1), (None, 3, 1, 1)])

dicc2 = Category.all().to_dict(label=True)
dicc3 = Product.select(
	"product__product_name", "category__category").link(
	"category").all().to_dict(label=True)
dicc4 = Recipe.all().to_dict(label=True)
dicc5 = RecipeProductLine.link("product", "category", "recipe").select(
	"category__category", "product__product_name", "recipe__recipe_ingredient").all().to_dict(label=True)


print()
print(pd.DataFrame(dicc2))
print()
print(pd.DataFrame(dicc3))
print()
print(pd.DataFrame(dicc4))
print()
print(pd.DataFrame(dicc5))
