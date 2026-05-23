# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""-*- Test Self oriented link -*-"""

from pathlib import Path

from pancakes.models.model import PanCakesORM
from pancakes.sql import datatype
from pancakes.abstract.query_box import QueryBox

# GLOBAL PATH para pruebas
dir_ = Path.cwd() / "data" / "test_env"
file_ = dir_ / "optimization.sqlite"


class Category(PanCakesORM):
    _table = "category"
    _depends = "self"
    _db_dir = dir_
    _db_file = file_

    category = datatype.Char(comment="Category", unique=True)


class Product(PanCakesORM):
    _table = "product"
    _depends = ["category"]
    _group_constraint = ("product_name", "category_id")
    _db_dir = dir_
    _db_file = file_

    product_name = datatype.Char(comment="Product Name", unique=True)
    category_id = datatype.ForeignKey(
        comment="Product Category Rel",
        second_table="category",
        column_id="category_id",
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
        comment="Recipe Line Rel", second_table="recipe", column_id="recipe_id"
    )
    product_id = datatype.ForeignKey(
        comment="Product Line Rel",
        second_table="product",
        column_id="product_id",
    )
    category_id = datatype.ForeignKey(
        comment="Category Line Rel",
        second_table="category",
        column_id="category_id",
    )


Category.i(category=[(None, "Dessert")])
Product.i(product=[(None, "Concha", 1)])
Recipe.i(recipe=[(None, "Sugar"), (None, "Milk"), (None, "Salt")])
RecipeProductLine.i(
    recipe_product_line=[(None, 1, 1, 1), (None, 2, 1, 1), (None, 3, 1, 1)]
)


def test_real_1():
    m = QueryBox(Category)
    dicc = m.all().dictionary(label=True)

    assert dicc == [{"CATEGORY ID": 1, "Category": "Dessert"}]


def test_real_2():
    m = QueryBox(Product)
    dicc2 = (
        m.select("product__product_name", "category__category")
        .link("category")
        .all()
        .dictionary(label=True)
    )
    assert dicc2 == [{"Product Name": "Concha", "Category": "Dessert"}]


def test_real_3():
    m = QueryBox(Recipe)
    dicc3 = m.all().dictionary(label=True)
    assert dicc3 == [
        {"RECIPE ID": 1, "Recipe Ingredient": "Sugar"},
        {"RECIPE ID": 2, "Recipe Ingredient": "Milk"},
        {"RECIPE ID": 3, "Recipe Ingredient": "Salt"},
    ]


def test_real_4():
    m = QueryBox(RecipeProductLine)
    dicc4 = (
        m.link("product", "category", "recipe")
        .select(
            "category__category",
            "product__product_name",
            "recipe__recipe_ingredient",
        )
        .all()
        .dictionary(label=True)
    )

    assert dicc4 == [
        {
            "Category": "Dessert",
            "Product Name": "Concha",
            "Recipe Ingredient": "Milk",
        },
        {
            "Category": "Dessert",
            "Product Name": "Concha",
            "Recipe Ingredient": "Salt",
        },
        {
            "Category": "Dessert",
            "Product Name": "Concha",
            "Recipe Ingredient": "Sugar",
        },
    ]


def test_final_link():
    m = QueryBox(Product)
    dicc5 = (
        m.link("recipe", "category", "recipe_product_line")
        .select(
            "recipe_product_line__recipe_product_line_id",
            "category__category",
            "product__product_name",
            "recipe__recipe_ingredient",
        )
        .all()
        .dictionary(label=True)
    )

    assert dicc5 == [
        {
            "RECIPE_PRODUCT_LINE ID": 1,
            "Category": "Dessert",
            "Product Name": "Concha",
            "Recipe Ingredient": "Sugar",
        },
        {
            "RECIPE_PRODUCT_LINE ID": 2,
            "Category": "Dessert",
            "Product Name": "Concha",
            "Recipe Ingredient": "Milk",
        },
        {
            "RECIPE_PRODUCT_LINE ID": 3,
            "Category": "Dessert",
            "Product Name": "Concha",
            "Recipe Ingredient": "Salt",
        },
    ]