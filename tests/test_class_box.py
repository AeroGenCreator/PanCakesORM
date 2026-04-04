# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

""" Test QueryBox Class -*- PanCakesORM -*- """

# Modulos Propios
from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype
from pancakes.cook.furnace import insert
from pancakes.tool.box import QueryBox

# Modulos Python
from pathlib import Path
from datetime import date

# Modulos Desarrollo
import ipdb
import pandas as pd

# GLOBAL PATH para pruebas
dir_ = Path.cwd() / 'data' / 'test_env'
file =  dir_ / 'box.sqlite'

class Country(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = 'country'

    name = sql_datatype.Char(comment='Country')

class Client(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = 'client'

    name = sql_datatype.Char(comment='Client Name')
    country_id = sql_datatype.ForeignKey(
        second_table='country',
        column_id='country_id',
        comment="Country Rel",
        on_del='cascade',
        on_upd='cascade'
    )

class Sale(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = 'sale'

    name = sql_datatype.Char(comment='Sale Code')
    client_id = sql_datatype.ForeignKey(
        second_table='client',
        column_id='client_id',
        comment='Cliente Rel',
        on_del='cascade',
        on_upd='cascade'
    )

insert(
    db_path=file,
    params=[
        {
        'table':'country',
        'data':[
                (None, 'Mexico'),
                (None, 'Brasil')
            ]
        },
        {
        'table':'client',
        'data':[
                (None, 'Andres', 1),
                (None, 'Lupita', 1),
                (None, 'Peke', 2),
                (None, 'Polar', 1),
                (None, 'Malteada', 2),
            ]
        },
        {
        'table':'sale',
        'data':[
                (None, 'F1', 1),
                (None, 'F2', 3),
                (None, 'F3', 4),
                (None, 'F4', 1),
                (None, 'F5', 3),
                (None, 'F6', 3),
                (None, 'F7', 5),
                (None, 'F8', 3),
                (None, 'F9', 2),
            ]
        }
        
    ]
)

# API directo (PanCakesORM)(query())(QueryBox) <- Todo en conjunto
def test_box_meth_all():
    api = Client.all().to_dict()

    api == [
        {'client_id': 1, 'name': 'Andres', 'country_id': 1},
        {'client_id': 2, 'name': 'Lupita', 'country_id': 1},
        {'client_id': 3, 'name': 'Peke', 'country_id': 2},
        {'client_id': 4, 'name': 'Polar', 'country_id': 1},
        {'client_id': 5, 'name': 'Malteada', 'country_id': 2}
    ]


def test_box_meth_limit_all():
    api = Client.lim(2).all().to_dict()
    
    api = [
        {'client_id': 1, 'name': 'Andres', 'country_id': 1},
        {'client_id': 2, 'name': 'Lupita', 'country_id': 1}
    ]

"""
def test_box_meth_join_all():
    row, col = Client.q().add(
        rg__country=['country_id','client'],
        rg__sale=['client_id','client']).all()

    lab1 = Client.comment
    lab2 = Country.comment
    lab3 = Sale.comment

    labs = lab1 + lab2 + lab3

    api = QueryBox().to_dict(row, labs)

    assert api == [
    {'Client id': 1, 'Client Name': 'Andres', 'Country Rel': 1, 'Country id': 1, 'Country': 'Mexico', 'Sale id': 1, 'Sale Code': 'F1', 'Cliente Rel': 1},
    {'Client id': 1, 'Client Name': 'Andres', 'Country Rel': 1, 'Country id': 1, 'Country': 'Mexico', 'Sale id': 4, 'Sale Code': 'F4', 'Cliente Rel': 1},
    {'Client id': 2, 'Client Name': 'Lupita', 'Country Rel': 1, 'Country id': 1, 'Country': 'Mexico', 'Sale id': 9, 'Sale Code': 'F9', 'Cliente Rel': 2},
    {'Client id': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'Country id': 2, 'Country': 'Brasil', 'Sale id': 2, 'Sale Code': 'F2', 'Cliente Rel': 3},
    {'Client id': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'Country id': 2, 'Country': 'Brasil', 'Sale id': 5, 'Sale Code': 'F5', 'Cliente Rel': 3},
    {'Client id': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'Country id': 2, 'Country': 'Brasil', 'Sale id': 6, 'Sale Code': 'F6', 'Cliente Rel': 3},
    {'Client id': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'Country id': 2, 'Country': 'Brasil', 'Sale id': 8, 'Sale Code': 'F8', 'Cliente Rel': 3},
    {'Client id': 4, 'Client Name': 'Polar', 'Country Rel': 1, 'Country id': 1, 'Country': 'Mexico', 'Sale id': 3, 'Sale Code': 'F3', 'Cliente Rel': 4},
    {'Client id': 5, 'Client Name': 'Malteada', 'Country Rel': 2, 'Country id': 2, 'Country': 'Brasil', 'Sale id': 7, 'Sale Code': 'F7', 'Cliente Rel': 5}
    ]

def test_box_meth_join_lim_all():
    row, col = Client.q().add(
        rg__country=['country_id','client'],
        rg__sale=['client_id','client']).lim(1).all()

    lab1 = Client.comment
    lab2 = Country.comment
    lab3 = Sale.comment

    labs = lab1 + lab2 + lab3

    api = QueryBox().to_dict(row, labs)

    assert api == [
    {'Client id': 1, 'Client Name': 'Andres', 'Country Rel': 1, 'Country id': 1, 'Country': 'Mexico', 'Sale id': 1, 'Sale Code': 'F1', 'Cliente Rel': 1}
    ]

def test_box_meth_filter_between_all():

    row, col = Sale.q().filter(sale__sale_id__btwn=[3,6]).all()
    lab = Sale.comment
    api = QueryBox().to_dict(row, lab)

    assert api == [
        {'Sale id': 3, 'Sale Code': 'F3', 'Cliente Rel': 4},
        {'Sale id': 4, 'Sale Code': 'F4', 'Cliente Rel': 1},
        {'Sale id': 5, 'Sale Code': 'F5', 'Cliente Rel': 3},
        {'Sale id': 6, 'Sale Code': 'F6', 'Cliente Rel': 3}
    ]


def test_box_meth_filter_in_all():
    row, col = Sale.q().filter(sale__sale_id__in=[3, 4]).all()

    lab = Sale.comment

    api = QueryBox().to_dict(row, lab)

    assert api == [
        {'Sale id': 3, 'Sale Code': 'F3', 'Cliente Rel': 4},
        {'Sale id': 4, 'Sale Code': 'F4', 'Cliente Rel': 1}
    ]


def test_box_meth_filter_ind_all():
    row, col = Sale.q().filter(sale__name__same="F1").all()

    lab = Sale.comment

    api = QueryBox().to_dict(row, lab)

    assert api == [{'Sale id': 1, 'Sale Code': 'F1', 'Cliente Rel': 1}]

def test_box_meth_filter_logic_all():
    row, col = Sale.q().filter(sale__sale_id__btwn__and=[1,3],
        sale__client_id__same=1
        ).all()

    lab = Sale.comment

    api = QueryBox().to_dict(row, lab)

    assert api == [{'Sale id': 1, 'Sale Code': 'F1', 'Cliente Rel': 1}]

def test_box_meth_filter_logic_join_all():
    row, col = Sale.q().filter(
        sale__sale_id__same__or=1,
        sale__client_id__same=5
        ).add(lf__client=['client_id', 'sale']).all()

    lab1 = Sale.comment
    lab2 = Client.comment

    labs = lab1 + lab2

    api = QueryBox().to_dict(row, labs)

    assert api == [
    {'Sale id': 1, 'Sale Code': 'F1', 'Cliente Rel': 1, 'Client id': 1, 'Client Name': 'Andres', 'Country Rel': 1},
    {'Sale id': 7, 'Sale Code': 'F7', 'Cliente Rel': 5, 'Client id': 5, 'Client Name': 'Malteada', 'Country Rel': 2}]

def test_box_meth_filter_numeric():
    row, col = Sale.q().filter(sale__sale_id__gt=4).all()

    lab = Sale.comment

    api = QueryBox().to_dict(row, lab)

    assert api == [
        {'Sale id': 5, 'Sale Code': 'F5', 'Cliente Rel': 3},
        {'Sale id': 6, 'Sale Code': 'F6', 'Cliente Rel': 3},
        {'Sale id': 7, 'Sale Code': 'F7', 'Cliente Rel': 5},
        {'Sale id': 8, 'Sale Code': 'F8', 'Cliente Rel': 3},
        {'Sale id': 9, 'Sale Code': 'F9', 'Cliente Rel': 2}
    ]

def test_box_meth_id():
    row, col = Client.q().filter(
        client__name__in=['Polar','Malteada','Peke']).id().all()

    lab = Client.comment

    # Caso cuando llamamos a los ids
    api = [{col[0]: row}]

    assert row == (3, 4, 5)
    assert api == [{'client_id': (3, 4, 5)}]

def test_box_tuple_of_ids():
    row, col = Client.q().filter(client__client_id__in=(3, 4, 5)).all()

    lab = Client.comment

    api = QueryBox().to_dict(row, lab)

    assert api == [
        {'Client id': 3, 'Client Name': 'Peke', 'Country Rel': 2},
        {'Client id': 4, 'Client Name': 'Polar', 'Country Rel': 1},
        {'Client id': 5, 'Client Name': 'Malteada', 'Country Rel': 2}
    ]

def test_box_meth_gp_all():
    row, col = Sale.q().gp(
        client='country_id').add(
        lf__client=['client_id','sale']).all()

    lab = Sale.comment

    api = QueryBox().to_dict(row, lab)

    assert api == [
        {'Sale id': 1, 'Sale Code': 'F1', 'Cliente Rel': 1},
        {'Sale id': 2, 'Sale Code': 'F2', 'Cliente Rel': 3}
    ]

def test_order_add_all():
    row, col = Client.q().sorts(client__name='DESC').add(
        rg__country=['country_id','client'],
        rg__sale=['client_id','client']).all()

    lab1 = Client.comment
    lab2 = Country.comment
    lab3 = Sale.comment

    labs = lab1 + lab2 + lab3

    api = QueryBox().to_dict(row, labs)

    assert api == [
    {'Client id': 4, 'Client Name': 'Polar', 'Country Rel': 1, 'Country id': 1, 'Country': 'Mexico', 'Sale id': 3, 'Sale Code': 'F3', 'Cliente Rel': 4},
    {'Client id': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'Country id': 2, 'Country': 'Brasil', 'Sale id': 2, 'Sale Code': 'F2', 'Cliente Rel': 3},
    {'Client id': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'Country id': 2, 'Country': 'Brasil', 'Sale id': 5, 'Sale Code': 'F5', 'Cliente Rel': 3},
    {'Client id': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'Country id': 2, 'Country': 'Brasil', 'Sale id': 6, 'Sale Code': 'F6', 'Cliente Rel': 3},
    {'Client id': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'Country id': 2, 'Country': 'Brasil', 'Sale id': 8, 'Sale Code': 'F8', 'Cliente Rel': 3},
    {'Client id': 5, 'Client Name': 'Malteada', 'Country Rel': 2, 'Country id': 2, 'Country': 'Brasil', 'Sale id': 7, 'Sale Code': 'F7', 'Cliente Rel': 5},
    {'Client id': 2, 'Client Name': 'Lupita', 'Country Rel': 1, 'Country id': 1, 'Country': 'Mexico', 'Sale id': 9, 'Sale Code': 'F9', 'Cliente Rel': 2},
    {'Client id': 1, 'Client Name': 'Andres', 'Country Rel': 1, 'Country id': 1, 'Country': 'Mexico', 'Sale id': 1, 'Sale Code': 'F1', 'Cliente Rel': 1},
    {'Client id': 1, 'Client Name': 'Andres', 'Country Rel': 1, 'Country id': 1, 'Country': 'Mexico', 'Sale id': 4, 'Sale Code': 'F4', 'Cliente Rel': 1}
    ]

# TEST DE HELPERS DIRECTOS:
def test_direct_filter():
    row, col = Client.filter(
        client__name__in__or=["Andres","Polar"],
        client__client_id__gtsm=5).all()

    api = Client.q().to_dict(row, col)

    assert api == [
        {'client_id': 1, 'name': 'Andres', 'country_id': 1},
        {'client_id': 4, 'name': 'Polar', 'country_id': 1},
        {'client_id': 5, 'name': 'Malteada', 'country_id': 2}
    ]

def test_direct_link():
    row, col = Client.link('country').all()

    api = Client.q().to_dict(row, col)

    assert api == [
        {'client_id 0': 1, 'name 1': 'Andres', 'country_id 2': 1, 'country_id 3': 1, 'name 4': 'Mexico'},
        {'client_id 0': 2, 'name 1': 'Lupita', 'country_id 2': 1, 'country_id 3': 1, 'name 4': 'Mexico'},
        {'client_id 0': 3, 'name 1': 'Peke', 'country_id 2': 2, 'country_id 3': 2, 'name 4': 'Brasil'},
        {'client_id 0': 4, 'name 1': 'Polar', 'country_id 2': 1, 'country_id 3': 1, 'name 4': 'Mexico'},
        {'client_id 0': 5, 'name 1': 'Malteada', 'country_id 2': 2, 'country_id 3': 2, 'name 4': 'Brasil'}
    ]

def test_direct_all():
    row, col = Country.all()

    api = Client.q().to_dict(row, col)

    assert api == [
        {'country_id': 1, 'name': 'Mexico'},
        {'country_id': 2, 'name': 'Brasil'}
    ]

def test_direct_link_filter():
    row, col = Sale.link("client").all() # <- Ya no estoy mandando q()

    api = Sale.q().to_dict(row, col)

    assert api == [
        {'sale_id 0': 1, 'name 1': 'F1', 'client_id 2': 1, 'client_id 3': 1, 'name 4': 'Andres', 'country_id 5': 1},
        {'sale_id 0': 2, 'name 1': 'F2', 'client_id 2': 3, 'client_id 3': 3, 'name 4': 'Peke', 'country_id 5': 2},
        {'sale_id 0': 3, 'name 1': 'F3', 'client_id 2': 4, 'client_id 3': 4, 'name 4': 'Polar', 'country_id 5': 1},
        {'sale_id 0': 4, 'name 1': 'F4', 'client_id 2': 1, 'client_id 3': 1, 'name 4': 'Andres', 'country_id 5': 1},
        {'sale_id 0': 5, 'name 1': 'F5', 'client_id 2': 3, 'client_id 3': 3, 'name 4': 'Peke', 'country_id 5': 2},
        {'sale_id 0': 6, 'name 1': 'F6', 'client_id 2': 3, 'client_id 3': 3, 'name 4': 'Peke', 'country_id 5': 2},
        {'sale_id 0': 7, 'name 1': 'F7', 'client_id 2': 5, 'client_id 3': 5, 'name 4': 'Malteada', 'country_id 5': 2},
        {'sale_id 0': 8, 'name 1': 'F8', 'client_id 2': 3, 'client_id 3': 3, 'name 4': 'Peke', 'country_id 5': 2},
        {'sale_id 0': 9, 'name 1': 'F9', 'client_id 2': 2, 'client_id 3': 2, 'name 4': 'Lupita', 'country_id 5': 1}
    ]"""