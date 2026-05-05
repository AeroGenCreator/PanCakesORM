# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

""" Test QueryBox Class -*- PanCakesORM -*- """

# Modulos Propios
from pancakes.models.model import PanCakesORM
from pancakes.sql import datatype
from pancakes.orm.insert import insert

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
    _depends = "self"

    name = datatype.Char(comment='Country')

class Client(PanCakesORM):
    _db_dir = dir_
    _db_file = file

    _table = 'client'
    _depends = ["country"]

    name = datatype.Char(comment='Client Name')
    country_id = datatype.ForeignKey(
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
    _depends = ["client"]

    name = datatype.Char(comment='Sale Code')
    client_id = datatype.ForeignKey(
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

    assert api == [
    {'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1},
    {'client__client_id': 2, 'client__name': 'Lupita', 'client__country_id': 1},
    {'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2},
    {'client__client_id': 4, 'client__name': 'Polar', 'client__country_id': 1},
    {'client__client_id': 5, 'client__name': 'Malteada', 'client__country_id': 2}
    ]

def test_box_meth_limit_all():
    api = Client.lim(2).all().to_dict()
    
    assert api == [
    {'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1},
    {'client__client_id': 2, 'client__name': 'Lupita', 'client__country_id': 1}
    ]

def test_box_meth_join_all():
    api = Client.add(
        rg__country=['country_id','client'],
        rg__sale=['client_id','client']).all().to_dict()

    assert api == [
    {'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1, 'country__country_id': 1, 'country__name': 'Mexico', 'sale__sale_id': 1, 'sale__name': 'F1', 'sale__client_id': 1},
    {'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1, 'country__country_id': 1, 'country__name': 'Mexico', 'sale__sale_id': 4, 'sale__name': 'F4', 'sale__client_id': 1},
    {'client__client_id': 2, 'client__name': 'Lupita', 'client__country_id': 1, 'country__country_id': 1, 'country__name': 'Mexico', 'sale__sale_id': 9, 'sale__name': 'F9', 'sale__client_id': 2},
    {'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2, 'country__country_id': 2, 'country__name': 'Brasil', 'sale__sale_id': 2, 'sale__name': 'F2', 'sale__client_id': 3},
    {'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2, 'country__country_id': 2, 'country__name': 'Brasil', 'sale__sale_id': 5, 'sale__name': 'F5', 'sale__client_id': 3},
    {'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2, 'country__country_id': 2, 'country__name': 'Brasil', 'sale__sale_id': 6, 'sale__name': 'F6', 'sale__client_id': 3},
    {'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2, 'country__country_id': 2, 'country__name': 'Brasil', 'sale__sale_id': 8, 'sale__name': 'F8', 'sale__client_id': 3},
    {'client__client_id': 4, 'client__name': 'Polar', 'client__country_id': 1, 'country__country_id': 1, 'country__name': 'Mexico', 'sale__sale_id': 3, 'sale__name': 'F3', 'sale__client_id': 4},
    {'client__client_id': 5, 'client__name': 'Malteada', 'client__country_id': 2, 'country__country_id': 2, 'country__name': 'Brasil', 'sale__sale_id': 7, 'sale__name': 'F7', 'sale__client_id': 5}
    ]


def test_box_meth_join_lim_all():
    api = Client.q().add(
        rg__country=['country_id','client'],
        rg__sale=['client_id','client']).lim(1).all().to_dict()

    assert api == [
    {'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1, 'country__country_id': 1, 'country__name': 'Mexico', 'sale__sale_id': 1, 'sale__name': 'F1', 'sale__client_id': 1}
    ]

def test_box_meth_filter_between_all():

    api = Sale.q().filter(sale__sale_id__btwn=[3,6]).all().to_dict()

    assert api == [
    {'sale__sale_id': 3, 'sale__name': 'F3', 'sale__client_id': 4},
    {'sale__sale_id': 4, 'sale__name': 'F4', 'sale__client_id': 1},
    {'sale__sale_id': 5, 'sale__name': 'F5', 'sale__client_id': 3},
    {'sale__sale_id': 6, 'sale__name': 'F6', 'sale__client_id': 3}
    ]

def test_box_meth_filter_in_all():
    api = Sale.filter(sale__sale_id__in=[3, 4]).all().to_dict()

    assert api == [
    {'sale__sale_id': 3, 'sale__name': 'F3', 'sale__client_id': 4},
    {'sale__sale_id': 4, 'sale__name': 'F4', 'sale__client_id': 1}
    ]

def test_box_meth_filter_ind_all():
    api = Sale.filter(sale__name__same="F1").all().to_dict()

    assert api == [{'sale__sale_id': 1, 'sale__name': 'F1', 'sale__client_id': 1}]

def test_box_meth_filter_logic_all():
    api = Sale.filter(sale__sale_id__btwn__and=[1,3],
        sale__client_id__same=1
        ).all().to_dict()

    assert api == [{'sale__sale_id': 1, 'sale__name': 'F1', 'sale__client_id': 1}]

def test_box_meth_filter_logic_join_all():
    api = Sale.filter(
        sale__sale_id__same__or=1,
        sale__client_id__same=5
        ).link('client').all().to_dict()

    assert api == [
    {'sale__sale_id': 1, 'sale__name': 'F1', 'sale__client_id': 1, 'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1},
    {'sale__sale_id': 7, 'sale__name': 'F7', 'sale__client_id': 5, 'client__client_id': 5, 'client__name': 'Malteada', 'client__country_id': 2}
    ]

def test_box_meth_filter_numeric():
    api = Sale.filter(sale__sale_id__gt=4).all().to_dict()

    assert api == [
    {'sale__sale_id': 5, 'sale__name': 'F5', 'sale__client_id': 3},
    {'sale__sale_id': 6, 'sale__name': 'F6', 'sale__client_id': 3},
    {'sale__sale_id': 7, 'sale__name': 'F7', 'sale__client_id': 5},
    {'sale__sale_id': 8, 'sale__name': 'F8', 'sale__client_id': 3},
    {'sale__sale_id': 9, 'sale__name': 'F9', 'sale__client_id': 2}
    ]

def test_box_meth_id():
    api = Client.filter(
        client__name__in=['Polar','Malteada','Peke']).id().all().to_dict()

    assert api == [
    {'client__client_id': 3},
    {'client__client_id': 4},
    {'client__client_id': 5}
    ]

def test_box_tuple_of_ids():
    api = Client.filter(client__client_id__in=(3, 4, 5)).all().to_dict()

    assert api == [
    {'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2},
    {'client__client_id': 4, 'client__name': 'Polar', 'client__country_id': 1},
    {'client__client_id': 5, 'client__name': 'Malteada', 'client__country_id': 2}
    ]

def test_box_meth_gp_all():
    api = Sale.gp(
        client='country_id').link('client').all().to_dict()

    assert api == [
    {'sale__sale_id': 1, 'sale__name': 'F1', 'sale__client_id': 1, 'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1},
    {'sale__sale_id': 2, 'sale__name': 'F2', 'sale__client_id': 3, 'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2}
    ]

def test_order_add_all():
    # En este caso ocupo el metodo verboso de add()
    # porque la relacion se puede dar uniendo linealmente
    # y no por una relacion Many 2 Many.
    # Si fuera many to many podria ocupar link()
    api = Client.sort(client__name='DESC').add(
        rg__country=['country_id','client'],
        rg__sale=['client_id','client']).all().to_dict()

    assert api == [
    {'client__client_id': 4, 'client__name': 'Polar', 'client__country_id': 1, 'sale__sale_id': 3, 'sale__name': 'F3', 'sale__client_id': 4, 'country__country_id': 1, 'country__name': 'Mexico'},
    {'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2, 'sale__sale_id': 2, 'sale__name': 'F2', 'sale__client_id': 3, 'country__country_id': 2, 'country__name': 'Brasil'},
    {'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2, 'sale__sale_id': 5, 'sale__name': 'F5', 'sale__client_id': 3, 'country__country_id': 2, 'country__name': 'Brasil'},
    {'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2, 'sale__sale_id': 6, 'sale__name': 'F6', 'sale__client_id': 3, 'country__country_id': 2, 'country__name': 'Brasil'},
    {'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2, 'sale__sale_id': 8, 'sale__name': 'F8', 'sale__client_id': 3, 'country__country_id': 2, 'country__name': 'Brasil'},
    {'client__client_id': 5, 'client__name': 'Malteada', 'client__country_id': 2, 'sale__sale_id': 7, 'sale__name': 'F7', 'sale__client_id': 5, 'country__country_id': 2, 'country__name': 'Brasil'},
    {'client__client_id': 2, 'client__name': 'Lupita', 'client__country_id': 1, 'sale__sale_id': 9, 'sale__name': 'F9', 'sale__client_id': 2, 'country__country_id': 1, 'country__name': 'Mexico'},
    {'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1, 'sale__sale_id': 1, 'sale__name': 'F1', 'sale__client_id': 1, 'country__country_id': 1, 'country__name': 'Mexico'},
    {'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1, 'sale__sale_id': 4, 'sale__name': 'F4', 'sale__client_id': 1, 'country__country_id': 1, 'country__name': 'Mexico'}
    ]

# TEST DE HELPERS DIRECTOS:
def test_direct_filter():
    api = Client.filter(
        client__name__in__or=["Andres","Polar"],
        client__client_id__gtsm=5).all().to_dict()

    assert api == [
    {'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1},
    {'client__client_id': 4, 'client__name': 'Polar', 'client__country_id': 1},
    {'client__client_id': 5, 'client__name': 'Malteada', 'client__country_id': 2}
    ]

def test_direct_link():
    api = Client.link('country').all().to_dict()

    assert api == [
    {'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1, 'country__country_id': 1, 'country__name': 'Mexico'},
    {'client__client_id': 2, 'client__name': 'Lupita', 'client__country_id': 1, 'country__country_id': 1, 'country__name': 'Mexico'},
    {'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2, 'country__country_id': 2, 'country__name': 'Brasil'},
    {'client__client_id': 4, 'client__name': 'Polar', 'client__country_id': 1, 'country__country_id': 1, 'country__name': 'Mexico'},
    {'client__client_id': 5, 'client__name': 'Malteada', 'client__country_id': 2, 'country__country_id': 2, 'country__name': 'Brasil'}
    ]

def test_direct_all():
    api = Country.all().to_dict()

    assert api == [
        {'country__country_id': 1, 'country__name': 'Mexico'},
        {'country__country_id': 2, 'country__name': 'Brasil'}
    ]

def test_direct_link_filter():
    api = Sale.link("client").all().to_dict()

    assert api == [
    {'sale__sale_id': 1, 'sale__name': 'F1', 'sale__client_id': 1, 'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1},
    {'sale__sale_id': 2, 'sale__name': 'F2', 'sale__client_id': 3, 'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2},
    {'sale__sale_id': 3, 'sale__name': 'F3', 'sale__client_id': 4, 'client__client_id': 4, 'client__name': 'Polar', 'client__country_id': 1},
    {'sale__sale_id': 4, 'sale__name': 'F4', 'sale__client_id': 1, 'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1},
    {'sale__sale_id': 5, 'sale__name': 'F5', 'sale__client_id': 3, 'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2},
    {'sale__sale_id': 6, 'sale__name': 'F6', 'sale__client_id': 3, 'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2},
    {'sale__sale_id': 7, 'sale__name': 'F7', 'sale__client_id': 5, 'client__client_id': 5, 'client__name': 'Malteada', 'client__country_id': 2},
    {'sale__sale_id': 8, 'sale__name': 'F8', 'sale__client_id': 3, 'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2},
    {'sale__sale_id': 9, 'sale__name': 'F9', 'sale__client_id': 2, 'client__client_id': 2, 'client__name': 'Lupita', 'client__country_id': 1}
    ]

    # hacer to_dict() funcion global en tool.function
    # helper select()

def test_direct_select():
    api = Client.select('client__name').all().to_dict()

    assert api == [
    {'client__name': 'Andres'},
    {'client__name': 'Lupita'},
    {'client__name': 'Peke'},
    {'client__name': 'Polar'},
    {'client__name': 'Malteada'}
    ]

def test_direct_select_agg():
    api = Client.select("client__client_id__sum").all().to_dict()

    assert api == [{'client__client_id__sum': 15}]

def test_direct_select_avg():
    api = Client.select("client__country_id__avg").all().to_dict()

    assert api == [{'client__country_id__avg': 1.4}]

def test_direct_select_link():
    api = Sale.select(
        "sale__name","client__name").link("client").all().to_dict()

    assert api == [
    {'sale__name': 'F1', 'client__name': 'Andres'},
    {'sale__name': 'F2', 'client__name': 'Peke'},
    {'sale__name': 'F3', 'client__name': 'Polar'},
    {'sale__name': 'F4', 'client__name': 'Andres'},
    {'sale__name': 'F5', 'client__name': 'Peke'},
    {'sale__name': 'F6', 'client__name': 'Peke'},
    {'sale__name': 'F7', 'client__name': 'Malteada'},
    {'sale__name': 'F8', 'client__name': 'Peke'},
    {'sale__name': 'F9', 'client__name': 'Lupita'}
    ]

def test_insert_select_link_count():
    api = Sale.select(
        "client__name", "sale__name__count").link(
        "client").gp(client="name").all().to_dict()

    assert api == [
    {'sale__name__count': 2, 'client__name': 'Andres'},
    {'sale__name__count': 1, 'client__name': 'Lupita'},
    {'sale__name__count': 1, 'client__name': 'Malteada'},
    {'sale__name__count': 4, 'client__name': 'Peke'},
    {'sale__name__count': 1, 'client__name': 'Polar'}
    ]

def test_ids_select_warning():
    api = Sale.id().select("sale__name").all().to_dict()

    assert api == [
    {'sale__sale_id': 1},
    {'sale__sale_id': 2},
    {'sale__sale_id': 3},
    {'sale__sale_id': 4},
    {'sale__sale_id': 5},
    {'sale__sale_id': 6},
    {'sale__sale_id': 7},
    {'sale__sale_id': 8},
    {'sale__sale_id': 9}]

def test_helper_count():
    api = Sale.count().to_dict()

    assert api == [{'sale__sale_id__count': 9}]

def test_output_raw():
    row, col = Sale.count().raw()

    assert row == [(9,)]
    assert col == ['sale__sale_id__count']

def test_output_json():
    api = Sale.link("client").all().to_json()

    assert api == {
    'sale':
        {
            'sale_id': [1, 2, 3, 4, 5, 6, 7, 8, 9],
            'name': ['F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9'],
            'client_id': [1, 3, 4, 1, 3, 3, 5, 3, 2]
        },
    'client': 
        {
            'client_id': [1, 3, 4, 1, 3, 3, 5, 3, 2],
            'name': ['Andres', 'Peke', 'Polar', 'Andres', 'Peke', 'Peke', 'Malteada', 'Peke', 'Lupita'],
            'country_id': [1, 2, 1, 1, 2, 2, 2, 2, 1]
        }
    }

def test_reverse_link():
    api = Client.link("sale").all().to_json()

    assert api == {
        'client': 
            {
                'client_id': [1, 3, 4, 1, 3, 3, 5, 3, 2],
                'name': ['Andres', 'Peke', 'Polar', 'Andres', 'Peke', 'Peke', 'Malteada', 'Peke', 'Lupita'],
                'country_id': [1, 2, 1, 1, 2, 2, 2, 2, 1]
            },
        'sale': 
            {
                'sale_id': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                'name': ['F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9'], 
                'client_id': [1, 3, 4, 1, 3, 3, 5, 3, 2]
            }
        }

def test_reverse_link_filter():
    api = Client.link("sale").filter(
        sale__name__same="F4"
    ).all().to_dict()

    assert api == [
    {'client__client_id': 1, 'client__name': 'Andres', 'client__country_id': 1, 'sale__sale_id': 4, 'sale__name': 'F4', 'sale__client_id': 1}
    ]

def test_multi_link_agg_gp():
    api = Client.select(
        "client__name","client__name__count").link("sale", "country").gp(
        client="name").all().to_dict()

    assert api == [
        {'client__name': 'Andres', 'client__name__count': 2},
        {'client__name': 'Lupita', 'client__name__count': 1},
        {'client__name': 'Malteada', 'client__name__count': 1},
        {'client__name': 'Peke', 'client__name__count': 4},
        {'client__name': 'Polar', 'client__name__count': 1}]

def test_output_raw_line_up_ids():
    row, col = Client.id().all().raw(line_up=True)

    assert row == [(1, 2, 3, 4, 5)]
    assert col == ['client__client_id']

def test_output_raw_line_up_():
    row, col = Client.all().raw(line_up=True)

    assert row == [(1, 2, 3, 4, 5), ('Andres', 'Lupita', 'Peke', 'Polar', 'Malteada'), (1, 1, 2, 1, 2)]
    assert col == ['client__client_id', 'client__name', 'client__country_id']

# --*-- QUERYBOX - ETIQUETAS DE FRONTEND RAW() --*--

def test_raw_labels():
    row, col = Client.all().raw(label=True)

    assert row == [(1, 'Andres', 1), (2, 'Lupita', 1), (3, 'Peke', 2), (4, 'Polar', 1), (5, 'Malteada', 2)]
    assert col == ['CLIENT ID', 'Client Name', 'Country Rel']

def test_raw_labels_join():
    row, col = Client.link('sale').all().raw(label=True)

    assert row == [
        (1, 'Andres', 1, 1, 'F1', 1),
        (3, 'Peke', 2, 2, 'F2', 3),
        (4, 'Polar', 1, 3, 'F3', 4),
        (1, 'Andres', 1, 4, 'F4', 1),
        (3, 'Peke', 2, 5, 'F5', 3),
        (3, 'Peke', 2, 6, 'F6', 3),
        (5, 'Malteada', 2, 7, 'F7', 5),
        (3, 'Peke', 2, 8, 'F8', 3),
        (2, 'Lupita', 1, 9, 'F9', 2)
    ]

    assert col == ['CLIENT ID', 'Client Name', 'Country Rel', 'SALE ID', 'Sale Code', 'Cliente Rel']

def test_raw_labels_full_join():
    row, col = Client.link('sale', 'country').all().raw(label=True)

    assert row == [
    (1, 'Andres', 1, 1, 'Mexico', 1, 'F1', 1),
    (3, 'Peke', 2, 2, 'Brasil', 2, 'F2', 3),
    (4, 'Polar', 1, 1, 'Mexico', 3, 'F3', 4),
    (1, 'Andres', 1, 1, 'Mexico', 4, 'F4', 1),
    (3, 'Peke', 2, 2, 'Brasil', 5, 'F5', 3),
    (3, 'Peke', 2, 2, 'Brasil', 6, 'F6', 3),
    (5, 'Malteada', 2, 2, 'Brasil', 7, 'F7', 5),
    (3, 'Peke', 2, 2, 'Brasil', 8, 'F8', 3),
    (2, 'Lupita', 1, 1, 'Mexico', 9, 'F9', 2)
    ]

    assert col == ['CLIENT ID', 'Client Name', 'Country Rel', 'COUNTRY ID', 'Country', 'SALE ID', 'Sale Code', 'Cliente Rel']

def test_raw_labels_select():
    vec, col = Client.select('client__name').all().raw(label=True, line_up=True)

    assert vec == [('Andres', 'Lupita', 'Peke', 'Polar', 'Malteada')]
    assert col == ['Client Name']

def test_raw_labels_select_multi():
    row, col = Client().select(
        "client__name", "sale__name"
    ).link("sale").all().raw(label=True)

    assert row == [
        ('Andres', 'F1'),
        ('Peke', 'F2'), 
        ('Polar', 'F3'),
        ('Andres', 'F4'), 
        ('Peke', 'F5'),
        ('Peke', 'F6'), 
        ('Malteada', 'F7'),
        ('Peke', 'F8'),
        ('Lupita', 'F9')
    ]
    assert col == ['Client Name', 'Sale Code']

def test_raw_labels_select_multi_agg():
    row, col = Client.select(
        "client__name","sale__name__count"
    ).link("sale").gp(client="name").all().raw(label=True)

    assert row == [('Andres', 2), ('Lupita', 1), ('Malteada', 1), ('Peke', 4), ('Polar', 1)]
    assert col == ['Client Name', 'Sale Code COUNT']

def test_raw_labels_select_full_agg():
    row, col = Client.select(
        "client__name","sale__name__count", "country__name"
    ).link("sale", "country").gp(client="name").all().raw(label=True)

    assert row == [
        ('Andres', 2, 'Mexico'),
        ('Lupita', 1, 'Mexico'),
        ('Malteada', 1, 'Brasil'),
        ('Peke', 4, 'Brasil'),
        ('Polar', 1, 'Mexico')
    ]
    assert col == ['Client Name', 'Sale Code COUNT', 'Country']

# --*-- QUERYBOX - ETIQUETAS DE FRONTEND TO_DICT() --*--

def test_dict_label():
    dicc = Client.all().to_dict(label=True)

    assert dicc == [
    {'CLIENT ID': 1, 'Client Name': 'Andres', 'Country Rel': 1},
    {'CLIENT ID': 2, 'Client Name': 'Lupita', 'Country Rel': 1},
    {'CLIENT ID': 3, 'Client Name': 'Peke', 'Country Rel': 2},
    {'CLIENT ID': 4, 'Client Name': 'Polar', 'Country Rel': 1},
    {'CLIENT ID': 5, 'Client Name': 'Malteada', 'Country Rel': 2}
    ]

def test_dict_label_join():
    dicc = Client.link('country').all().to_dict(label=True)

    assert dicc == [
    {'CLIENT ID': 1, 'Client Name': 'Andres', 'Country Rel': 1, 'COUNTRY ID': 1, 'Country': 'Mexico'},
    {'CLIENT ID': 2, 'Client Name': 'Lupita', 'Country Rel': 1, 'COUNTRY ID': 1, 'Country': 'Mexico'},
    {'CLIENT ID': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'COUNTRY ID': 2, 'Country': 'Brasil'},
    {'CLIENT ID': 4, 'Client Name': 'Polar', 'Country Rel': 1, 'COUNTRY ID': 1, 'Country': 'Mexico'},
    {'CLIENT ID': 5, 'Client Name': 'Malteada', 'Country Rel': 2, 'COUNTRY ID': 2, 'Country': 'Brasil'}
    ]

def test_dict_label_full_join():
    dicc = Client.link('sale','country').all().to_dict(label=True)

    assert dicc == [
    {'CLIENT ID': 1, 'Client Name': 'Andres', 'Country Rel': 1, 'COUNTRY ID': 1, 'Country': 'Mexico', 'SALE ID': 1, 'Sale Code': 'F1', 'Cliente Rel': 1},
    {'CLIENT ID': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'COUNTRY ID': 2, 'Country': 'Brasil', 'SALE ID': 2, 'Sale Code': 'F2', 'Cliente Rel': 3},
    {'CLIENT ID': 4, 'Client Name': 'Polar', 'Country Rel': 1, 'COUNTRY ID': 1, 'Country': 'Mexico', 'SALE ID': 3, 'Sale Code': 'F3', 'Cliente Rel': 4},
    {'CLIENT ID': 1, 'Client Name': 'Andres', 'Country Rel': 1, 'COUNTRY ID': 1, 'Country': 'Mexico', 'SALE ID': 4, 'Sale Code': 'F4', 'Cliente Rel': 1},
    {'CLIENT ID': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'COUNTRY ID': 2, 'Country': 'Brasil', 'SALE ID': 5, 'Sale Code': 'F5', 'Cliente Rel': 3},
    {'CLIENT ID': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'COUNTRY ID': 2, 'Country': 'Brasil', 'SALE ID': 6, 'Sale Code': 'F6', 'Cliente Rel': 3},
    {'CLIENT ID': 5, 'Client Name': 'Malteada', 'Country Rel': 2, 'COUNTRY ID': 2, 'Country': 'Brasil', 'SALE ID': 7, 'Sale Code': 'F7', 'Cliente Rel': 5},
    {'CLIENT ID': 3, 'Client Name': 'Peke', 'Country Rel': 2, 'COUNTRY ID': 2, 'Country': 'Brasil', 'SALE ID': 8, 'Sale Code': 'F8', 'Cliente Rel': 3},
    {'CLIENT ID': 2, 'Client Name': 'Lupita', 'Country Rel': 1, 'COUNTRY ID': 1, 'Country': 'Mexico', 'SALE ID': 9, 'Sale Code': 'F9', 'Cliente Rel': 2}
    ]

def test_dict_label_select():
    dicc = Client.select("client__name").all().to_dict()

    assert dicc == [
        {'client__name': 'Andres'},
        {'client__name': 'Lupita'},
        {'client__name': 'Peke'},
        {'client__name': 'Polar'},
        {'client__name': 'Malteada'}
    ]

def test_dict_label_select_multi():
    dicc = Client().select(
        "client__name", "sale__name"
    ).link("sale").all().to_dict(label=True)

    assert dicc == [
        {'Client Name': 'Andres', 'Sale Code': 'F1'},
        {'Client Name': 'Peke', 'Sale Code': 'F2'},
        {'Client Name': 'Polar', 'Sale Code': 'F3'},
        {'Client Name': 'Andres', 'Sale Code': 'F4'},
        {'Client Name': 'Peke', 'Sale Code': 'F5'},
        {'Client Name': 'Peke', 'Sale Code': 'F6'},
        {'Client Name': 'Malteada', 'Sale Code': 'F7'},
        {'Client Name': 'Peke', 'Sale Code': 'F8'},
        {'Client Name': 'Lupita', 'Sale Code': 'F9'}
    ]

def test_dict_label_select_multi_agg():
    dicc = Client.select(
        "client__name","sale__name__count"
    ).link("sale").gp(client="name").all().to_dict(label=True)

    assert dicc == [
        {'Client Name': 'Andres', 'Sale Code COUNT': 2},
        {'Client Name': 'Lupita', 'Sale Code COUNT': 1},
        {'Client Name': 'Malteada', 'Sale Code COUNT': 1},
        {'Client Name': 'Peke', 'Sale Code COUNT': 4},
        {'Client Name': 'Polar', 'Sale Code COUNT': 1}
    ]

def test_dict_label_select_full_agg():
    dicc = Client.select(
        "client__name","sale__name__count", "country__name"
    ).link("sale", "country").gp(client="name").all().to_dict(label=True)

    assert dicc == [
        {'Client Name': 'Andres', 'Sale Code COUNT': 2, 'Country': 'Mexico'},
        {'Client Name': 'Lupita', 'Sale Code COUNT': 1, 'Country': 'Mexico'},
        {'Client Name': 'Malteada', 'Sale Code COUNT': 1, 'Country': 'Brasil'},
        {'Client Name': 'Peke', 'Sale Code COUNT': 4, 'Country': 'Brasil'},
        {'Client Name': 'Polar', 'Sale Code COUNT': 1, 'Country': 'Mexico'}
    ]

# --*-- QUERYBOX - ETIQUETAS DE FRONTEND JSON() --*--

def test_to_json_label():
    api = Client.all().to_json(label=True)
    
    assert api == {
        'client': {
            'CLIENT ID': [1, 2, 3, 4, 5],
            'Client Name': ['Andres', 'Lupita', 'Peke', 'Polar', 'Malteada'],
            'Country Rel': [1, 1, 2, 1, 2]
        }
    }

def test_to_json_label_join():
    api = Client.link('country').all().to_json(label=True)
    
    assert api == {
        'client': {
            'CLIENT ID': [1, 2, 3, 4, 5],
            'Client Name': ['Andres', 'Lupita', 'Peke', 'Polar', 'Malteada'],
            'Country Rel': [1, 1, 2, 1, 2]
        },
        'country': {
            'COUNTRY ID': [1, 1, 2, 1, 2],
            'Country': ['Mexico', 'Mexico', 'Brasil', 'Mexico', 'Brasil']
        }
    }

def test_to_json_label_full_join():
    api = Client.link('sale','country').all().to_json(label=True)
    
    assert api == {
        'client': {
            'CLIENT ID': [1, 3, 4, 1, 3, 3, 5, 3, 2],
            'Client Name': ['Andres', 'Peke', 'Polar', 'Andres', 'Peke', 'Peke', 'Malteada', 'Peke', 'Lupita'],
            'Country Rel': [1, 2, 1, 1, 2, 2, 2, 2, 1]
        },
        'country': {
            'COUNTRY ID': [1, 2, 1, 1, 2, 2, 2, 2, 1],
            'Country': ['Mexico', 'Brasil', 'Mexico', 'Mexico', 'Brasil', 'Brasil', 'Brasil', 'Brasil', 'Mexico']
        },
        'sale': {
            'SALE ID': [1, 2, 3, 4, 5, 6, 7, 8, 9],
            'Sale Code': ['F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9'],
            'Cliente Rel': [1, 3, 4, 1, 3, 3, 5, 3, 2]
        }
    }

def test_to_json_label_select():
    api = Client.select("client__name").all().to_json()
    
    assert api == {'client': {'name': ['Andres', 'Lupita', 'Peke', 'Polar', 'Malteada']}}

def test_to_json_label_select_multi():
    api = Client().select(
        "client__name", "sale__name"
    ).link("sale").all().to_json(label=True)

    assert api == {
    'client': {
        'Client Name': ['Andres', 'Peke', 'Polar', 'Andres', 'Peke', 'Peke', 'Malteada', 'Peke', 'Lupita']
        },
    'sale': {
        'Sale Code': ['F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9']
        }
    }

def test_to_json_label_select_multi_agg():
    api = Client.select(
        "client__name","sale__name__count"
    ).link("sale").gp(client="name").all().to_json(label=True)

    assert api == {
        'client': {
            'Client Name': ['Andres', 'Lupita', 'Malteada', 'Peke', 'Polar']
        },
        'sale': {
            'Sale Code COUNT': [2, 1, 1, 4, 1]
        }
    }

def test_to_json_label_select_full_agg():
    api = Client.select(
        "client__name","sale__name__count", "country__name"
    ).link("sale", "country").gp(client="name").all().to_json(label=True)
    
    assert api == {
        'client': {
            'Client Name': ['Andres', 'Lupita', 'Malteada', 'Peke', 'Polar']
        },
        'sale': {
            'Sale Code COUNT': [2, 1, 1, 4, 1]
        },
        'country': {
            'Country': ['Mexico', 'Mexico', 'Brasil', 'Brasil', 'Mexico']
        }
    }

# TESTING -> "DATA TRUNCADA" -> POR PAQUETES

def test_lim_off():
    dicc = Client.off(num=2).lim(2).all().to_dict()

    assert dicc == [
    {'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2},
    {'client__client_id': 4, 'client__name': 'Polar', 'client__country_id': 1}
    ]

def test_lim_off_sort():
    dicc = Sale.sort(sale__sale_id="DESC").link("client").lim(2).off(2).all().to_dict()

    assert dicc == [
    {'sale__sale_id': 7, 'sale__name': 'F7', 'sale__client_id': 5, 'client__client_id': 5, 'client__name': 'Malteada', 'client__country_id': 2},
    {'sale__sale_id': 6, 'sale__name': 'F6', 'sale__client_id': 3, 'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2}
    ]

# OJO CON LOS JOINS QUE NO SE INTERSECTAN POR UNA LINEA
# Aqui la relacion, a pais es a traves de "client" pero "sale" no tiene
# una dependencia directa a "country" por tanto se usa add() y LEFT JOIN
def test_complex_link():
    dicc = Sale.sort(
        sale__name="DESC").add(
        lf__client=["client_id", "sale"],
        lf__country=["country_id", "client"]).lim(
        2).off(
        2).all(
        ).to_dict()

    assert dicc == [
    {'sale__sale_id': 7, 'sale__name': 'F7', 'sale__client_id': 5, 'client__client_id': 5, 'client__name': 'Malteada', 'client__country_id': 2, 'country__country_id': 2, 'country__name': 'Brasil'},
    {'sale__sale_id': 6, 'sale__name': 'F6', 'sale__client_id': 3, 'client__client_id': 3, 'client__name': 'Peke', 'client__country_id': 2, 'country__country_id': 2, 'country__name': 'Brasil'}]