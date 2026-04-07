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
    {'client__name': 'Polar', 'client__name__count': 1}
    ]