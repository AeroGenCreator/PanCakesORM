# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""
Declaracion De Clase CoffeeShop() Metodos Insert | Delete | Update
facilitados a traves de helpers.
de **kwargs y encadenamiento de metodes.
"""

# Modulos Propios
from ..cook.furnace import insert  # Funcion insert()
from ..cook.ingredient import update  # Funcion update()
from ..cook.clean import delete  # Funcion delete()
from .function import logger

# Modulos Python
import warnings


class CoffeeShop:
    """
    CoffeeShop genera una capa de abstraccion para las funciones:

    - insert()
    - update()
    - delete()

    Esto permite que las funciones anteriores puedan ser usadas
    de manera declarativa. Esto sobre clases PanCakesORM directamente
    o generando instancias a traves de CoffeeShop si se desea un uso global.
    """

    def __init__(self, model=None):
        self.model = model

    def i(self, db_path=None, **kwargs) -> None:
        """
        Los argumentos deben ser:

        tabla = datos

        Los datos deben ser una lista de tuplas.
        Ejemplo:
        user = [(None, 'Mexico')]
        """

        # Aseguramos la ruta por defecto.
        path = db_path if db_path else self.model._db_file

        argument = []
        for k, v in kwargs.items():

            # Validamos que los datos sean listas:
            if not isinstance(v, list):
                msg = (f"Invalid datatype passed to method .i()")
                logger.critical(msg)
                raise TypeError(type(v))

            argument.append({'table': k, 'data': v})

        insert(db_path=path, params=argument)
        return

    def u(
        self,
        update_all: bool = False,
        db_path=None,
        **kwargs
    ) -> None:
        """
        La actualizacion de datos en una tabla se da a traves de un
        diccionario en la funcion update(). Para poder usar este metodo
        declarativo se convierten las llaves del diccionario en variables:

        user__name__user_id__same = (Raul, 10)
        user__name__user_id__same = (data, valor_de_condicion)

        Equivale a:

        params = [{
            'table':'user',     <- Tabla
            'name':'name',      <- Campo donde se realiza un cambio
            'data':'Raul'       <- Nuevos datos
            'condition':[{      <- Condicion (Lista de diccionarios)
                'column':'user_id',  <- Columna de condicion
                'operator':'=', <- Operador
                'value':'10',   <- Valor de condicion
                'logic':'',     <- Operador logico
            }]
        }]

        NOTA: Este helper no permite actulizar por multiples condiciones
        esta pensado para ser usado por ids. Si se necesita una
        actualizacion por multiples condiciones usar metodo .update()

        Comparadores Validos:
        OPERATOR = {
        same: "=",
        lt: "<",
        ltsm: "<=",
        gt: ">",
        gtsm: ">=",
        diff:"<>",
        in: "IN",
        notin: "NOT IN",
        btwn: "BETWEEN",
        is: "IS",
        isnot: "IS NOT",
        like: "LIKE",
        notlike: "NOT LIKE"
        }

        LOGICS = {'AND', 'OR', ''}
        """

        OPERATOR = {
            "same": "=",
            "lt": "<",
            "ltsm": "<=",
            "gt": ">",
            "gtsm": ">=",
            "diff": "<>",
            "in": "IN",
            "notin": "NOT IN",
            "btwn": "BETWEEN",
            "is": "IS",
            "isnot": "IS NOT",
            "like": "LIKE",
            "notlike": "NOT LIKE"
        }

        argument = []
        if not update_all:

            for k, v in kwargs.items():
                dicc = {}

                # Validar que el argumento sea una tupla | lista:
                if not isinstance(v, (tuple, list)):
                    msg = ("Invalid datatype - argument in method .u().")
                    logger.critical(msg)
                    raise TypeError(type(v))

                # Validar que los elementos de la tupla sean dos:
                if len(v) != 2:
                    msg = (
                        "Invalid length of elements passed - argument .u(). "
                        "If you are trying to update an entire column "
                        "set update_all=True + table__column=data."
                    )
                    logger.critical(msg)
                    raise ValueError(v)

                # Validar sintaxis apropiada:
                if "__" not in k:
                    msg = (
                        "Invalid sintax form **kwargs in method .u(). "
                        "You must separate sentence using: '__'."
                    )
                    logger.critical(msg)
                    raise ValueError(k)

                line = k.split("__")

                if len(line) == 4:

                    tab = line[0]
                    col = line[1]
                    con = line[2]
                    op = line[3]

                    dat = v[0]
                    val = v[1]

                    if op not in OPERATOR.keys():
                        msg = "Invalid operator passed - method .u()."
                        logger.critical(msg)
                        raise ValueError(op)

                    dicc['table'] = tab
                    dicc['name'] = col
                    dicc['data'] = dat
                    dicc['condition'] = [
                        {
                            'column': con,
                            'operator': op,
                            'value': val,
                        }
                    ]
                    argument.append(dicc)

                else:
                    msg = (
                        "Invalid length passed - **kwargs in method .u(). "
                        "If you are trying to update an entire column "
                        "set update_all=True + table__column=data."
                    )
                    logger.critical(msg)
                    raise ValueError(k)

        else:

            for k, v in kwargs.items():
                dicc = {}

                # Validamos que la data no sea iterable
                if not isinstance(v, (str, int, float, bool)):
                    msg = (
                        "Invalid data datatype passed. "
                        "If update_all=True, data must be: "
                        "str, int, float, bool"
                    )
                    logger.critical(msg)
                    raise TypeError(type(v))

                # Valida sintaxis de **kwargs
                if "__" not in k:
                    msg = (
                        "Invalid sintax form **kwargs in method .u(). "
                        "You must separate sentence using: '__'."
                    )
                    logger.critical(msg)
                    raise ValueError(k)

                line = k.split("__")

                # Validar el extenso del **kwargs
                if len(line) != 2:
                    msg = (
                        "Invalid length passed - **kwargs in method .u(). "
                        "If you are trying to update by condition"
                        "set update_all=False + "
                        "table__column__column_con__operator=(data, data)."
                    )

                tab = line[0]
                col = line[1]

                dicc['table'] = tab
                dicc['name'] = col
                dicc['data'] = v

                argument.append(dicc)

        path = db_path if db_path else self.model._db_file

        update(db_path=path, params=argument, update_all=update_all)

        return

    def d():
        pass
