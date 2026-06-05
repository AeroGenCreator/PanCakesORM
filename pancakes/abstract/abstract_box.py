# ==============================================================================
# PROJECT:       PanCakesORM
# VERSION:       6.0.0
# AUTHOR:        AeroGenCreator
# GITHUB:        https://github.com/AeroGenCreator
# LICENSE:       Apache License 2.0
# ==============================================================================
# Copyright (c) 2026 AeroGenCreator. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
# DESCRIPTION:
# [Capa de abtraccion sobre los metodos INSERT, UPDATE, DELETE]
# ==============================================================================

# Modulos Python
import logging
from types import SimpleNamespace
from typing import Callable

from ..orm.delete import delete  # Funcion delete()
from ..orm.insert import insert  # Funcion insert()
from ..orm.query import query  # Para computar campos
from ..orm.update import update  # Funcion update()
from ..tools.functions import environment  # Variables de entorno
from ..valid.filter_validator import (
    DeleteFilterValidator,  # Val. Kwargs Del.
    UpdateFilterValidator,  # Val. Kwargs Udp.
)

# .envs; log, dir, db
envs = environment()
LOG = envs.get("log", "WARNING")
DEFAULT_DIR = envs.get("dir")
DEFAULT_DB_FILE = envs.get("db")

log_level = getattr(logging, LOG, logging.WARNING)
logging.basicConfig(
    level=log_level,
    format=(
        "%(asctime)s [%(levelname)s] "
        "%(name)s.%(funcName)s:%(lineno)d - %(message)s"
    ),
    force=True,
)
logger = logging.getLogger(__name__)

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
    "notlike": "NOT LIKE",
}


class AbstractBox:
    """
    AbstractBox genera una capa de abstraccion para las funciones:

    - insert()
    - update()
    - delete()

    Esto permite que las funciones anteriores puedan ser usadas
    de manera declarativa.

    Importante:
    Se puede usar como metodo directamente sobre el modelo:
    Inventory.i()

    Tambien puede ser instanciado:
    box = QueryBox(PanCakesORM)
    """

    def __init__(self, model):
        self.model = model

    def i(self, db_path=None, **kwargs) -> None:
        """
        Los argumentos deben ser:

        tabla = datos

        Los datos deben ser una lista de tuplas.
        Ejemplo:
        user = [(None, 'Mexico')]
        """

        MODEL = self.model
        path = MODEL._db_file

        # Pydantic Valida -> Inserts
        VALIDATED_KWARGS = {}

        for TAB, LISTS in kwargs.items():
            schema = MODEL._metadata[TAB]["schema"]
            SCH = self.model._metadata[TAB]["validators"]["CreateValidator"]
            COLS = self.model._metadata[TAB]["columns"]

            VALIDATED = []
            for TUPS in LISTS:
                dicc = dict(zip(COLS, TUPS))
                clean = {}
                for c, v in dicc.items():
                    # Sí compute; ejecutar y guardar valor
                    compute = schema[c]["metadata"]["compute"]
                    PK = schema[c]["metadata"].get("primary_key", False)
                    FK = schema[c]["metadata"].get("foreign_key", False)
                    MOCK = SimpleNamespace()

                    # No PK, FK, COMPUTE, sí None -> Skip
                    if not PK and not FK and not compute:
                        if v is None:
                            continue
                        # Lo anterior pero si hay valor
                        else:
                            clean.update({c: v})
                            continue

                    # NO COMPUTE, SI PK o FK con valor
                    if PK or FK and not compute:
                        if v is not None:
                            clean.update({c: v})
                            continue

                    # Si NO PK, NO FK, SI COMPUTE
                    if compute is not None:
                        for key, val in dicc.items():
                            setattr(MOCK, key, SimpleNamespace(value=val))

                    if isinstance(compute, str):
                        func = getattr(MODEL, compute)
                        value = func(MOCK)
                        clean.update({c: value})
                        continue

                    if isinstance(compute, Callable):
                        value = compute(MOCK)
                        clean.update({c: value})
                        continue

                data = SCH.model_validate(clean)
                val_dicc = data.model_dump()
                res = tuple([e for i, e in val_dicc.items()])
                VALIDATED.append(res)

            VALIDATED_KWARGS[TAB] = VALIDATED

        argument = []

        for k, v in VALIDATED_KWARGS.items():
            # Validamos que los datos sean listas:
            if not isinstance(v, list):
                msg = "Invalid datatype passed to method .i()"
                logger.critical(msg)
                raise TypeError(type(v))

            argument.append({"table": k, "data": v})

        insert(db_path=path, params=argument)

        return

    def u(self, **kwargs):
        """
        1. La actualizacion de datos de una tabla se da a traves de un
        diccionario en la funcion update(). Para poder usar este metodo
        declarativo se convierten las llaves del diccionario en variables:

        2. La actualización permite una condición; la idea es condicionar
        por indices.

        user__name__user_id__same = (Raul, 10)
        tabla__col__ccol__operador = (data, valor_de_condicion)

        3. Declaracion (Modificar Columna Completa)
        tabla__columna = Valor

        EQUIVALENCIAS update():

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

        NOTA: Este metodo no permite actualizar toda una columna; Para poder
        computar campos en tiempo real.

        NOTA: Este helper no permite actulizar por multiples condiciones
        (AND, OR) esta pensado para ser usado por ids. Si se necesita una
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
        """
        # CONSTANTES
        WHERE = {
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
            "notlike": "NOT LIKE",
        }
        MODEL = self.model
        PATH = MODEL._db_file
        META = MODEL._metadata

        # VALIDAR (UPDATE SYNTAX)
        validated = UpdateFilterValidator.model_validate({"filters": kwargs})
        kwargs = validated.filters

        TABLES_TO_UPDATE = []
        # Obtener las tablas que seran actualizadas
        for KEY, LIST in kwargs.items():
            TAB = KEY.split("__")[0]
            if TAB not in TABLES_TO_UPDATE:
                TABLES_TO_UPDATE.append(TAB)

        # VALIDAR POR CAMPO
        valid_kwargs = {}
        for key, val in kwargs.items():
            PARTS = key.split("__")
            TAB = PARTS[0]
            COL = PARTS[1]

            ADAPTER = META[TAB]["validators"]["AdapterValidator"][COL]

            # Primer valor, nuevo dato por ingresar
            VAL = val[0]
            VALID = ADAPTER.validate_python(VAL)
            valid_kwargs.update({key: [VALID, val[1]]})

        # Creacion volatil linea de valores:
        VOLATIL = {}
        for TAB in TABLES_TO_UPDATE:
            VOLATIL[TAB] = {}
            COLUMNS = META[TAB]["columns"]
            DICT = dict(zip(COLUMNS, [None for c in COLUMNS]))
            for KEY, LIST in valid_kwargs.items():
                PARTS = KEY.split("__")
                KTAB = PARTS[0]
                KCOL = PARTS[1]
                KVAL = LIST[0]
                if KTAB == TAB:
                    DICT[KCOL] = KVAL
            VOLATIL[TAB] = DICT

        # CAMPOS COMPUTADOS (Refactorizado: Un solo query unificado)
        # Usamos list() -> De esta manera se puede editar dict original
        # CAMPOS COMPUTADOS Y CONSTRUCCIÓN DE PARÁMETROS
        params = []

        # Primero, añadimos los cambios estáticos originales a params
        for key, val in valid_kwargs.items():
            PARTS = key.split("__")
            params.append(
                {
                    "table": PARTS[0],
                    "name": PARTS[1],
                    "data": val[0],
                    "condition": [
                        {
                            "column": PARTS[2],
                            "operator": WHERE[PARTS[3]],
                            "value": val[1],
                            "logic": "",
                        }
                    ],
                }
            )

        # Ahora procesamos los calculados fila por fila
        for key, val in list(valid_kwargs.items()):
            PARTS = key.split("__")
            TAB, CUR, CCL, OPE = PARTS[0], PARTS[1], PARTS[2], PARTS[3]
            DAT, CON = val[0], val[1]

            COLUMNS = META[TAB]["columns"]
            SQL = [{"table": TAB, "name": col} for col in COLUMNS]

            rows, columns = query(
                select=SQL,
                _from=TAB,
                db_path=PATH,
                condition=[
                    {
                        "table": TAB,
                        "column": CCL,
                        "operator": WHERE[OPE],
                        "value": CON,
                    }
                ],
            )

            for row in rows:
                MOCK = SimpleNamespace()
                DICC = dict(zip(columns, row))

                # Encontrar la PK esta fila específica para la condición final
                # Estandar {TAB}_id
                pk_column = f"{TAB}_id"
                pk_field = f"{TAB}__{pk_column}"
                current_pk_value = DICC.get(pk_field)

                # Construir el objeto Mock con datos fusionados (Real + Volátil)
                for cl, cv in DICC.items():
                    QCOL = cl.split("__")[1]
                    DATA = VOLATIL[TAB].get(QCOL)
                    VALUE = DATA if DATA is not None else cv
                    setattr(MOCK, QCOL, SimpleNamespace(value=VALUE))

                # Evaluar qué campos necesitan ser computados
                for cl in DICC.keys():
                    QCOL = cl.split("__")[1]
                    compute = META[TAB]["schema"][QCOL]["metadata"]["compute"]

                    if compute is not None:
                        if isinstance(compute, Callable):
                            computed_val = compute(MOCK)
                        elif isinstance(compute, str):
                            func = getattr(MODEL._family[TAB], compute)
                            computed_val = func(MOCK)
                        else:
                            continue

                        # Inyeccion directa a params apuntando ID único de fila
                        params.append(
                            {
                                "table": TAB,
                                "name": QCOL,
                                "data": computed_val,
                                "condition": [
                                    {
                                        "column": pk_column,
                                        "operator": "=",
                                        "value": current_pk_value,
                                        "logic": "",
                                    }
                                ],
                            }
                        )

        # ACTUALIZACIÓN
        update(db_path=PATH, params=params, update_all=False)
        return

    def d(self, db_path=None, **kwargs) -> None:
        """
        1 . La eliminación de datos en una tabla se da a traves de un
        diccionario pasado a la funcion delete(). Para poder usar este metodo
        declarativo .d() se convierten las llaves del diccionario en variables:

        2. Se admite una condicion; la idea es condicionar por indices.

        3. Este metodo no permite delete sobre SQLconstraints. Por tanto
        su seguridad de persistencia de datos esta al maximo; Si existen
        problemas con la eliminacion de datos:

            - Revisar que tus tablas con llaves foraneas tengan
            on_del="cascade" o usar función .delete(force=True)
            para ignorar las restricciones.

        4. Este metodo no borra todas las filas de una tabla. Si se quiere
        limpieza completa usar metodo .delete(delete_all=True)

        user__user_id__same = Raul
        user__user_id__same = valor_de_condicion

        Equivale a:

        params = [
            {
            'table':'user',     <- Tabla
            'condition':[       <- Lista de condiciones
                    {
                    'column',   <- Columna de condicion
                    'operator', <- Operador de comparacion
                    'value',    <- Valor de comparacion
                    }
                ]
            }
        ]

        NOTA: Este helper no permite eliminar por multiples condiciones
        (AND, OR) esta pensado para ser usado por ids. Si se necesita una
        eliminación por multiples condiciones usar metodo .delete()

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
        """

        # Hay modelo, no ruta
        if self.model and db_path is None:
            path = self.model._db_file
        # No hay modelo, no ruta
        if self.model is None and db_path is None:
            path = DEFAULT_DB_FILE
        # Hay ruta
        if db_path:
            path = db_path

        # Validar Pydantic & Parsing
        validated = DeleteFilterValidator.model_validate({"filters": kwargs})
        kwargs = validated.filters

        argument = []

        for k, v in kwargs.items():
            dicc = {}

            # Validar separador
            if "__" not in k:
                msg = (
                    "Invalid syntax - argument. "
                    "You must specify table__column__operator."
                )
                logger.critical(msg)
                raise ValueError(k)

            line = k.split("__")

            # Validar num. de argumentos
            if len(line) != 3:
                msg = (
                    "Invalid length - argument. "
                    "You must specify 3 elements: "
                    "table__column__operator"
                )
                logger.critical(msg)
                raise ValueError(k)

            tab = line[0]
            col = line[1]
            opr = line[2]
            dat = v

            # Validar operador
            if opr not in OPERATOR.keys():
                msg = f"Invalid operator {opr}. Valid ones are: {OPERATOR}."
                logger.critical(msg)
                raise KeyError(opr)

            dicc["table"] = tab
            dicc["condition"] = [
                {"column": col, "operator": OPERATOR[opr], "value": dat}
            ]
            argument.append(dicc)

        delete(db_path=path, params=argument, delete_all=False, force=False)

        return
