# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
"""
Clase Padre Abstracta:
Define La Logica de Tabla Que Sera Heredada Por Las Clases Hijas (Tablas)
"""

# Modulos Originales
from ..datatype import sql_datatype
from ..tool.function import db_connection
from ..cook.flavor import pancakes
from ..cook.ingredient import update

# Modulos de Python
import warnings
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.WARNING,  # Captura todo desde WARNING hacia arriba
    format='%(asctime)s [%(levelname)s] '
    '%(name)s.%(funcName)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)

# Ruta Por Defecto Para Cualquier Proyecto:
# data/mi_app_database.sqlite
DEFAULT_DIR = Path.cwd() / 'data'
DEFAULT_DB_FILE = DEFAULT_DIR / 'my_app_database.sqlite'


class PanCakesORM:
    """
    PanCakesORM:
    Clase dependiente de herencia. Opera despues de ser heredada en
    una clase hija.

    EN EL CONSTRUCTOR:

    1. _clean_table_name:
    Valida que la el atributo '_table' exista (dato: str).
    Sanitiza el string.

    2. _init_database:
    Valida ruta a la base de datos. Si no existe la genera por defecto.

    3. _get_fields:
    Valida los "objetos" de tipo "sql_datatype". Se genern los
    nombre de columna. Se asignan a su propio objeto. Se guardan los
    objetos como una lista en el atributo de clase: "_fields".

    4. _init_table:
    Genera la tabla en la base de datos.
    Los nombres de columna son sanitizados a traves de "[]".

    SIEMPRE AL CARGAR:

    5. Revisar el esto del loop.
    Si Es la primera carga del fichero '.py', se actualiza el esquema
    en la base de datos, de lo contrario. Se salta la sincronizacion
    del esquema.
    """

    # -> _db_dir
    # -> _db_file
    _db_dir = DEFAULT_DIR
    _db_file = DEFAULT_DB_FILE
    _loop_validation = False

    def __init_subclass__(cls, **kwargs):
        cls._clean_table_name()
        cls._init_database()
        cls._get_fields()
        cls._init_table()
        cls._which_loop()

    @classmethod  # Inyeccion Segura | Test Seguro
    def _clean_table_name(cls):
        """
        Esta Funcion Valida El Nombre De La Tabla:
        Evita Inyeccion De Datos Al Crear La Tabla.
        """
        if cls.__dict__.get('_table'):
            table = cls.__dict__.get('_table')
            if isinstance(table, str):
                cleaned = []
                for char in table:
                    if char.isalnum() or char == '_':
                        cleaned.append(char)
                cleaned = "".join(cleaned)
                if not cleaned:
                    msg = f'Variable "_table" Has Not Valid Characters.'
                    logger.critical(msg)
                    raise ValueError(msg)
                cls._table = cleaned
            else:
                msg = 'Variable "_table" Must Be A String'
                logger.critical(msg)
                raise TypeError(msg)
        else:
            msg = (
                "There Is No '_table' Variable Defined. "
                "Use It To Name Your Table"
            )
            logger.critical(msg)
            raise Exception(msg)

    @classmethod  # Test seguro
    def _init_database(cls):
        """
        Se Asegura La Ruta Por Defecto Para La Base De Datos
        Para Cualquiero Proyecto.
        Si El Directorio Y El Fichero No Existen Se Crean.
        """
        cls._db_dir.mkdir(exist_ok=True, parents=True)
        cls._db_file.touch(exist_ok=True)
        msg = (f'Structure of directories evaluated at {cls._db_file}.')
        logger.debug(msg)

    @classmethod  # Inyeccion Segura | Test seguro
    def _get_fields(cls):
        """
        Se extraen los "obtejos" columnas.
        Se sanitizan los "nombres" y se guardan como atributo.
        Cada "objeto" columna se guarda en "_fields"
        Cada objeto en la lista fields ahora cuenta con
        el atributo "_name". Para acceder a los nombres
        se debe iterar la lista y llamara al tributo "_name"
        """
        cls._fields = []
        data = cls.__dict__

        column_type = (
            sql_datatype.Char,
            sql_datatype.Int,
            sql_datatype.Float,
            sql_datatype.ForeignKey,
            sql_datatype.Text,
            sql_datatype.Bool
            )

        for key, value in data.items():
            if isinstance(value, column_type):
                clean_name = []
                for char in key:
                    if char.isalnum() or char == '_':
                        clean_name.append(char)
                clean_name = "".join(clean_name)
                if not clean_name:
                    raise ValueError(
                        f"""The Column Name "{key}" In
                        {cls.__name__} Is Not Valid."""
                    )
                value._name = clean_name
                cls._fields.append(value)

    @classmethod  # Inyeccion Segura
    def _init_table(cls):
        """
        Inicializa la tabla de cualquier clase hija declarada.
        Los nombre de columnas son sanitizados usando "[]".
        """
        extraction = []
        for data in cls._fields:
            extraction.append(["[" + data._name + "]", data._dtype])
        sql = ", ".join([" ".join(field) for field in extraction])
        with db_connection(db_path=cls._db_file) as (conn, cur):
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS [{cls._table}](
                [{cls._table}_id] INTEGER PRIMARY KEY,
                {sql});
            """)

    @classmethod  # Inyeccion Segura
    def _columns_table_validation(cls):
        """
        Se obtienen los nombres de columnas en tiempo real desde
        la clase y los nombres de columna en la base de datos.
        Se regresan ambos como (new_columns)(old_columns)
        """
        cls._get_fields()
        cls._clean_table_name()
        # Obtengo Las Columnas En La Clase Y Las Que Existen En Tiempo Real
        with db_connection(db_path=cls._db_file) as (conn, cur):
            cur.execute(f"""SELECT * FROM [{cls._table}] LIMIT 1;""")
            old_columns = ['[' + col[0] + ']' for col in cur.description]
            new_columns = ['[' + col._name + ']' for col in cls._fields]
            new_columns.insert(0, f'[{cls._table}_id]')
        return new_columns, old_columns

    @classmethod  # Inyeccion Segura
    def _extraction(cls):
        """
        Este Metodo Se Llama Despues De La Limpieza de
        _columns_table_validation()
        Devuelve el string SQL de columnas extraido desde las
        columnas en tiempo real desde la clase.
        * La intencion es usarlo como el string de declaracion de:
        "Creacion de tabla". (nombre)(tipo de dato)
        """
        extraction = []
        # Se Extraen Las Nuevas Columnas Como Objetos Completos
        for data in cls._fields:
            if isinstance(data, sql_datatype.ForeignKey):
                extraction.append(["[" + data._name + "]", data._dtype])
                if data._name != data.column_id:
                    warnings.warn(
                        f"""
                        Warning: Local Column Name '{data._name}'
                        Does Not Match External Column Name {data.column_id}
                        For Second Table: '{data.second_table}'.""",
                        UserWarning
                    )
            else:
                extraction.append(["[" + data._name + "]", data._dtype])
        union = [" ".join(data) for data in extraction]
        sql = ", ".join(union)
        # Las Cadenas De Texto que Devuelve Estan Limpias De Inyeccion
        # Maliciosa
        return sql

    @classmethod  # Inyeccion Segura
    def _table_on_change(cls, marks=None, new_data=None, less_columns=None):
        """
        Este metodo edita el esquema basado en la declaracion de clase
        en tiempo real. (renombra  -> crea -> copia data -> elimina).
        Si se pasan (new_data y marks) se inserta data nueva tambien.
        """
        sql = cls._extraction()
        less_columns = ", ".join(less_columns)
        # Inyeccion Segura
        with db_connection(db_path=cls._db_file) as (conn, cur):
            cur.execute(
                f"""ALTER TABLE [{cls._table}]
                RENAME TO [{cls._table + '_old'}];"""
            )
            cur.execute(
                f"""CREATE TABLE IF NOT EXISTS [{cls._table}](
                [{cls._table}_id] INTEGER PRIMARY KEY,
                {sql});"""
            )
            cur.execute(
                f"""INSERT INTO [{cls._table}]({less_columns})
                SELECT {less_columns}
                FROM [{cls._table + '_old'}];"""
            )
            if marks and new_data:
                cur.executemany(
                    f"""INSERT INTO [{cls._table}] VALUES({marks})""",
                    new_data
                )
        with db_connection(
            db_path=cls._db_file,
            no_foreign=True
        ) as (conn, cur):
            cur.execute(f'DROP TABLE [{cls._table + '_old'}];')

    @classmethod
    def _synchronize_table(cls):
        """
        Evalua el esquema en tiempo real y aquel que esta en la base de
        datos.
        La evaluacion depende de cambios en la cantidad de columnas.
        """
        new_columns, old_columns = cls._columns_table_validation()
        diference = len(new_columns) - len(old_columns)
        if diference > 0:
            cls._table_on_change(less_columns=old_columns)
        elif diference < 0:
            cls._table_on_change(less_columns=new_columns)
        return new_columns, old_columns

    @classmethod
    def _which_loop(cls):
        """
        Valida que la sincronizacion de esquema se ejecute unicamente
        en la primera ejecucion de cualquier aplicacion que haga
        herencia de la clase PanCakesORM.

        Funcion: Optimiza PanCakesORM haciendo que la sincrinizacion de
        esquema suceda unicamente en la primera compilacion.
        """
        if not cls._loop_validation:
            logger.debug(
                f"Starting first-time schema "
                f"synchronization for {cls.__name__}."
            )
            cls._synchronize_table()
            cls._loop_validation = True
            msg = ('Current schema synchronized.')
            logger.info(msg)
        else:
            logger.debug(
                f"Skipping synchronization: "
                f"{cls.__name__} already validated."
            )

    @classmethod  # Inyeccion Segura
    def table_exists(cls):
        """
        Valida la existencia de una tabla cuando es creada a traves
        de PanCakesORM.

        Devuelve True si la tabla existe en la base de datos.
        """
        cls._clean_table_name()
        with db_connection(db_path=cls._db_file) as (conn, cur):
            res = cur.execute(
                f"""SELECT name
                FROM sqlite_master
                WHERE name = ?;""",
                (cls._table,)
            )
            if res.fetchone() is not None:
                return True
            else:
                return False

    @classmethod  # Inyeccion Segura
    def return_all(cls):
        """
        Devuelve el query (SELECT * FROM <cls.table>)
        Permite revisar el contyenido de una tabla rapidamente,
        evitando usar el metodo "pancakes" el cual requiere de
        argumento especificos para funcionar.
        """
        cls._which_loop()
        with db_connection(db_path=cls._db_file) as (conn, cur):
            cur.execute(f'SELECT * FROM [{cls._table}];')
            data = cur.fetchall()
            return data

    # --*-- Metodos: CRUD --*--

    @classmethod
    def pancakes(
        cls,
        db_path: str = None,
        select: str | list = None,
        from_: str = None,
        special_select: list = None,
        join: list = None,
        condition: list = None,
        group_by: list = None,
        order_by: list = None,
        limit: int = None
    ):
        """
        Llama a la funcion de consulta global.
        Documentacion: cook/flavor
        """
        cls._which_loop()
        db_path = cls._db_file if db_path is None else db_path
        select = "*" if select is None else select
        from_ = cls._table if from_ is None else from_
        special_select = None if special_select is None else special_select
        join = None if join is None else join
        condition = None if condition is None else condition
        group_by = None if group_by is None else group_by
        order_by = None if order_by is None else order_by
        limit = None if limit is None else limit

        result = pancakes(
            db_path=db_path,
            select=select,
            from_=from_,
            special_select=special_select,
            join=join,
            condition=condition,
            group_by=group_by,
            order_by=order_by,
            limit=limit
        )
        return result

    @classmethod  # PARA DESARROLLADORES
    def query(cls, command: str):
        """
        Metodo Avanzado de Consulta.
        Pensado Para Desarrolladores.
        Evitar Que Un input de usuario termine en
        un query como este.
        """
        cls._which_loop()
        try:
            with db_connection(db_path=cls._db_file) as (conn, cur):
                cur.execute(command)
                columns = [col[0] for col in cur.description]
                result = cur.fetchall()
                return result, columns
        except Exception as e:
            print(e)

    @classmethod
    def write(cls, new_data: list):
        """
        Metodo "escritura".
        Este metodo permite insertar data de manera segura.

        Parametros: Lista de tuplas, siguiendo el orden de columnas de
        la tabla.

        new_data = [(indice, valor_1, valor2, ...)]

        IMPORTANTE:
        El indice puede ser None, esto lo asigna SQLite3 por su cuenta.
        """
        marks = None
        # Obtencion De Las Columnas En Tiempo Real Al Ejecutar Este Metodo
        new_columns, old_columns = cls._columns_table_validation()
        dtypes_list = [obj._dtype for obj in cls._fields]
        # Se suma en uno el largo del calculo para las ? por el hecho de
        # La columna id, la cual no se pasa en la inyeccion de datos.
        marks = ", ".join(['?'] * (len(cls._fields) + 1))
        # Diferencia Tiempo Real Columnas:
        diference = len(new_columns) - len(old_columns)
        if cls.table_exists():
            if diference > 0:
                cls._table_on_change(
                    marks=marks,
                    new_data=new_data,
                    less_columns=old_columns
                )
            elif diference < 0:
                cls._table_on_change(
                    marks=marks,
                    new_data=new_data,
                    less_columns=new_columns
                )
            else:  # Inyeccion segura
                with db_connection(db_path=cls._db_file) as (conn, cur):
                    cur.executemany(
                        f"""
                        INSERT INTO [{cls._table}]
                        VALUES({marks});""",
                        new_data
                    )
        else:
            print(f'Table {cls._table} Does Not Exist')

    @classmethod
    def update(
        cls,
        params: list,
        db_path: str = None,
        update_all: bool = None
    ):
        """
        Invoka a la funcion update()
        Documentacion de los parametros en /cook/ingredient.py
        """
        cls._which_loop()
        db_path = cls._db_file if db_path is None else db_path
        update_all = False if update_all is None else update_all

        update(
            db_path=db_path,
            params=params,
            update_all=update_all
        )

    @classmethod  # Inyeccion Segura
    def delete(
        cls,
        parameters: list,
        delete_all: bool = None,
        force: bool = None
    ):
        """
        Elimina Filas De Una Tabla;

        Parametros:

        parameters -> Lista De Diccionarios, especifica los parametros
        de borrado.
        Lo siguiente:
        parameters = [{'on':'name', 'key':'andres'}...]
        Equivale:
        DELETE FROM <table> WHERE name = andres;

        delete_all -> Simplemente Limpia Toda La Tabla:
        Lo siguiente con delete_all = True
        [{'table':'test'}]
        Equivale:
        DELETE FROM test;
        """
        force = False if not force else force
        cls._which_loop()
        if not isinstance(parameters, (list, tuple)):
            print(f"""
                Invalid data in {parameters}.
                Parameters must be a list or tuple of dictionaries.
            """)
            return
        if delete_all:  # Inyeccion Segura
            sentence = f'DELETE FROM [{cls._table}];'
            with db_connection(
                db_path=cls._db_file,
                no_foreign=force
            ) as (conn, cur):
                cur.execute(sentence)

        else:  # Inyeccion Segura
            with db_connection(
                db_path=cls._db_file,
                no_foreign=force
            ) as (conn, cur):
                errors = []
                valid_columns = []
                for data in cls._fields:
                    valid_columns.append("[" + data._name + "]")
                valid_columns = set(valid_columns)
                for dic in parameters:
                    if "[" + dic['on'] + "]" in valid_columns:
                        column = dic.get('on')
                        key = dic.get('key')
                        sentence = f"""
                        DELETE FROM [{cls._table}]
                        WHERE [{column}] = ?;"""
                        cur.execute(sentence, (key,))
                    else:
                        errors.append(dic.get('on'))
                if errors:
                    print(
                        f"""Following columns do not exist {errors}.
                        The rest of columns were altered"""
                    )
