# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
"""
Clase Padre Abstracta:
Define La Logica de Tabla Que Sera Heredada Por Las Clases Hijas (Tablas)
"""

# Modulos Originales PanCakesORM
from ..datatype import sql_datatype
from ..tool.function import db_connection, logger
from ..tool.box import QueryBox
from ..tool.idu import CoffeeShop
from ..cook.layer import query
from ..cook.ingredient import update
from ..cook.clean import delete
from ..cook.furnace import insert

# Modulos de Python
import warnings
from pathlib import Path

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

    2. _backup:
    Acumula en un diccionario {tabla: Clase} permitiendo el accesos
    a la metadata (comunicacion) de todas las tablas hermanas creadas
    con PanCakesORM.

    3. _init_database:
    Valida ruta a la base de datos. Si no existe la genera por defecto.

    4. _get_fields:
    Valida los "objetos" de tipo "sql_datatype". Se genern los
    nombre de columna. Se asignan a su propio objeto. Se guardan los
    objetos como una lista en el atributo de clase: "_fields".

    5. _init_table:
    Genera la tabla en la base de datos.
    Los nombres de columna son sanitizados a traves de "[]".

    SIEMPRE AL CARGAR:

    6. Revisar el esto del loop.
    Si Es la primera carga del fichero '.py', se actualiza el esquema
    en la base de datos, de lo contrario. Se salta la sincronizacion
    del esquema.
    """

    # -> _db_dir: Ruta directorio para la base de datos
    # -> _db_file: Ruta fichero para la base de datos
    # -> _family: Comunicacion entre clases hermanas
    # -> _loop_validation: Bandera: primera ejecucion = False,
    # sincroniza esquemas. Despues = True.

    _db_dir = DEFAULT_DIR
    _db_file = DEFAULT_DB_FILE
    _family = {}
    _loop_validation = False

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._clean_table_name()
        cls._backup()
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
        if '_table' not in cls.__dict__.keys():
            msg = "Key '_table' was not defined."
            logger.critical(msg)
            raise KeyError

        table = cls.__dict__.get('_table')

        if not isinstance(table, str):
            msg = 'Variable "_table" Must Be A String'
            logger.critical(msg)
            raise TypeError(msg)

        clean = [c for c in table if c.isalnum() or c == '_']
        c_table = "".join(clean)

        if not c_table:
            msg = f'Invalid characters passed: {table}.'
            logger.critical(msg)
            raise ValueError(msg)

        logger.info(f"New table named as {c_table}")
        cls._table = c_table

    @classmethod
    def _backup(cls):
        """
        Almacena cada clase en un diccionario,
        de esta manera cualquier clase hija que herede de
        PanCakesORM puede acceder a cualquier metadata de las
        clases hermanas a traves de su llave.
        """
        cls._family[cls._table] = cls

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
        cls.comment = [f"{cls._table} Id".capitalize()]
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
                cls.comment.append(value.comment)

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
    def _table_on_change(
        cls,
        marks=None,
        new_data=None,
        less_columns=None
    ):
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
                f"ALTER TABLE [{cls._table}] "
                f"RENAME TO [{cls._table + '_old'}];"
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS [{cls._table}]("
                f"[{cls._table}_id] INTEGER PRIMARY KEY, "
                f"{sql});"
            )
            cur.execute(
                f"INSERT INTO [{cls._table}]({less_columns}) "
                f"SELECT {less_columns} "
                f"FROM [{cls._table + '_old'}];"
            )
            if marks and new_data:
                cur.executemany(
                    f"INSERT INTO [{cls._table}] VALUES({marks})",
                    new_data
                )
        with db_connection(
            db_path=cls._db_file,
            no_foreign=True
        ) as (conn, cur):
            cur.execute(f"DROP TABLE [{cls._table + '_old'}];")

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
            cur.execute(f"SELECT * FROM [{cls._table}];")
            data = cur.fetchall()
            return data

    # --*-- Metodos: CRUD --*--
    @classmethod  # PARA DESARROLLADORES
    def sql(cls, command: str):
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

    @classmethod  # Inyeccion Segura
    def query(
        cls,
        db_path: str = None,
        select: str | list = None,
        _from: str = None,
        sp_select: list = None,
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
        _from = cls._table if _from is None else _from
        sp_select = None if sp_select is None else sp_select
        join = None if join is None else join
        condition = None if condition is None else condition
        group_by = None if group_by is None else group_by
        order_by = None if order_by is None else order_by
        limit = None if limit is None else limit

        res, col = query(
            db_path=db_path,
            select=select,
            _from=_from,
            sp_select=sp_select,
            join=join,
            condition=condition,
            group_by=group_by,
            order_by=order_by,
            limit=limit
        )

        return res, col

    @classmethod  # Inyeccion Segura
    def insert(
        cls,
        params: list,
        db_path: str = None
    ):
        """
        Invoca a la funcion insert()
        Documentacion de los parametros en /cook/furnace.py
        """
        cls._which_loop()
        db_path = db_path if db_path else cls._db_file

        insert(
            db_path=db_path,
            params=params
        )

    @classmethod  # Inyeccion Segura
    def update(
        cls,
        params: list,
        db_path: str = None,
        update_all: bool = None
    ):
        """
        Invoca a la funcion update()
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
        params: list,
        db_path: str = None,
        delete_all: bool = None,
        force: bool = None
    ):
        """
        Invoca a la funcion delete()
        Documentacion de los parametros en /cook/clean.py
        """
        cls._which_loop()
        db_path = db_path if db_path else cls._db_file
        delete_all = delete_all if delete_all else False
        force = force if force else False

        delete(
            db_path=db_path,
            params=params,
            delete_all=delete_all,
            force=force
        )

    # Queries declarativos:
    @classmethod  # Inyeccion Segura
    def q(cls):
        return QueryBox(model=cls)

    @classmethod  # Inyeccion Segura
    def select(cls, *columns):
        return cls.q().select(*columns)

    @classmethod  # Inyeccion Segura
    def id(cls):
        return cls.q().id()

    @classmethod  # Inyeccion Segura
    def add(cls, **kwargs):
        return cls.q().add(**kwargs)

    @classmethod  # Inyeccion Segura
    def link(cls, *relation):
        return cls.q().link(*relation)

    @classmethod  # Inyeccion Segura
    def filter(cls, **kwargs):
        return cls.q().filter(**kwargs)

    @classmethod  # Inyeccion Segura
    def gp(cls, **kwargs):
        return cls.q().gp(**kwargs)

    @classmethod  # Inyeccion Segura
    def sort(cls, **kwargs):
        return cls.q().sort(**kwargs)

    @classmethod  # Iyeccion Segura
    def lim(cls, num: int = None):
        return cls.q().lim(num)

    @classmethod  # Inyeccion Segura
    def all(cls):
        return cls.q().all()

    @classmethod  # Inyeccion Segura
    def count(cls):
        return cls.q().count()

    # Insert declarativo
    @classmethod
    def manager(cls):
        return CoffeeShop(model=cls)

    @classmethod  # Inyeccion Segura
    def i(cls, **kwargs):
        return cls.manager().i(**kwargs)

    # Update declarativo
    @classmethod  # Inyeccion Segura
    def u(cls, update_all=False, **kwargs):
        return cls.manager().u(update_all=update_all, **kwargs,)