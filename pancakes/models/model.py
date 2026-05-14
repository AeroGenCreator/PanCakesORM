# Copyright 2026 AeroGenCreator
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

"""
'Modelo Padre' -> PanCakesORM:

Define herencia de 'modelo' de todas las clases hijas.

Contenido:

1. Carga de variables de entorno. / definir rutas defecto.
2. Carga de logger.
3. Clase PanCakesORM.
4. Inyeccion Modelo PanCakesORM -> Modelos Validacion Pydantic.
"""

# Modulos de Python
import logging
from typing import Annotated, Optional

from fastapi import APIRouter

# Modulos Terceros
from pydantic import Field, TypeAdapter, create_model

from ..abstract.abstract_box import AbstractBox
from ..abstract.query_box import QueryBox
from ..orm.delete import delete
from ..orm.insert import insert
from ..orm.query import query
from ..orm.update import update

# Modulos Originales PanCakesORM
from ..sql import datatype
from ..tools.functions import db_connection, environment
from ..valid.filter_validator import (
    DeleteFilterValidator,
    UpdateFilterValidator,
)
from ..valid.query_validator import (
    ValidateAdd,
    ValidateFilter,
    ValidateGroupBy,
    ValidateLimitOffset,
    ValidateLink,
    ValidateOrderBy,
    ValidateSelect,
)

# Variables Entorno
envs = environment()
LOG = envs.get("log", "WARNING")
DEFAULT_DIR = envs.get("dir")
DEFAULT_DB_FILE = envs.get("db")

# Configurar Logger
log_level = getattr(logging, LOG, logging.WARNING)
logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] "
    "%(name)s.%(funcName)s:%(lineno)d - %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)


class PanCakesORM:
    """
    SIEMPRE AL LLAMAR AL CONSTRUCTOR

    PanCakesORM:
    Clase dependiente de herencia -> Opera despues de ser heredada.

    EN EL CONSTRUCTOR:

    1. _clean_table_name_:
    Valida el atributo '_table' -> (dato: str).
    Sanitiza el string.

    2. _backup_:
    Acumula en un diccionario {tabla: Clase} permitiendo el accesos
    a la metadata (comunicacion) de todas las tablas hermanas PanCakesORM.

    3. _init_database_:
    Valida rutas -> Directorio | Fichero (Base de Datos del Proyecto).

    4. _fetch_dependency_:
    Se valida el atributo '_depends'. Por defecto ["self"].
    En caso contrario se deben especificar como una Lista | Tupla de Strings.
    Se almacena en -> cls._metadata

    5. _fetch_group_constraints_:
    Se valida restriccion UNIQUE(col1, col2, ...) nivel SQL.
    Se almacene -> cls._metadata

    6. _get_fields_:
    Valida los "objetos" de tipo "datatype".
    Se genern nombre de columna.
    Se asignan a su propio objeto.
    Se guardan objetos como una lista en el atributo de clase: "_fields".
    Se guardan los comments de frontend en cls.comment.
    Tanto 'objeto columna', 'nombres columna', 'etiquetas frontend' son
    almacenados en -> cls._metadata

    7. _sort_dependency_:
    Mediante Algoritmo: Genera lista ordenada de nombres de tablas.
    Se almacena en cls._order.
    Si no se encuentra relacion se lanza un INFO, se pasa a la siguiente
    tabla y se intenta nuevamente.

    8. _init_table_:
    Se generan tablas segun su orden en cls._order
    Cuando una tabla ya existe se:
    renombre -> se crea nueva -> se insertan datos -> se borra antigua.
    Este loop mantiene el codigo declarado en los .py sincronizado con
    lo que existe en la base de datos.

    9. _pydantic_validators_:
    Se generan los esquemas de validacion Pydantic para INSERT y UPDATE.

    10. _expose_validators_:
    Asigna los modelos de validacion INSERT & UPDATE asi como la metadata
    de validacion en los atributos de instancia 'CREATE',  'ADAPTER' y
    'ANNOTATED'.

    11. _routers_:
    Genera rutas FastAPI (GET, POST, PUT, DELETE) (QUERY & P-QUERY).

    # ATRIBUTOS

    -> _table: (Atributo Instancia) Nombre de la tabla.
    -> _family: (Atributo Clase) Comunicacion entre clases hermanas.
    -> _metadata: (Atributo Clase) Diccionario de diccionarios;
    Primer nivel -> {tabla: metadata}
    LLaves Glables = [
      'depends',
      'group_constraint',
      'fields',
      'comments',
      'columns',
      'validators']
    -> _db_dir: (Atributo Clase) Ruta directorio para la base de datos.
    -> _db_file: (Atributo Clase) Ruta fichero para la base de datos.
    -> _depends: (Atributo Instancia) Declarar dependencia de tablas.
    -> _group_constraint: (Atributo Instancia) Linea UNIQUE(col1, col2, ...)
    -> _fields: (Atributo Instancia) Lista de campos [obj1, obj2, ...]
    -> _comment: (Atributo Instancia) Lista de etiquetas [lab1, lab2, ...]
    -> _json: (Atributo Instancia) Diccionario {'campo': {Pydantic Schema}}
    -> _order: (Atributo Clase) Guarda el orden de creación de tablas segun.
    -> _depends: (Atributo Instancia) Declarar si la tabla depende
    de otra u otras.
    -> CREATE = (Atributo Instancia) Esquema Pydantic 'INSERT'.
    -> ADAPTER = (Atributo Instancia) Esquema Pydantic 'UPDATE'.
    -> ANNOTATED = (Atributo Instancia) Esquemas 'Tabla | Col' Metadata.
    -> ROUTERS: (Atributo Clase) Almacena los endpoints de TODOS los modelos
    por routers.
    -> MODEL_COUNT_ROUTERS: (Atributo Clase) [] -> Compara;
    Modelo tiene 'routers'?
    """

    _family = {}
    _metadata = {}
    _db_dir = DEFAULT_DIR
    _db_file = DEFAULT_DB_FILE
    _order = []
    ROUTERS = []
    MODEL_COUNT_ROUTERS = []

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._clean_table_name_()
        cls._backup_()
        cls._init_database_()
        cls._fetch_dependency_()
        cls._fetch_group_constraints_()
        cls._get_fields_()
        cls._sort_dependency_()
        cls._init_table_()
        cls._pydantic_validators_()
        cls._expose_validators_()
        cls._routers_()

    @classmethod  # Inyeccion Segura | Test Seguro
    def _clean_table_name_(cls):
        """
        Esta Funcion Valida El Nombre De La Tabla:
        Evita Inyeccion De Datos Al Crear La Tabla.
        """
        if "_table" not in cls.__dict__.keys():
            msg = "Key '_table' was not defined."
            logger.critical(msg)
            raise KeyError

        table = cls.__dict__.get("_table")

        if not isinstance(table, str):
            msg = 'Variable "_table" Must Be A String'
            logger.critical(msg)
            raise TypeError(msg)

        clean = [c for c in table if c.isalnum() or c == "_"]
        c_table = "".join(clean)

        if not c_table:
            msg = f"Invalid characters passed: {table}."
            logger.critical(msg)
            raise ValueError(msg)

        logger.info(f"New table named as {c_table}")
        cls._table = c_table

    @classmethod
    def _backup_(cls):
        """
        Almacena cada clase en un diccionario,
        de esta manera cualquier clase hija que herede de
        PanCakesORM puede acceder a cualquier metadata de las
        clases hermanas a traves de su llave.
        """
        cls._family[cls._table] = cls

    @classmethod  # Test seguro
    def _init_database_(cls):
        """
        Se Asegura La Ruta Por Defecto Para La Base De Datos
        Para Cualquiero Proyecto.
        Si El Directorio Y El Fichero No Existen Se Crean.
        """
        try:
            cls._db_dir.mkdir(exist_ok=True, parents=True)
            cls._db_file.touch(exist_ok=True)
            msg = f"Structure of directories evaluated at {cls._db_file}."
            logger.debug(msg)
        except FileNotFoundError:
            logger.critical(f"Invalid paths ({cls._db_dir}) ({cls._db_file})")
            raise ValueError

    @classmethod
    def _fetch_dependency_(cls) -> None:
        """
        Evalua el atributo de clase cls._depends:

        1. Valida el tipo de dato
        2. Valida: String o Iterable.
        3. Se guardan las dependencias en una lista de diccionarios.
        4. Cada diccionario es {tabla: {dependencias}}

        """

        externals = []

        if "_depends" not in cls.__dict__.keys():
            msg = (
                f"Model {cls._table} does not have declared attribute "
                f"'_depends' which must be specified for relations "
                f"among tables in database. PanCakesORM set _'depends' "
                f"to 'self'; table - {cls._table}"
            )
            logger.warning(msg)
            cls._depends = "self"

        # Validar tipo de dato
        if not isinstance(cls._depends, (str, list, tuple)):
            msg = (
                "Invalid datatype for attribute _depends. "
                "If table does not depend on others set it to 'self'. "
                "If depends on others, listem: [user, client...]. "
            )
            logger.critical(msg)
            raise TypeError(type(cls._depends))

        # Validar string "self"
        if isinstance(cls._depends, str) and cls._depends != "self":
            msg = (
                "Class attribute _depends only accepts the "
                "following string when there are no dependencies "
                "to other tables: 'self'. Any other string will "
                "raise this message."
            )
            logger.critical(msg)
            raise ValueError(cls._depends)

        # Si Dependencia == "self"
        if isinstance(cls._depends, str):
            externals.append(cls._depends)
            cls._metadata[cls._table] = {"depends": externals}

        # Si Dependencia == ["self"]
        elif (
            isinstance(cls._depends, (list, tuple)) and
            cls._depends == ["self"]
        ):
            externals.extend(cls._depends)
            cls._metadata[cls._table] = {"depends": externals}

        # Si Dependencias Complejas [tab1, tab2, etc]
        elif (
            isinstance(cls._depends, (list, tuple)) and
            "self" not in cls._depends
        ):
            externals.extend(cls._depends)
            cls._metadata[cls._table] = {"depends": externals}

        else:
            msg = (
                "Impossible to valid dependencies for '_depends'. "
                f"{cls._depends}. "
                "Valid dependencias are: "
                "No dependency; 'self', ['self']. "
                "Complex dependency; ['tab1', tab2, ...]."
            )
            logger.error(msg)
            raise ValueError

        return

    @classmethod
    def _fetch_group_constraints_(cls) -> None:
        """
        1. Se busca la existencia del atributo cls._group_constraint
        2. Se agrega a cls._metadata[cls.table]["group_constraint"]
        """

        # Validar un valor constraint en las llaves de la clase.
        if "_group_constraint" not in cls.__dict__.keys():
            cls._metadata[cls._table]["group_constraint"] = False
            return

        constraint = cls.__dict__.get("_group_constraint")
        if constraint:
            # Validar que sea una lista o tupla
            if not isinstance(constraint, (list, tuple)):
                msg = (
                    "Invalid datatype class attribute "
                    "'_group_constraint'. Valid datatype 'list', 'tuple'."
                )
                logger.critical(msg)
                raise TypeError(type(constraint))

            # Agregamos el constraint a su respectiva tabla: metadata
            cls._metadata[cls._table]["group_constraint"] = constraint
            return

    @classmethod  # Inyeccion Segura | Test seguro
    def _get_fields_(cls) -> None:
        """
        Se extraen los "obtejos" columnas.
        Se sanitizan los "nombres" y se guardan como atributo.
        Cada "objeto" columna se guarda en "_fields"
        Cada objeto en la lista fields ahora cuenta con
        el atributo "_name". Para acceder a los nombres
        se debe iterar la lista y llamara al tributo "_name"
        """

        # Atributos nuevos de clase; _fields, comment
        cls._fields = []

        # JSON para API
        cls._json = {cls._table: {}}

        # Etiqueta por defecto PK -> TABLA + ID
        id_comment = f"{cls._table} id".upper()
        cls.comment = [id_comment]

        # Metadata de PanCakesORM
        data = cls.__dict__

        # Tipo de datos validos
        column_type = (
            datatype.Char,
            datatype.Int,
            datatype.Float,
            datatype.ForeignKey,
            datatype.Text,
            datatype.Bool,
        )

        # Esquema; field_id 'modelo' | cache esquema 'modelo'
        schema_cache = {
            f"{cls._table}_id": {
                "type": int,
                "required": False,
                "default": None,
                "constraints": {},
                "metadata": {
                    "comment": id_comment,
                    "primary_key": True,
                    "read_only": True,
                },
            }
        }

        # Iteramos PanCakesORM
        for key, value in data.items():

            # Valida unica seleccion tipo de dato "columna sql"
            if isinstance(value, column_type):
                # Limpiamos los nombres de columnas
                clean_name = []
                for char in key:
                    if char.isalnum() or char == "_":
                        clean_name.append(char)
                clean_name = "".join(clean_name)
                if not clean_name:
                    raise ValueError(
                        f"""The Column Name "{key}" In
                        {cls.__name__} Is Not Valid."""
                    )
                # Agrega 'nombre', 'obj. columna', 'comentario frontend'
                value._name = clean_name
                cls._fields.append(value)
                cls.comment.append(value.comment)
                # Esquema; actualizado
                schema_cache.update({value._name: value._schema})

        # Campos Guardados Como Objeto
        cls._metadata[cls._table]["fields"] = cls._fields
        # Etiquetas Guardadas
        cls._metadata[cls._table]["comments"] = cls.comment
        # Nombres de campos guardados + campo PK
        columns = [f._name for f in cls._fields]
        columns.insert(0, f"{cls._table}_id")
        cls._metadata[cls._table]["columns"] = columns
        # Esquema Pydantic
        cls._json[cls._table]["schema"] = schema_cache

        return

    @classmethod
    def _sort_dependency_(cls):
        """
        Función vital para la creación de tablas referenciales.

        Se evalua y altera cls._order.

        Almacena el orden de creación de tablas para mantener
        coherenecia en las referencia.
        """

        # Copia que podemos modificar; Tablas respaldadas:
        llaves = list(cls._metadata.keys())

        while len(llaves) != 0:
            # Iteramos cada llave, freno; largo de llaves antes de 'for':
            freno = len(llaves)
            # Cache de llaves pendientes
            cache = []
            for k in llaves:
                # Guardo la dependecia en la variable dep
                dep = cls._metadata[k]["depends"]

                # Sin dependencias
                if dep == ["self"]:
                    if k in cls._order:
                        continue
                    cls._order.append(k)

                # Ya hemos encontrado su dependencia
                elif set(dep).issubset(cls._order):
                    if k in cls._order:
                        continue
                    cls._order.append(k)

                # En este loop: no referencia; se guarda para el sig.
                else:
                    cache.append(k)

            # Se actualiza la lista de diccs con los faltantes.
            llaves = cache

            # Detener si encontramos referencia circular o error de nombre.
            if freno == len(llaves):
                valids = list(cls._order)
                msg = (
                    "No relation found for this iteration. "
                    f"Found relations are: {valids} ... "
                    "Moving to next one..."
                )
                logger.info(msg)
                break

    @classmethod
    def _init_table_(cls):  # Inyeccion Segura
        """
        Esta función contruye los esquemas de tablas en tiempo real.
        Importante:

        1. Construye esquemas segun la dependencia de tablas.
        2. Mantiene los datos siempre y cuando no se borren columnas.
        3. Se ejecuta para todas las tablas, por tanto la primera
        ejecución puede tardar. Pero a cambio se obtiene un esquema bien
        definido y conectado.
        4. Esto parcha el error de tablas no creadas por no encontrar
        referencia de tabla 'padre'.
        """

        # Guarda: sentencias SQL y el orden de dependencias.
        lines = []
        order = list(cls._order)

        # Iteramos por orden
        for t in order:
            fields = dict(cls._metadata)[t]["fields"]
            constn = dict(cls._metadata)[t]["group_constraint"]
            unique = ""
            if constn:
                unique = ", UNIQUE" + f"({', '.join(constn)})"
            extraction = []
            for f in fields:
                extraction.append([f"[{f._name}]", f._dtype])
            union = ", ".join([" ".join(f) for f in extraction])
            line = (
                f"CREATE TABLE IF NOT EXISTS [{t}]("
                f"[{t}_id] INTEGER PRIMARY KEY, "
                f"{union} "
                f"{unique});"
            ).strip()
            lines.append(line)

        # Guarda; Tablas por eliminar
        tabs_to_drop = []

        # Conexion; FK activado.
        with db_connection(db_path=cls._db_file) as (conn, cur):
            for ln, t in zip(lines, order):
                column_in_model = list(cls._metadata[t]["columns"])
                cur.execute(
                    f"SELECT name "
                    f"FROM sqlite_master "
                    f"WHERE type='table' AND name='{t}'"
                )
                if cur.fetchone():
                    cur.execute(f"SELECT * FROM [{t}] LIMIT 1;")
                    db_column = [c[0] for c in cur.description]

                    common = [c for c in column_in_model if c in db_column]
                    columns_str = ", ".join([f"[{c}]" for c in common])

                    cur.execute(f"ALTER TABLE [{t}] RENAME TO [{t}_old];")
                    cur.execute(ln)  # Recreación de tabla.
                    cur.execute(
                        f"INSERT INTO [{t}]({columns_str}) "
                        f"SELECT {columns_str} "
                        f"FROM [{t}_old];"
                    )
                    tabs_to_drop.append(t)
                    continue
                else:
                    cur.execute(ln)

        # Conexion; FK desactivado.
        with db_connection(db_path=cls._db_file, no_foreign=True) as (
            conn,
            cur,
        ):
            for drop_tab in tabs_to_drop:
                cur.execute(f"DROP TABLE IF EXISTS [{drop_tab}_old];")

        logger.info(f"PASSED TABLES: [{list(cls._metadata.keys())}].")
        logger.info(f"SUCCESSFUL ONES: [{order}].")

    @classmethod
    def _pydantic_validators_(cls) -> None:

        # Llaves extensas -> Se usan mas abajo.
        KEY = "foreign_key"
        JSON = "json_schema_extra"

        # 0. Objetivo -> Validadores
        CREATE = "CreateValidator"
        ADAPTER = "AdapterValidator"
        ANNOTATED = "AnnotatedFields"

        # 1. Acceder a los modelos
        for table, model in cls._family.items():
            # 2. Skip... Si "validators" existe en mdoelo actual.
            skip = cls._metadata[table].get("validators", False)
            if skip:
                continue

            # 3. Esquema Pydantic de la Tabla
            schema = cls._json[table]["schema"]

            # 4. Objetivo -> Mapear Annotated[] por campos de modelo.
            FIELDS = {}

            cls._metadata[table]["validators"] = {
                CREATE: None,
                ADAPTER: {},
                ANNOTATED: None,
            }

            # 5. Extraer metatada por campo
            for name, field in schema.items():
                KWARGS = {}

                # Validadores Capa 1
                f_type = field.get("type", str)
                f_required = field.get("required", False)
                f_default = field.get("default", None)
                f_constraints = field.get("constraints", {})
                f_metadata = field.get("metadata", {})

                # Validadores Capa 2
                f_comment = f_metadata.get("comment", "NO COMMENT")
                f_unique = f_constraints.get("unique", False)

                # Text -> 'No Valida Extra' ... Salto a 'Char'
                f_max_length = f_constraints.get("max_length", None)
                f_min_length = f_constraints.get("min_length", None)

                # 'Int' y 'Float'
                f_lt = f_constraints.get("lt", None)
                f_le = f_constraints.get("le", None)
                f_gt = f_constraints.get("gt", None)
                f_ge = f_constraints.get("ge", None)

                # Bool --> 'No valida Extra' ... Salto a 'ForeignKey'
                f_key = f_metadata.get(KEY, {})
                if f_key:
                    f_second_table = f_metadata[KEY].get("second_table")
                    f_column_id = f_metadata[KEY].get("column_id")
                else:
                    f_second_table = None
                    f_column_id = None

                # 6. Construir 'KWARGS' Pydantic de cada campo
                KWARGS[JSON] = {}

                # Capa Profunda -> Extra Metadata "NO PYDANTIC"
                KWARGS[JSON].update({"unique": f_unique})
                KWARGS[JSON].update({"second_table": f_second_table})
                KWARGS[JSON].update({"column_id": f_column_id})

                # Capa Superior -> Argumentos Nombrados "SI PYDANTIC"
                KWARGS.update({"description": f_comment})
                KWARGS.update({"max_length": f_max_length})
                KWARGS.update({"min_length": f_min_length})
                KWARGS.update({"lt": f_lt})
                KWARGS.update({"le": f_le})
                KWARGS.update({"gt": f_gt})
                KWARGS.update({"ge": f_ge})

                # 7. Asegurar | Default | Required | Optional
                if f_default is not None:
                    TYPE = Optional[f_type]
                    DEFAULT = f_default
                elif f_required:
                    TYPE = f_type
                    DEFAULT = ...
                else:
                    TYPE = Optional[f_type]
                    DEFAULT = None

                # 8. Guardar Campo Pydantic "Anottated"
                FIELDS[name] = Annotated[TYPE, Field(DEFAULT, **KWARGS)]

            # 9. Modelo "CREATE"
            MODEL = create_model(
                f"{model.__name__.capitalize()}{CREATE}", **FIELDS
            )
            cls._metadata[table]["validators"][CREATE] = MODEL

            # 10. Adapter "UPDATE" | "DELETE" <- Comparten Validador
            for name, annotated in FIELDS.items():
                adapter = TypeAdapter(annotated)
                cls._metadata[table]["validators"][ADAPTER].update(
                    {name: adapter}
                )

            # 11. Respaldo Metadata Por Campos
            cls._metadata[table]["validators"][ANNOTATED] = FIELDS

        return

    @classmethod
    def _expose_validators_(cls) -> None:
        # Llaves
        CREATE = "CreateValidator"
        ADAPTER = "AdapterValidator"
        ANNOTATED = "AnnotatedFields"

        # Exponers CREATE Y ADTAPERS -> Validators
        for table, model in cls._family.items():
            # Validar existencia
            FLAG1 = cls._metadata[table].get("CREATE", False)
            FLAG2 = cls._metadata[table].get("ADAPTER", False)
            FLAG3 = cls._metadata[table].get("ANNOTATED", False)

            if FLAG1 and FLAG2 and FLAG3:
                continue

            # Exposicion:
            create = cls._metadata[table]["validators"][CREATE]
            adapter = cls._metadata[table]["validators"][ADAPTER]
            annotated = cls._metadata[table]["validators"][ANNOTATED]

            cls.CREATE = create
            cls.ADAPTER = adapter
            cls.ANNOTATED = annotated

        return

    @classmethod
    def _routers_(cls) -> None:
        """
        Importantante:
        Evaluar; Si modelo ya tiene routers

        1. Se iteran los modelos.
        2. Si ya tiene routers, se salta al siguiente.

        """

        # Iterar modelos:
        for n, m in cls._family.items():
            # Evaluar si los routers existen:
            if n in cls.MODEL_COUNT_ROUTERS:
                msg = (
                    f"'{cls._table}' model's routers have been "
                    f"already created... moving to next one..."
                )
                logger.debug(msg)
                continue

            # Cargar esquemas pydantic de validacion
            CREATE = m.CREATE

            # Creacion del router en esta iteracion.
            router = APIRouter(prefix=f"/{m.__name__.lower()}")

            # --*-- CREACION DE ENDPOINTS --*--

            def make_read_all(model):
                def read_all():
                    return model.all().to_dict(label=True)

                return read_all

            def make_create(model):
                def create(data: CREATE, model=model):
                    dicc = data.model_dump()
                    cols = model._metadata[model._table]["columns"]
                    SQL = [dicc.get(col) for col in cols]
                    line = tuple(SQL)
                    kwargs = {model._table: [line]}
                    model.i(**kwargs)
                    return {"ms": "Data Created"}

                return create

            def make_update(model):
                def update(kwargs: UpdateFilterValidator, model=model):
                    argument = kwargs.filters
                    entire = kwargs.update_all
                    model.u(**argument, update_all=entire)
                    return {"ms": "Data Updated"}

                return update

            def make_delete(model):
                def delete(kwargs: DeleteFilterValidator, model=model):
                    filters = kwargs.filters
                    model.d(**filters)
                    return {"ms": "Data Deleted"}

                return delete

            def make_api_query(model):
                def api_query(
                    s: ValidateSelect,
                    f: ValidateFilter,
                    lk: ValidateLink,
                    g: ValidateGroupBy,
                    o: ValidateOrderBy,
                    limit: ValidateLimitOffset,
                    offset: ValidateLimitOffset,
                    model=model,
                ):
                    select = s.select
                    filters = f.filters
                    links = lk.links
                    groups = g.groups
                    orders = o.orders
                    limits = limit.num
                    offsets = offset.num

                    dicc = (
                        model.select(*select)
                        .filter(**filters)
                        .link(*links)
                        .gp(**groups)
                        .sort(**orders)
                        .lim(limit=limits)
                        .off(offset=offsets)
                        .all()
                        .to_dict(label=True)
                    )
                    return dicc

                return api_query

            def make_api_pro_query(model):
                def api_pro_query(
                    s: ValidateSelect,
                    f: ValidateFilter,
                    ad: ValidateAdd,
                    g: ValidateGroupBy,
                    o: ValidateOrderBy,
                    limit: ValidateLimitOffset,
                    offset: ValidateLimitOffset,
                    model=model,
                ):
                    select = s.select
                    filters = f.filters
                    added = ad.added
                    groups = g.groups
                    orders = o.orders
                    limits = limit.num
                    offsets = offset.num

                    dicc = (
                        model.select(*select)
                        .filter(**filters)
                        .add(**added)
                        .gp(**groups)
                        .sort(**orders)
                        .lim(limit=limits)
                        .off(offset=offsets)
                        .all()
                        .to_dict(label=True)
                    )
                    return dicc

                return api_pro_query

            # --*-- EXPOSICION DE ENDPOINTS --*--

            router.add_api_route("/", make_read_all(m), methods=["GET"])
            router.add_api_route("/", make_create(m), methods=["POST"])
            router.add_api_route("/", make_update(m), methods=["PUT"])
            router.add_api_route("/", make_delete(m), methods=["DELETE"])
            router.add_api_route(
                "/query/", make_api_query(m), methods=["POST"]
            )
            router.add_api_route(
                "/pquery/", make_api_pro_query(m), methods=["POST"]
            )

            cls.ROUTERS.append(router)
            cls.MODEL_COUNT_ROUTERS.append(n)

        return

    @classmethod  # Inyeccion Segura
    def table_exists(cls):
        """
        Valida la existencia de una tabla cuando es creada a traves
        de PanCakesORM.

        Devuelve True si la tabla existe en la base de datos.
        """
        cls._clean_table_name_()
        with db_connection(db_path=cls._db_file) as (conn, cur):
            res = cur.execute(
                "SELECT name FROM sqlite_master WHERE name = ?;", (cls._table,)
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
        limit: int = None,
        offset: int = None,
    ):
        """
        Llama a la funcion de consulta global.
        Documentacion: cook/flavor
        """
        db_path = cls._db_file if db_path is None else db_path
        select = "*" if select is None else select
        _from = cls._table if _from is None else _from
        sp_select = None if sp_select is None else sp_select
        join = None if join is None else join
        condition = None if condition is None else condition
        group_by = None if group_by is None else group_by
        order_by = None if order_by is None else order_by
        limit = None if limit is None else limit
        offset = None if offset is None else offset

        res, col = query(
            db_path=db_path,
            select=select,
            _from=_from,
            sp_select=sp_select,
            join=join,
            condition=condition,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

        return res, col

    @classmethod  # Inyeccion Segura
    def insert(cls, params: list, db_path: str = None):
        """
        Invoca a la funcion insert()
        Documentacion de los parametros en /cook/furnace.py
        """
        db_path = db_path if db_path else cls._db_file

        insert(db_path=db_path, params=params)

    @classmethod  # Inyeccion Segura
    def update(cls, params: list, db_path: str = None, update_all: bool = None):
        """
        Invoca a la funcion update()
        Documentacion de los parametros en /cook/ingredient.py
        """
        db_path = cls._db_file if db_path is None else db_path
        update_all = False if update_all is None else update_all

        update(db_path=db_path, params=params, update_all=update_all)

    @classmethod  # Inyeccion Segura
    def delete(
        cls,
        params: list,
        db_path: str = None,
        delete_all: bool = None,
        force: bool = None,
    ):
        """
        Invoca a la funcion delete()
        Documentacion de los parametros en /cook/clean.py
        """
        db_path = db_path if db_path else cls._db_file
        delete_all = delete_all if delete_all else False
        force = force if force else False

        delete(
            db_path=db_path, params=params, delete_all=delete_all, force=force
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

    @classmethod  # Inyección Segura
    def off(cls, num: int = None):
        return cls.q().off(num)

    @classmethod  # Inyeccion Segura
    def all(cls):
        return cls.q().all()

    @classmethod  # Inyeccion Segura
    def count(cls):
        return cls.q().count()

    # --*-- HELPERS CRUD --*--

    # Insert declarativo
    @classmethod
    def manager(cls):
        return AbstractBox(model=cls)

    @classmethod  # Inyeccion Segura
    def i(cls, **kwargs):
        return cls.manager().i(**kwargs)

    # Update declarativo
    @classmethod  # Inyeccion Segura
    def u(cls, update_all=False, **kwargs):
        return cls.manager().u(update_all=update_all, **kwargs)

    # Delete declarativo
    @classmethod  # noqa: W191
    def d(cls, **kwargs):
        return cls.manager().d(**kwargs)

# Despues de carga: Inyectar "Clase Modelo" -> Validadores Pydantic
ValidateSelect.MODEL = PanCakesORM
ValidateFilter.MODEL = PanCakesORM
ValidateLink.MODEL = PanCakesORM
ValidateGroupBy.MODEL = PanCakesORM
ValidateOrderBy.MODEL = PanCakesORM
ValidateAdd.MODEL = PanCakesORM
DeleteFilterValidator.MODEL = PanCakesORM
UpdateFilterValidator.MODEL = PanCakesORM
