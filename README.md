# PanCakesORM
![static](https://img.shields.io/badge/python-python?style=for-the-badge&logo=python&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/sqlite-sqlite?style=for-the-badge&logo=sqlite&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/pytest-pytest?style=for-the-badge&logo=pytest&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/git--hub-git--bun?style=for-the-badge&logo=github&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/pypi-pypi?style=for-the-badge&logo=pypi&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/ipdb-ipdb?style=for-the-badge&logoColor=white&label=debugger&labelColor=black&color=yellow)

![image](assets/banner.png)

**PanCakesORM: Gestión Evolutiva para SQLite3**

Con PanCakesORM, el desarrollador se enfoca en la lógica de negocio y el frontend, mientras el motor se encarga del esquema:

    Sincronización Automática: Gracias a __init_subclass__, 
    las tablas y esquemas se crean o migran en tiempo real al heredar de la clase padre.

    Seguridad Integrada: Implementa una sanitización estricta de nombres 
    de tablas y columnas, además de usar ? (query parameters) para prevenir ataques de inyección SQL.

    Migraciones Inteligentes: El método _table_on_change gestiona cambios en el número de columnas 
    (añadir/eliminar) moviendo los datos automáticamente a tablas temporales para preservar la integridad.

    Consultas Avanzadas: La función pancakes permite realizar JOINs (INNER, LEFT), 
    agrupaciones y filtrado complejo mediante una interfaz de diccionarios intuitiva.

    Cero Configuración: Maneja rutas por defecto, creación de directorios 
    y archivos .sqlite de forma transparente para el desarrollador.

    Preparado para Producción: Incluye logging profesional y está optimizado 
    para evitar validaciones redundantes tras la primera compilación.

[Link a PyPI](https://pypi.org/project/pancakes-orm/)

[Documentacion Oficial](https://fringe-edge-3f8.notion.site/PanCakesORM-32b8851a844d80bd8299f040210ee165)

## 🚀 Instalación

Instálalo directamente desde PyPI:

```bash
pip install pancakes-orm
```

## 🛠️ Uso Básico

### Definir un Modelo
Crea tus tablas definiendo clases que hereden de `PanCakesORM`. Atributo `_table` obligatorio.

```python
from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype

class User(PanCakesORM):
    _table = 'users'
    
    name = sql_datatype.Char(comment='User Name')
    age = sql_datatype.Integer(comment='User Age')
```

### Insertar Datos con `insert()`
```python
from pancakes.cook.furnace import insert
insert(
    db_path="data/my_app_database.sqlite",
    params=[
        {
        'table':'users',
        'data':[
                (None, 'Andres', 25),
                (None, 'Polar', 10),
            ]
        }
    ]
)
```

### Consultas con `query()`
```python
from pancakes.cook.layer import query

results, columns = query(
    db_path="data/my_app_database.sqlite",
    select='*',
    _from='users',
    condition=[
        {'table': 'users', 'column': 'age', 'operator': '>', 'value': 18}
    ]
)
```
## PanCakesORM a Fondo:

1. Clase Padre PanCakesORM: Permite la creación automática de tablas en bases de datos mediante herencia de clase `pancakes/cook/mold.py`.

2. Seguridad y Robustez: * Capa de Sanitización: Implementa métodos avanzados de limpieza y seguridad para prevenir ataques de Inyección SQL.

3. Gestión de Transacciones: Configuración optimizada para múltiples lecturas, con soporte nativo para Rollbacks y Commits que aseguran la integridad de los datos.

4. Operaciones CRUD Optimizadas: Métodos de Escritura (`.insert()`), Actualización (`.update()`), Eliminación (`.delete()`) y Lectura (`.query()`) testeados para alto rendimiento.

5. Conexión Multihilo: Configuración de base de datos preparada para gestionar múltiples lecturas concurrentes de forma estable.

6. Declaración de Tipos: Clases que operan en conjunto con el ORM para definir tipos de datos de forma estructurada `pancakes/datatype/sql_datatype`.

7. SQLite3 ForeignKey(): Clase para relacionar tablas; el ORM gestiona automáticamente los constraints de llaves foráneas sin escribir una sola línea de SQL `pancakes/datatype/sql_datatype`.

8. Método de Consulta Avanzada: Uso del método .sql() para sentencias SQL puras. Se recomienda su uso exclusivo para lógica de desarrollo y no para interacción directa con el usuario `pancakes/cook/mold.py`.
