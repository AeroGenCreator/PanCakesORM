# PanCakesORM
![static](https://img.shields.io/badge/python-python?style=for-the-badge&logo=python&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/sqlite-sqlite?style=for-the-badge&logo=sqlite&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/pytest-pytest?style=for-the-badge&logo=pytest&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/git--hub-git--bun?style=for-the-badge&logo=github&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/pypi-pypi?style=for-the-badge&logo=pypi&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/ipdb-ipdb?style=for-the-badge&logoColor=white&label=debugger&labelColor=black&color=yellow)

![image](assets/banner.png)

**PanCakesORM: Gestión Evolutiva para SQLite3**

PanCakesORM es un ORM ligero diseñado para acelerar el desarrollo de proyectos, permitiendo que tu base de datos esté lista en apenas unas líneas de código. Con PanCakesORM, el desarrollador se enfoca en la lógica de negocio y el frontend, mientras el motor se encarga del esquema:

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

## 🚀 Instalación

Instálalo directamente desde PyPI:

```bash
pip install pancakes-orm
```

## 🛠️ Uso Básico

### Definir un Modelo
Crea tus tablas definiendo clases que hereden de `PanCakesORM`.

```python
from pancakes.cook.mold import PanCakesORM
from pancakes.datatype import sql_datatype

class User(PanCakesORM):
    _table = 'users'
    
    name = sql_datatype.Char(comment='User Name')
    age = sql_datatype.Integer(comment='User Age')
```

### Insertar Datos
```python
User.write([
    (None, 'Andres Lopez', 30),
    (None, 'Gemini AI', 1)
])
```

### Consultas con `pancakes`
```python
from pancakes.tool.function import pancakes

results, columns = pancakes(
    db_path='my_app.db',
    select='*',
    from_='users',
    condition=[
        {'table': 'users', 'column': 'age', 'operator': '>', 'value': 18}
    ]
)
```

