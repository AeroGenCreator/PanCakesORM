# PanCakesORM

![static](https://img.shields.io/badge/python-python?style=for-the-badge&logo=python&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/sqlite-sqlite?style=for-the-badge&logo=sqlite&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/pytest-pytest?style=for-the-badge&logo=pytest&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/git--hub-git--bun?style=for-the-badge&logo=github&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/pypi-pypi?style=for-the-badge&logo=pypi&logoColor=white&labelColor=black&color=yellow)
![static](https://img.shields.io/badge/ipdb-ipdb?style=for-the-badge&logoColor=white&label=debugger&labelColor=black&color=yellow)

![image](assets/banner.png)

# PanCakesORM 🥞

**Simplify Your Data Layer**

PanCakesORM es una librería de mapeo objeto-relacional (ORM) para Python y SQLite3 que prioriza la legibilidad y la velocidad de desarrollo. Si buscas la potencia de SQL con la elegancia de una sintaxis declarativa y moderna, PanCakesORM es para ti.

---

## 🚀 Inicio Rápido

Obtener PanCakesORM es tan sencillo como un comando. Disponible directamente en **PyPI**:

```bash
pip install pancakes-orm
```

**Agrega tus rutas**

En la raiz de proyecto agrega el archivo .env "De esta manera controlas `directorio` base de datos y fichero `Sqlite3` -> Base de Datos."

```
LOG=INFO
DB_DIR=data
DB_FILE=database.sqlite3
```

**Tu primera tabla en segundos**

```python
from pancakes.models.model import PanCakesORM
from pancakes.sql import datatype

class User(PanCakesORM):  # Tu fichero .env carga las rutas
    _table = "user"
    _depends = "self"
    
    name = datatype.Char(comment="User Name")
    age = datatype.Integer(comment="User Age")

# ¡Insertar!
User.i(user=[(None, "Omar", 30)])  # <- El primer elemento de tupla es el "id"
# ¡Sqlit3 Lo maneja por ti!

# ¡Listo para consultar!
users = User.filter(user__age__gt=18).all().to_dict()
```

# [Link a PyPI](https://pypi.org/project/pancakes-orm/) | [Documentacion Oficial](https://fringe-edge-3f8.notion.site/PanCakesORM-3408851a844d80a39dd9c813c88cfb16)

### 💪 Fortalezas y Robustez

PanCakesORM no es solo una cara bonita; está construido para ser el motor confiable de tus aplicaciones:

    Sintaxis Declarativa Fluida: Olvídate de concatenar strings de SQL. 
    Usa métodos encadenados (.filter(), .link(), .sort()) que se leen como lenguaje natural.

    Escalabilidad: Gracias a su gestión eficiente de esquemas y su sistema de inyección segura,
    puede crecer desde un pequeño script hasta aplicaciones empresariales complejas.

    Integración Moderna: Diseñado para brillar en ecosistemas de alto rendimiento:

        FastAPI: Tipado compatible para respuestas JSON rápidas.

        Streamlit: Ideal para aplicaciones de datos donde la velocidad de desarrollo es clave.

    Calidad Garantizada: La robustez de la librería está respaldada por una batería
    de más de 125 pruebas automatizadas utilizando pytest. Puedes consultar la suite completa en la carpeta /tests.

### 🏗️ Arquitectura de Consultas (QueryBox)

El corazón de PanCakesORM es el QueryBox, que permite realizar operaciones complejas de forma visual y estructurada:

    Joins Automáticos: Usa .link('tabla') y deja que el ORM gestione las llaves foráneas por ti.

    Agregaciones: Calcula SUM, AVG, COUNT directamente en el select con sufijos como __avg.

    Lógica Booleana: Encadena condiciones con __and y __or de forma nativa en los argumentos.

### 🗂️ Jerarquia de Directorios

```
        .
        └── pancakes/
            ├── models/
            │   └── model.py
            ├── sql/
            │   └── datatype.py
            ├── valid/
            │   ├── query_validator.py
            │   └── filter_validator.py
            ├── orm/
            │   ├── insert.py
            │   ├── update.py
            │   ├── delete.py
            │   └── query.py
            ├── tools/
            │   └── functions.py
            └── abstract/
                ├── query_box.py
                └── abstract_box.py
```

### 🛠️ Requisitos

    Python 3.12+

    SQLite3

    fastapi[standard]

    python-dotenv