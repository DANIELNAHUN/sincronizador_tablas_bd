# db_table_sync

Librería Python para sincronizar tablas entre bases de datos usando SQLAlchemy y pandas. Lee una tabla de una base de datos origen, aplica un mapeo de columnas opcional y la inserta en una base de datos destino.

## Instalación

Requiere Python 3.9+. Instala las dependencias con:

```bash
pip install -r requirements.txt
```

## Configuración

Para construir los engines desde variables de entorno, crea un archivo `.env` en la raíz del proyecto:

```env
SOURCE_DB_URL=postgresql://user:password@localhost:5432/source_db
TARGET_DB_URL=postgresql://user:password@localhost:5432/target_db
```

Luego usa `get_engine_from_env` para obtener los engines:

```python
from db_table_sync import get_engine_from_env

source = get_engine_from_env("SOURCE_DB_URL")
target = get_engine_from_env("TARGET_DB_URL")
```

También puedes pasar directamente connection strings o instancias de `sqlalchemy.Engine`.

## Uso

El proyecto incluye un script de ejemplo `run_sync.py` en la raíz. Edítalo con tu tabla y columnas:

```python
from db_table_sync import Syncer, get_engine_from_env

# 1. Conectar a las bases de datos (lee SOURCE_DB_URL y TARGET_DB_URL del .env)
source = get_engine_from_env("SOURCE_DB_URL")
target = get_engine_from_env("TARGET_DB_URL")

# 2. Definir qué tabla y qué columnas sincronizar
syncer = Syncer(
    source=source,
    target=target,
    source_table="users",                 # tabla a leer en la BD origen
    column_list=["id", "name", "email"],  # columnas a sincronizar
    target_table="users_backup",          # tabla destino (opcional, por defecto igual a source_table)
    column_mapping={"name": "full_name"}, # renombrar columnas en destino (opcional)
    if_exists="append",                   # qué hacer si la tabla destino ya existe
)

# 3. Ejecutar la sincronización
rows_inserted = syncer.sync()
print(f"Filas sincronizadas: {rows_inserted}")
```

Luego ejecútalo desde la terminal:

```bash
python run_sync.py
```

### Parámetros de `Syncer`

| Parámetro        | Tipo                          | Descripción                                                      |
|------------------|-------------------------------|------------------------------------------------------------------|
| `source`         | `Engine` o `str`              | Base de datos origen                                             |
| `target`         | `Engine` o `str`              | Base de datos destino                                            |
| `source_table`   | `str`                         | Nombre de la tabla a leer                                        |
| `column_list`    | `list[str]`                   | Columnas a seleccionar (obligatorio, al menos una)               |
| `target_table`   | `str` &#124; `None`           | Nombre de la tabla destino (por defecto igual a `source_table`)  |
| `column_mapping` | `dict[str, str]` &#124; `None`| Renombrado de columnas antes de insertar                         |
| `if_exists`      | `"append"` &#124; `"replace"` &#124; `"fail"` | Comportamiento si la tabla destino ya existe (por defecto `"append"`) |

## Tests

Ejecuta todos los tests con:

```bash
pytest
```

Los tests incluyen pruebas unitarias (`tests/test_syncer.py`) y pruebas basadas en propiedades con Hypothesis (`tests/test_properties.py`).
