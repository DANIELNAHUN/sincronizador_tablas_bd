# Tasks: db-table-sync

## Task List

- [x] 1. Estructura del módulo y dependencias
  - [x] 1.1 Crear la estructura de directorios `db_table_sync/` con `__init__.py`, `syncer.py` y `env_helper.py`
  - [x] 1.2 Crear `requirements.txt` con `sqlalchemy`, `pandas`, `python-dotenv` y `hypothesis[pytest]`

- [x] 2. Implementar `get_engine_from_env`
  - [x] 2.1 Implementar `get_engine_from_env(var_name: str) -> Engine` en `env_helper.py` que cargue `.env` con `python-dotenv`, lea la variable y retorne un `Engine`; lanzar `KeyError` descriptivo si la variable no está definida
  - [x] 2.2 Exportar `get_engine_from_env` desde `__init__.py`

- [x] 3. Implementar `Syncer.__init__`
  - [x] 3.1 Aceptar parámetros `source`, `target`, `source_table`, `column_list`, `target_table`, `column_mapping`, `if_exists` con sus tipos y defaults según el diseño
  - [x] 3.2 Validar que `column_list` no esté vacía; lanzar `ValueError` descriptivo si lo está
  - [x] 3.3 Validar que `if_exists` sea uno de `"append"`, `"replace"`, `"fail"`; lanzar `ValueError` con los valores permitidos si no lo es
  - [x] 3.4 Validar que todas las claves de `column_mapping` existan en `column_list`; lanzar `ValueError` indicando la clave problemática
  - [x] 3.5 Convertir cadenas de conexión a `Engine` usando `sqlalchemy.create_engine`; verificar conectividad con `engine.connect()`; lanzar `ConnectionError` descriptivo (indicando source o target) si falla

- [x] 4. Implementar `Syncer.sync`
  - [x] 4.1 Construir la `Sync_Query` como `SELECT col1, col2, ... FROM {source_table}` usando `sqlalchemy.text`
  - [x] 4.2 Ejecutar la query contra `source` y cargar el resultado en un `DataFrame` con `pd.read_sql`; capturar excepciones y lanzar `RuntimeError` con `__cause__`
  - [x] 4.3 Aplicar `column_mapping` al DataFrame con `DataFrame.rename` si se proporcionó
  - [x] 4.4 Insertar el DataFrame en `target_table` usando `DataFrame.to_sql` con el parámetro `if_exists`; capturar excepciones y lanzar `RuntimeError` con `__cause__`
  - [x] 4.5 Retornar el número de filas insertadas como `int`

- [x] 5. Implementar logging
  - [x] 5.1 Agregar `logger = logging.getLogger(__name__)` en `syncer.py`
  - [x] 5.2 Emitir `INFO` antes de ejecutar la query (tabla origen, número de columnas)
  - [x] 5.3 Emitir `INFO` al cargar el DataFrame (número de filas recuperadas)
  - [x] 5.4 Emitir `INFO` al completar la inserción (número de filas, tabla destino)
  - [x] 5.5 Emitir `ERROR` con detalles de la excepción antes de re-lanzar en cualquier bloque `except`

- [x] 6. Unit tests (pytest)
  - [x] 6.1 Test: inicialización con Engine y con connection string (SQLite en memoria)
  - [x] 6.2 Test: `target_table` por defecto igual a `source_table`
  - [x] 6.3 Test: `if_exists` por defecto `"append"`
  - [x] 6.4 Test: `ValueError` con `column_list` vacía
  - [x] 6.5 Test: `ValueError` con clave de `column_mapping` inválida
  - [x] 6.6 Test: `ValueError` con `if_exists` inválido
  - [x] 6.7 Test: `get_engine_from_env` con variable presente retorna Engine
  - [x] 6.8 Test: `get_engine_from_env` con variable ausente lanza `KeyError`
  - [x] 6.9 Test: sync end-to-end con SQLite en memoria (origen y destino), verifica filas y valor de retorno
  - [x] 6.10 Test: sync con `column_mapping` verifica que las columnas destino tienen los nombres mapeados
  - [x] 6.11 Test: `RuntimeError` ante tabla origen inexistente

- [x] 7. Property tests (Hypothesis)
  - [x] 7.1 Property 1 — `test_conn_string_creates_engine`: para cualquier conn string SQLite válida, el Syncer crea un Engine interno
  - [x] 7.2 Property 3 — `test_target_table_resolution`: para cualquier `source_table`, si `target_table` es None el efectivo es `source_table`; si se especifica, se usa el especificado
  - [x] 7.3 Property 4 — `test_sync_query_format`: para cualquier lista de columnas no vacía y nombre de tabla, la query generada contiene exactamente esas columnas y ese nombre
  - [x] 7.4 Property 6 — `test_column_mapping_applied`: para cualquier mapping válido, las columnas del DataFrame antes de insertar tienen los nombres destino
  - [x] 7.5 Property 8 — `test_sync_round_trip`: para cualquier conjunto de filas en SQLite origen, `sync()` retorna N y la tabla destino contiene exactamente esas N filas
  - [x] 7.6 Property 10 — `test_if_exists_validation`: valores válidos no lanzan error; cualquier valor fuera de los tres permitidos lanza `ValueError`
  - [x] 7.7 Property 13 — `test_sync_emits_logs`: cualquier sync exitoso emite al menos 3 mensajes INFO; cualquier sync con error emite al menos 1 ERROR
