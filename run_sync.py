"""run_sync.py — Script de ejemplo para ejecutar una sincronización."""
from db_table_sync import Syncer, get_engine_from_env

# Lee las URLs de conexión desde el archivo .env
source = get_engine_from_env("SOURCE_DB_URL")
target = get_engine_from_env("TARGET_DB_URL")

syncer = Syncer(
    source=source,
    target=target,
    source_table="users",                  # tabla a leer en la BD origen
    column_list=["id", "name", "email"],   # columnas a sincronizar
    target_table="users_backup",           # tabla destino (opcional)
    column_mapping={"name": "full_name"},  # renombrar columnas (opcional)
    if_exists="append",                    # "append" | "replace" | "fail"
)

rows_inserted = syncer.sync()
print(f"Filas sincronizadas: {rows_inserted}")
