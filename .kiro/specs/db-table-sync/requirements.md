# Requirements Document

## Introduction

Este feature implementa un módulo de sincronización de tablas entre bases de datos de diferentes proveedores (ej: MySQL, PostgreSQL, SQLite, etc.) usando SQLAlchemy como capa de abstracción, Pandas para la transformación de datos y python-dotenv para la gestión de configuración. El módulo permite extraer datos de una tabla origen mediante una query SELECT configurable e insertarlos en una tabla destino, con soporte para mapeo de columnas entre origen y destino.

## Glossary

- **Syncer**: Componente principal que orquesta el proceso de sincronización.
- **Source_Connection**: Conexión SQLAlchemy a la base de datos de origen.
- **Target_Connection**: Conexión SQLAlchemy a la base de datos de destino.
- **Source_Table**: Nombre de la tabla en la base de datos de origen.
- **Target_Table**: Nombre de la tabla en la base de datos de destino. Si no se especifica, se asume el mismo nombre que Source_Table.
- **Column_List**: Lista de nombres de columnas a sincronizar desde la tabla origen.
- **Column_Mapping**: Diccionario opcional que define la correspondencia entre nombres de columnas en origen y destino (ej: `{"id_user": "id_usuario"}`).
- **Sync_Query**: Sentencia SELECT generada o provista para extraer los datos del origen.
- **DataFrame**: Estructura de datos de Pandas que representa el resultado de la query.

---

## Requirements

### Requirement 1: Configuración de conexiones

**User Story:** As a developer, I want to provide two database connections (source and target), so that the Syncer can operate across different database providers without being tied to a specific vendor.

#### Acceptance Criteria

1. THE Syncer SHALL accept a Source_Connection and a Target_Connection como instancias de `sqlalchemy.engine.Engine` o cadenas de conexión (connection strings).
2. WHEN a connection string is provided, THE Syncer SHALL create the corresponding SQLAlchemy engine internally.
3. IF a Source_Connection cannot be established, THEN THE Syncer SHALL raise a descriptive `ConnectionError` indicating the source failed.
4. IF a Target_Connection cannot be established, THEN THE Syncer SHALL raise a descriptive `ConnectionError` indicating the target failed.

---

### Requirement 2: Definición de tablas

**User Story:** As a developer, I want to specify source and target table names, so that I can sync data between tables with different names across databases.

#### Acceptance Criteria

1. THE Syncer SHALL accept a Source_Table name as a required parameter.
2. WHEN no Target_Table is specified, THE Syncer SHALL use the Source_Table name as the Target_Table name.
3. WHEN a Target_Table is explicitly specified, THE Syncer SHALL use that name for the destination table.

---

### Requirement 3: Selección de columnas

**User Story:** As a developer, I want to define which columns to synchronize, so that I can transfer only the relevant fields between tables.

#### Acceptance Criteria

1. THE Syncer SHALL accept a Column_List as a required parameter containing at least one column name.
2. WHEN a Column_List is provided, THE Syncer SHALL generate a Sync_Query of the form `SELECT col1, col2, ... FROM {Source_Table}`.
3. IF Column_List is empty or not provided, THEN THE Syncer SHALL raise a `ValueError` with a descriptive message.
4. WHEN the Sync_Query is executed, THE Syncer SHALL load the results into a DataFrame.

---

### Requirement 4: Mapeo de columnas

**User Story:** As a developer, I want to define a column mapping between source and target, so that I can sync tables whose column names differ between databases.

#### Acceptance Criteria

1. WHERE a Column_Mapping is provided, THE Syncer SHALL rename the DataFrame columns according to the mapping before inserting into the target.
2. WHERE a Column_Mapping is provided, THE Syncer SHALL generate the INSERT statement using the mapped (destination) column names.
3. WHEN a column in Column_Mapping does not exist in Column_List, THE Syncer SHALL raise a `ValueError` indicating the unmapped column name.
4. WHEN no Column_Mapping is provided, THE Syncer SHALL use the original column names from Column_List for the INSERT statement.

---

### Requirement 5: Ejecución de la sincronización

**User Story:** As a developer, I want to execute the sync process with a single method call, so that data is extracted from the source and inserted into the target reliably.

#### Acceptance Criteria

1. WHEN the sync method is called, THE Syncer SHALL execute the Sync_Query against the Source_Connection and load results into a DataFrame.
2. WHEN the DataFrame is loaded, THE Syncer SHALL insert all rows into the Target_Table using the Target_Connection.
3. WHEN the insert operation completes successfully, THE Syncer SHALL return the number of rows synchronized as an integer.
4. IF an error occurs during query execution, THEN THE Syncer SHALL raise a descriptive `RuntimeError` with the original exception context.
5. IF an error occurs during the insert operation, THEN THE Syncer SHALL raise a descriptive `RuntimeError` with the original exception context.

---

### Requirement 6: Estrategia de inserción

**User Story:** As a developer, I want to control how data is inserted into the target table, so that I can choose between appending, replacing, or failing on existing data.

#### Acceptance Criteria

1. THE Syncer SHALL accept an `if_exists` parameter with allowed values: `"append"`, `"replace"`, `"fail"`.
2. WHEN `if_exists` is not specified, THE Syncer SHALL default to `"append"`.
3. IF an invalid value is provided for `if_exists`, THEN THE Syncer SHALL raise a `ValueError` listing the allowed values.

---

### Requirement 7: Configuración mediante variables de entorno

**User Story:** As a developer, I want to load database connection strings from environment variables using python-dotenv, so that credentials are not hardcoded in the source code.

#### Acceptance Criteria

1. THE Syncer SHALL support loading connection strings from environment variables via python-dotenv.
2. WHEN a `.env` file is present in the working directory, THE Syncer SHALL load it automatically if the caller uses the provided helper.
3. WHERE environment variable loading is used, THE Syncer SHALL provide a helper function `get_engine_from_env(var_name: str)` that reads the variable and returns a SQLAlchemy engine.
4. IF the specified environment variable is not set, THEN `get_engine_from_env` SHALL raise a `KeyError` with a descriptive message indicating the missing variable name.

---

### Requirement 8: Logging del proceso

**User Story:** As a developer, I want the sync process to emit log messages, so that I can monitor progress and diagnose issues without modifying the library code.

#### Acceptance Criteria

1. THE Syncer SHALL emit a log message at INFO level before executing the Sync_Query, including the Source_Table name and column count.
2. WHEN the DataFrame is loaded, THE Syncer SHALL emit a log message at INFO level indicating the number of rows retrieved.
3. WHEN the insert completes, THE Syncer SHALL emit a log message at INFO level indicating the number of rows inserted and the Target_Table name.
4. IF an error occurs, THE Syncer SHALL emit a log message at ERROR level with the exception details before re-raising.
