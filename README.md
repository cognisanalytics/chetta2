# ETL Pipeline: SQL Server to PostgreSQL

A Python ETL pipeline that extracts data from SQL Server, transforms it (pass-through), and loads it into PostgreSQL with upsert capabilities.

## Features

- **Extract**: Execute SELECT queries on SQL Server
- **Transform**: Pass-through data transformation (no changes)
- **Load**: Upsert data into PostgreSQL with error handling, rollback, and progress tracking
- **Configuration**: Environment-based configuration management
- **Logging**: Comprehensive logging for monitoring and debugging
- **Error Handling**: Robust error handling with retry mechanisms

## Project Structure

```
extraction/
├── README.md
├── requirements.txt
├── .env.example
├── config/
│   └── database_config.py
├── src/
│   ├── extract/
│   │   └── sql_server_extractor.py
│   ├── transform/
│   │   └── data_transformer.py
│   ├── load/
│   │   └── postgresql_loader.py
│   └── utils/
│       └── logger.py
└── main.py
```

## Setup

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

3. **Configure Database Connections**
   Edit `.env` file with your database details:
   ```env
   # SQL Server Configuration
   SQL_SERVER_DRIVER=ODBC Driver 17 for SQL Server
   SQL_SERVER_SERVER=your_sql_server_host
   SQL_SERVER_DATABASE=your_database_name
   SQL_SERVER_USERNAME=your_username
   SQL_SERVER_PASSWORD=your_password
   SQL_SERVER_TRUSTED_CONNECTION=no

   # PostgreSQL Configuration
   POSTGRES_HOST=your_postgres_host
   POSTGRES_PORT=5432
   POSTGRES_DATABASE=your_postgres_database
   POSTGRES_USERNAME=your_postgres_username
   POSTGRES_PASSWORD=your_postgres_password

   # ETL Configuration
   BATCH_SIZE=1000
   MAX_RETRIES=3
   ```

## Usage

### Basic Usage

```python
from main import ETLPipeline

# Define your queries and table configurations
queries = [
    "SELECT * FROM users WHERE created_date >= '2024-01-01'",
    "SELECT * FROM orders WHERE order_date >= '2024-01-01'"
]

table_names = ["users", "orders"]
conflict_columns_list = [["user_id"], ["order_id"]]
update_columns_list = [
    ["username", "email", "created_date"],
    ["customer_id", "order_date", "total_amount"]
]

# Run ETL Pipeline
etl = ETLPipeline()
success = etl.run_etl(queries, table_names, conflict_columns_list, update_columns_list)
```

### Command Line Usage

```bash
python main.py
```

## Configuration

### SQL Server Connection
- Uses ODBC driver for SQL Server connectivity
- Supports both username/password and trusted connection authentication
- Configure connection parameters in `.env` file

### PostgreSQL Connection
- Uses psycopg2 for PostgreSQL connectivity
- Supports upsert operations with conflict resolution
- Batch processing for performance

### ETL Parameters
- `BATCH_SIZE`: Number of rows to process in each batch (default: 1000)
- `MAX_RETRIES`: Maximum number of retry attempts (default: 3)

## Error Handling

- **Connection Errors**: Automatic retry with exponential backoff
- **Transaction Management**: Automatic rollback on errors
- **Logging**: Comprehensive logging to both console and file
- **Progress Tracking**: Real-time progress updates during processing

## Logging

Logs are written to:
- Console: INFO level and above
- File: `logs/etl_YYYYMMDD_HHMMSS.log` (one file per run; DEBUG level and above)

## Dependencies

- `pyodbc`: SQL Server connectivity
- `psycopg2-binary`: PostgreSQL connectivity
- `pandas`: Data manipulation
- `python-dotenv`: Environment variable management

## Notes

- Ensure SQL Server ODBC driver is installed on Windows machine
- PostgreSQL tables must exist before running the ETL process
- Conflict columns define the primary key or unique constraint for upsert operations
- Update columns specify which columns to update on conflict (if not specified, all non-conflict columns are updated)
