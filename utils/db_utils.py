import logging
import os
import io

import polars as pl
import psycopg2
import yaml
import duckdb

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(BASE_DIR, "configs.yaml")


def load_config(section: str = None, config_file: str = DEFAULT_CONFIG) -> dict:
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    if section:
        if section not in config:
            raise Exception(f"Section '{section}' not found in {config_file}")
        return config[section]

    return config


def get_db_connection(section: str = "fin_cdc"):
    db_config = load_config(section)
    try:
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False
        logger.info("Connected to PostgreSQL successfully")
        return conn
    except psycopg2.DatabaseError as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise



def transform_with_duckdb(df: pl.DataFrame, query: str):
    """
    Run SQL transformations using DuckDB
    """
    con = duckdb.connect()
    con.register("input_df", df)

    result = con.execute(query).pl()
    con.close()

    return result

import io
import uuid
import polars as pl


def load_data(
    df: pl.DataFrame,
    schema_name: str,
    table_name: str,
    conn=get_db_connection()
):
    """
    COPY → staging → UPSERT → SOFT DELETE
    """

    if df.is_empty():
        print_log(
            f"Empty DataFrame — nothing to insert into {schema_name}.{table_name}",
            level="warning"
        )
        return

    full_table = f"{schema_name}.{table_name}"
    staging_table = f"{table_name}_stg_{uuid.uuid4().hex[:8]}"

    buffer = io.StringIO()
    df.write_csv(buffer)
    buffer.seek(0)

    try:
        with conn.cursor() as cur:

            # -------------------------
            # 1. Create staging table
            # -------------------------
            cur.execute(f"""
                CREATE TEMP TABLE {staging_table}
                (LIKE {full_table} INCLUDING ALL)
            """)

            # -------------------------
            # 2. COPY into staging
            # -------------------------
            cur.copy_expert(
                f"COPY {staging_table} FROM STDIN WITH CSV HEADER",
                buffer
            )

            columns = df.columns
            col_list = ", ".join(columns)

            update_clause = ", ".join([
                f"{col} = EXCLUDED.{col}"
                for col in columns if col != "id"
            ])

            # -------------------------
            # 3. UPSERT
            # -------------------------
            cur.execute(f"""
                INSERT INTO {full_table} ({col_list})
                SELECT {col_list} FROM {staging_table}
                ON CONFLICT (id)
                DO UPDATE SET {update_clause}
            """)

            # -------------------------
            # 4. SOFT DELETE (missing rows)
            # -------------------------
            cur.execute(f"""
                UPDATE {full_table}
                SET is_deleted = TRUE
                WHERE id NOT IN (
                    SELECT id FROM {staging_table}
                )
                AND is_deleted = FALSE
            """)

        conn.commit()

        print_log(
            f"Upserted + soft-deleted in {full_table} | rows: {len(df)}"
        )

    except Exception as e:
        conn.rollback()
        print_log(
            f"Failed load into {full_table}: {str(e)}",
            level="error"
        )
        raise

# -------------------------------
# LOGGING
# -------------------------------
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    return logging.getLogger(__name__)


def print_log(message: str, level: str = "info", logger: logging.Logger = None):
    _logger = logger or logging.getLogger(__name__)

    levels = {
        "info": _logger.info,
        "warning": _logger.warning,
        "error": _logger.error,
        "debug": _logger.debug
    }

    log_fn = levels.get(level.lower(), _logger.info)
    log_fn(message)