"""
Schema Introspector — Loads CSV data into SQLite.
Builds column metadata from data + JSON catalog.
"""
import csv
import io
import json
import os
import sqlite3
import logging
from dataclasses import dataclass, field

logger = logging.getLogger("nl2sql.introspector")


@dataclass
class ColumnInfo:
    name: str
    dtype: str = "TEXT"
    is_pk: bool = False
    fk_ref: str = ""
    sample_values: list = field(default_factory=list)


@dataclass
class SchemaMetadata:
    tables: list = field(default_factory=list)
    columns: dict = field(default_factory=dict)  # table -> [ColumnInfo]
    row_counts: dict = field(default_factory=dict)
    relationships: list = field(default_factory=list)
    column_profiles: dict = field(default_factory=dict)  # col_name -> {min, max, mean, samples}
    db_connection: sqlite3.Connection = None


class SchemaIntrospector:
    """Load CSVs into in-memory SQLite and introspect schema."""

    def __init__(self):
        self.metadata = SchemaMetadata()

    def load_from_csvs(self, csv_files: dict[str, bytes]) -> SchemaMetadata:
        """
        Load CSV files into SQLite.
        
        Args:
            csv_files: dict of {filename: bytes_content}
        Returns:
            SchemaMetadata
        """
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row

        tables = []
        columns = {}
        row_counts = {}

        for filename, content in csv_files.items():
            table_name = os.path.splitext(filename.split("/")[-1])[0]
            table_name = table_name.replace("-", "_").replace(" ", "_").lower()

            try:
                text = content.decode("utf-8", errors="replace")
                reader = csv.DictReader(io.StringIO(text))
                field_names = reader.fieldnames or []

                if not field_names:
                    logger.warning(f"No columns in {filename}")
                    continue

                # Read all rows
                rows = list(reader)
                if not rows:
                    logger.warning(f"No data rows in {filename}")
                    continue

                # Infer types from data
                col_types = {}
                for col in field_names:
                    col_types[col] = self._infer_type(col, rows)

                # Create table
                col_defs = []
                for col in field_names:
                    safe_col = col.replace(" ", "_").replace("-", "_")
                    col_defs.append(f'"{safe_col}" {col_types[col]}')

                create_sql = f'CREATE TABLE "{table_name}" ({", ".join(col_defs)})'
                conn.execute(create_sql)

                # Insert rows
                placeholders = ", ".join(["?"] * len(field_names))
                safe_cols = [c.replace(" ", "_").replace("-", "_") for c in field_names]
                insert_sql = f'INSERT INTO "{table_name}" ({", ".join(f"{c}" for c in safe_cols)}) VALUES ({placeholders})'

                for row in rows:
                    values = [row.get(col) for col in field_names]
                    conn.execute(insert_sql, values)

                conn.commit()

                # Build metadata
                tables.append(table_name)
                row_counts[table_name] = len(rows)

                col_infos = []
                for col in field_names:
                    safe_col = col.replace(" ", "_").replace("-", "_")
                    samples = list(set(
                        str(row.get(col, ""))[:50]
                        for row in rows[:20]
                        if row.get(col)
                    ))[:5]
                    col_infos.append(ColumnInfo(
                        name=safe_col,
                        dtype=col_types[col],
                        is_pk=(safe_col.lower() == "id"),
                        sample_values=samples,
                    ))

                columns[table_name] = col_infos
                logger.info(f"Loaded table '{table_name}': {len(rows)} rows, {len(field_names)} columns")

            except Exception as e:
                logger.error(f"Error loading {filename}: {e}")
                continue

        # Compute column profiles from live data
        col_profiles = {}
        for table in tables:
            col_profiles.update(self._compute_column_profiles(conn, table, columns.get(table, [])))

        self.metadata = SchemaMetadata(
            tables=tables,
            columns=columns,
            row_counts=row_counts,
            column_profiles=col_profiles,
            db_connection=conn,
        )

        return self.metadata

    def _infer_type(self, col_name: str, rows: list[dict]) -> str:
        """Infer SQLite type from column data."""
        samples = [row.get(col_name) for row in rows[:100] if row.get(col_name)]
        if not samples:
            return "TEXT"

        int_count = 0
        float_count = 0

        for val in samples:
            val_str = str(val).strip()
            try:
                int(val_str)
                int_count += 1
                continue
            except ValueError:
                pass
            try:
                float(val_str)
                float_count += 1
            except ValueError:
                pass

        total_valid = len(samples)
        if int_count > total_valid * 0.8:
            return "INTEGER"
        if (int_count + float_count) > total_valid * 0.8:
            return "REAL"
        return "TEXT"

    def _compute_column_profiles(self, conn, table: str, col_infos: list) -> dict:
        """Compute min/max/mean/top_values for each column from live PostgreSQL data."""
        profiles = {}
        try:
            with conn.cursor() as cursor:
                for ci in col_infos:
                    name = ci.name
                    try:
                        if ci.dtype in ('REAL', 'INTEGER'):
                            cursor.execute(
                                f'SELECT MIN("{name}") as mn, MAX("{name}") as mx, '
                                f'ROUND(AVG(CAST("{name}" AS numeric)),4) as av FROM "{table}" '
                                f'WHERE "{name}" IS NOT NULL'
                            )
                            row = cursor.fetchone()
                            cursor.execute(
                                f'SELECT DISTINCT "{name}" FROM "{table}" WHERE "{name}" IS NOT NULL '
                                f'ORDER BY RANDOM() LIMIT 5'
                            )
                            samples_raw = cursor.fetchall()
                            profiles[name] = {
                                'min': row[0], 'max': row[1], 'mean': float(row[2]) if row[2] else None,
                                'type': ci.dtype,
                                'samples': [str(s[0]) for s in samples_raw],
                            }
                        else:
                            cursor.execute(
                                f'SELECT "{name}", COUNT(*) as cnt FROM "{table}" '
                                f'WHERE "{name}" IS NOT NULL GROUP BY "{name}" '
                                f'ORDER BY cnt DESC LIMIT 8'
                            )
                            top_vals = cursor.fetchall()
                            profiles[name] = {
                                'type': ci.dtype,
                                'top_values': [str(v[0]) for v in top_vals],
                                'distinct_count': len(top_vals),
                            }
                    except Exception as e:
                        # Rollback on individual column failure
                        conn.rollback()
                        logger.debug(f"Profile error for {name}: {e}")
        except Exception as e:
            logger.error(f"Failed to profile table {table}: {e}")
            
        logger.info(f"Computed profiles for {len(profiles)} columns in '{table}'")
        return profiles

    def get_sample_rows(self, table_name: str, n: int = 3) -> list[dict]:
        """Fetch N random sample rows from the table."""
        if not getattr(self.metadata, 'db_connection', None):
            return []
        try:
            from psycopg2.extras import RealDictCursor
            with self.metadata.db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f'SELECT * FROM "{table_name}" ORDER BY RANDOM() LIMIT {n}')
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception:
            return []

    def get_schema_text(self, table_names: list = None) -> str:
        """Get CREATE TABLE DDL text for specified tables."""
        tables = table_names or self.metadata.tables
        parts = []
        for table in tables:
            cols = self.metadata.columns.get(table, [])
            col_defs = [f"  {c.name} {c.dtype}" for c in cols]
            parts.append(f'CREATE TABLE "{table}" (\n' + ",\n".join(col_defs) + "\n);")
        return "\n\n".join(parts)
