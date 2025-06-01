from trino.dbapi import connect
import pandas as pd
from datetime import date
from typing import List, Dict, Any
import json
from minio import Minio
from io import BytesIO
import tarfile
import os

class MinioClient:
    def __init__(
        self,
        endpoint: str = "storage:9000",
        access_key: str = "admin",
        secret_key: str = "password",
        secure: bool = False
    ):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )

    def ensure_bucket(self, bucket_name: str):
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)

    def upload_bytes(
        self,
        bucket_name: str,
        object_name: str,
        data_bytes: bytes,
        content_type: str = "application/gzip"
    ):
        self.ensure_bucket(bucket_name)
        data_stream = BytesIO(data_bytes)
        data_length = len(data_bytes)
        self.client.put_object(
            bucket_name,
            object_name,
            data_stream,
            data_length,
            content_type=content_type
        )
    
def format_value(val):
    if val is None:
        return "NULL"
    val_str = str(val).replace("'", "''")
    return f"'{val_str}'"

class TrinoClient:
    def __init__(
        self,
        host: str = "trino",
        port: int = 8080,
        user: str = "admin",
        catalog: str = "iceberg",
        schema: str = "landing"
    ):
        self.conn = connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema
        )

    def read_table(self, table_name: str) -> pd.DataFrame:
        cursor = self.conn.cursor()
        cursor.execute(f'SELECT * FROM {table_name}')
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)

    def read_sql(self, query: str) -> pd.DataFrame:
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    
    def insert_raw_payloads(self, table_name: str, rows: List[Dict[str, Any]], id_field: str):
        if not rows:
            return
        
        ingestion_date = date.today().isoformat()
        values_sql = []
        for row in rows:
            record_id = row.get(id_field)
            raw_json = json.dumps(row).replace("'", "''")
            values_sql.append(
                f"({format_value(record_id)}, DATE '{ingestion_date}', '{raw_json}')"
            )
        
        values_sql_str = ",\n".join(values_sql)
        insert_sql = (
            f"INSERT INTO {table_name} (id, ingestion_date, raw_payload) VALUES \n{values_sql_str}"
        )
        
        cursor = self.conn.cursor()
        cursor.execute(insert_sql)
    def execute_query(self, query: str) -> None:
        cursor = self.conn.cursor()
        cursor.execute(query)

    def execute_sql_file(self, filepath: str) -> None:
        with open(filepath, 'r', encoding='utf-8') as file:
            sql_script = file.read()
        
        cursor = self.conn.cursor()
        cursor.execute(sql_script)
