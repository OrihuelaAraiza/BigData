import json
import os
from io import BytesIO

import boto3
import fastavro
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


def ensure_bucket_exists(s3_client, bucket_name: str) -> None:
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise


def upload_bytes(s3_client, bucket_name: str, key: str, data: bytes) -> None:
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
    print(f"Uploaded {key} ({len(data)} bytes)")


def build_s3_client():
    access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    endpoints = []
    endpoint_from_env = os.getenv("MINIO_ENDPOINT")
    if endpoint_from_env:
        endpoints.append(endpoint_from_env)
    endpoints.extend(["http://minio:9000", "http://localhost:9000"])

    last_error = None
    for endpoint in endpoints:
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        try:
            s3_client.list_buckets()
            print(f"Connected to MinIO at {endpoint}")
            return s3_client
        except Exception as exc:
            last_error = exc

    raise RuntimeError(
        f"No se pudo conectar a MinIO. Endpoints probados: {endpoints}"
    ) from last_error


def main() -> None:
    # Datos tabulares comunes (usuarios)
    users_df = pd.DataFrame(
        {
            "user_id": [1001, 1002, 1003],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
            "signup_date": pd.to_datetime(["2023-01-01", "2023-02-15", "2023-03-20"]),
        }
    )

    # Cliente MinIO (funciona dentro y fuera de Docker)
    s3 = build_s3_client()

    bucket = "datalake"
    ensure_bucket_exists(s3, bucket)

    # CSV
    csv_buffer = BytesIO()
    users_df.to_csv(csv_buffer, index=False)
    upload_bytes(s3, bucket, "users.csv", csv_buffer.getvalue())

    # Parquet
    table = pa.Table.from_pandas(users_df, preserve_index=False)
    parquet_buffer = BytesIO()
    pq.write_table(table, parquet_buffer)
    upload_bytes(s3, bucket, "users.parquet", parquet_buffer.getvalue())

    # Avro
    records = users_df.to_dict(orient="records")
    for row in records:
        row["signup_date"] = row["signup_date"].isoformat()

    schema = {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "user_id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "signup_date", "type": "string"},
        ],
    }

    avro_buffer = BytesIO()
    fastavro.writer(avro_buffer, schema, records)
    upload_bytes(s3, bucket, "users.avro", avro_buffer.getvalue())

    # JSON
    json_data = {
        "users": records,
        "metadata": {
            "source": "user_signup_system",
            "generated_at": pd.Timestamp.now().isoformat(),
        },
    }
    json_buffer = json.dumps(json_data, indent=2).encode("utf-8")
    upload_bytes(s3, bucket, "users.json", json_buffer)

    # Segundo ejemplo Parquet usando pandas
    data_df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob", "Charlie"],
            "Age": [25, 30, 35],
            "City": ["New York", "Los Angeles", "Chicago"],
        }
    )

    local_file = "data.parquet"
    data_df.to_parquet(local_file, engine="pyarrow", index=False)
    print(f"Parquet local escrito: {local_file}")

    # Subir archivo local a MinIO (corrige el bug del buffer vacío)
    with open(local_file, "rb") as fp:
        upload_bytes(s3, bucket, "data.parquet", fp.read())

    # Leer archivo local e imprimir
    read_back_df = pq.read_table(local_file).to_pandas()
    print("Data from Parquet file:")
    print(read_back_df)


if __name__ == "__main__":
    main()
