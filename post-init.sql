CREATE SCHEMA iceberg.landing WITH (location = 's3://iceberg/landing');
CREATE SCHEMA iceberg.curated WITH (location = 's3://iceberg/curated');
CREATE TABLE iceberg.landing.repositories (
    id VARCHAR,
    ingestion_date DATE,
    raw_payload VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ingestion_date']
);

CREATE TABLE iceberg.landing.commits (
    id VARCHAR,
    ingestion_date DATE,
    raw_payload VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ingestion_date']
);
CREATE TABLE iceberg.landing.commit_files (
    id VARCHAR,
    ingestion_date DATE,
    raw_payload VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ingestion_date']
);
