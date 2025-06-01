from utils import TrinoClient

query_create_curated_commit_files = """
        CREATE TABLE iceberg.curated.commit_files AS

        SELECT
            id,
            ingestion_date,
            json_extract_scalar(json_parse(raw_payload), '$.repo_id') AS repo_id,
            json_extract_scalar(json_parse(raw_payload), '$.commit_sha') AS commit_sha,
            json_extract_scalar(json_parse(raw_payload), '$.sha') AS blob_sha,
            json_extract_scalar(json_parse(raw_payload), '$.filename') AS file_path,
            json_extract_scalar(json_parse(raw_payload), '$.status') AS status,
            CAST(json_extract_scalar(json_parse(raw_payload), '$.additions') AS INTEGER) AS lines_added,
            CAST(json_extract_scalar(json_parse(raw_payload), '$.deletions') AS INTEGER) AS lines_removed,
            CAST(json_extract_scalar(json_parse(raw_payload), '$.changes') AS INTEGER) AS total_changes,
            SUBSTR(
                id,
                STRPOS(id, '.') + 1
            ) AS file_format,
            CONCAT(
                'repositories/',
                json_extract_scalar(json_parse(raw_payload), '$.repo_id'), '/',
                json_extract_scalar(json_parse(raw_payload), '$.commit_sha'), '/',
                json_extract_scalar(json_parse(raw_payload), '$.filename')
            ) AS s3_path
        FROM iceberg.landing.commit_files
    """

query_create_curated_commit_change_metrics = """
        CREATE TABLE iceberg.curated.commit_change_metrics AS
            SELECT
                repo_id,
                commit_sha,
                COUNT(*) AS number_of_files_changed,
                SUM(lines_added + lines_removed) AS total_lines_changed,
                ARRAY_DISTINCT(ARRAY_AGG(file_format)) AS file_types_changed
            FROM (
                SELECT
                    json_extract_scalar(json_parse(raw_payload), '$.repo_id') AS repo_id,
                    json_extract_scalar(json_parse(raw_payload), '$.commit_sha') AS commit_sha,
                    CAST(json_extract_scalar(json_parse(raw_payload), '$.additions') AS INTEGER) AS lines_added,
                    CAST(json_extract_scalar(json_parse(raw_payload), '$.deletions') AS INTEGER) AS lines_removed,
                    SUBSTR(
                        id,
                        STRPOS(id, '.') + 1
                    ) AS file_format
                FROM iceberg.landing.commit_files
            )
            GROUP BY repo_id, commit_sha
    """
query_create_curated_commit = """
        CREATE TABLE iceberg.curated.commits AS
            SELECT
                CAST(json_extract_scalar(raw_payload, '$.sha') AS VARCHAR) AS commit_sha,
                json_extract_scalar(raw_payload, '$.commit.author.name') AS author_name,
                json_extract_scalar(raw_payload, '$.commit.author.email') AS author_email,
                json_extract_scalar(raw_payload, '$.commit.message') AS message,
                json_extract_scalar(raw_payload, '$.commit.author.date') AS timestamp,
                json_extract_scalar(raw_payload, '$.owner') AS owner,
                json_extract_scalar(raw_payload, '$.repo') AS repo,
                json_extract_scalar(raw_payload, '$.commit.message') AS message,
                json_extract_scalar(raw_payload, '$.parents[0].sha') AS parent_sha,
                ingestion_date
            FROM iceberg.landing.commits

    """
trino_client = TrinoClient()

trino_client.execute_query("DROP TABLE IF EXISTS iceberg.curated.commit_files") 
trino_client.execute_query("DROP TABLE IF EXISTS iceberg.curated.commit_change_metrics") 
trino_client.execute_query("DROP TABLE IF EXISTS iceberg.curated.commits") 

trino_client.execute_query(query_create_curated_commit_files)
trino_client.execute_query(query_create_curated_commit_change_metrics)
trino_client.execute_query(query_create_curated_commit)