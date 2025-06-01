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
	  ) AS file_format
FROM iceberg.landing.commit_files


