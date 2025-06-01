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
        regexp_extract(
            id,
        '\\.([a-zA-Z0-9]+)$',
        1
        ) AS file_format;
    FROM iceberg.landing.commit_files
)
GROUP BY repo_id, commit_sha    