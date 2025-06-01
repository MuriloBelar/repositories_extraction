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
