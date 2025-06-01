import os
import requests
from dotenv import load_dotenv
from datetime import date
from typing import Any, Dict, List
from utils import TrinoClient

load_dotenv()

GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN")
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

def fetch_commit_files_for_commit(
    owner_repo: str,
    commit_sha: str
) -> List[Dict[str, Any]]:
    """
    Fetch files changed in a specific commit, keeping full file info nested in raw_payload.
    """
    url = f"https://api.github.com/repos/{owner_repo}/commits/{commit_sha}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    commit_data = response.json()
    files = commit_data.get("files", [])
    ingestion_date = date.today().isoformat()

    records = []
    for f in files:
        f["repo_id"] = owner_repo
        f["commit_sha"] = commit_sha
        f["ingestion_date"] = ingestion_date
        f["id"] = f"{commit_sha}_{f.get('filename').replace('/', '_')}"
        records.append(f)
    return records


def fetch_commits_from_trino(client: TrinoClient, ingestion_date: str) -> List[Dict[str, Any]]:
    query = f"""
        SELECT 
            id AS sha,
            CONCAT(
                CAST(json_extract_scalar(raw_payload, '$.owner') AS varchar), 
                '/', 
                CAST(json_extract_scalar(raw_payload, '$.repo') AS varchar)
            ) AS repo_id
            FROM iceberg.landing.commits
            WHERE ingestion_date = DATE '{ingestion_date}'
    """
    df = client.read_sql(query)
    return df.to_dict(orient="records")


if __name__ == "__main__":
    client = TrinoClient()
    today_str = date.today().isoformat()

    commits = fetch_commits_from_trino(client, ingestion_date=today_str)
    print(f"Found {len(commits)} commits to process...")

    for commit in commits:
        owner_repo = commit["repo_id"]
        sha = commit["sha"]
        print(f"Fetching files for commit {sha} in {owner_repo}")
        try:
            files = fetch_commit_files_for_commit(owner_repo, sha)
            if files:
                client.insert_raw_payloads(
                    table_name="iceberg.landing.commit_files",
                    rows=files,
                    id_field="id"
                )
                print(f"Inserted {len(files)} files for commit {sha}")
            else:
                print(f"No files found for commit {sha}")
        except Exception as e:
            print(f"Failed to fetch or insert files for commit {sha}: {e}")
