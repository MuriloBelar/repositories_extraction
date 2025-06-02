import os
import requests
from dotenv import load_dotenv
from datetime import date
from typing import Any, Dict, List
from utils import TrinoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def process_commit(client: TrinoClient, commit: Dict[str, str]) -> None:
    owner_repo = commit["repo_id"]
    sha = commit["sha"]
    print(f"Fetching rows for commit details {sha} in {owner_repo}")

    try:
        files = fetch_commit_files_for_commit(owner_repo, sha)
    except Exception as e:
        print(f"Failed to fetch rows for commit details {sha}: {e}")
        return

    if not files:
        print(f"No files found for commit {sha}")
        return

    try:
        client.insert_raw_payloads(
            table_name="iceberg.landing.commit_files",
            rows=files,
            id_field="id"
        )
        print(f"Inserted {len(files)} rows for commit details {sha}")
    except Exception as e:
        print(f"Failed to insert rows for commit details {sha}: {e}")





if __name__ == "__main__":
    client = TrinoClient()
    today_str = date.today().isoformat()
    commits = fetch_commits_from_trino(client, ingestion_date=today_str)
    print(f"Found {len(commits)} commits to process...")

    max_workers = 5  
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_commit, client, commit) for commit in commits]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Unhandled exception in thread: {e}")