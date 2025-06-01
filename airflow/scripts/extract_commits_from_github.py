import os
import requests
from dotenv import load_dotenv
from datetime import date
from typing import Any, Dict, List, Optional
from utils import TrinoClient

load_dotenv()

GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN")
GITHUB_API_URL = "https://api.github.com"


HEADERS: Dict[str, str] = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

def fetch_commits_for_repo(
    owner_repo: str,
    limit: int = 10,
    since: Optional[str] = None,
    until: Optional[str] = None
) -> List[Dict[str, Any]]:

    url = f"{GITHUB_API_URL}/repos/{owner_repo}/commits"
    params = {
        "per_page": limit
    }
    if since:
        params["since"] = since
    if until:
        params["until"] = until

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    commits_raw = response.json()
    return commits_raw

def fetch_repo_names_from_table(client: TrinoClient, ingestion_date: str) -> List[str]:
    query = f"""
        SELECT id
        FROM iceberg.landing.repositories
        WHERE ingestion_date = DATE '{ingestion_date}'
    """
    df = client.read_sql(query)
    return df["id"].tolist()

if __name__ == "__main__":
    client = TrinoClient()
    today_str = date.today().isoformat()  # Parameter can be controlled by Airflow or environment

    repo_names = fetch_repo_names_from_table(client, ingestion_date=today_str)

    for repo in repo_names:
        print(f"\nFetching commits for: {repo}")
        try:
            commits = fetch_commits_for_repo(repo)
            if commits:
                owner, repo_name = repo.split("/")  # Split "owner/repo" string
                for commit in commits:
                    commit["owner"] = owner
                    commit["repo"] = repo_name

                client.insert_raw_payloads(
                    table_name="iceberg.landing.commits",
                    rows=commits,
                    id_field="sha"
                )
                print(f"Inserted {len(commits)} commits for repo {repo}.")

            else:
                print(f"No commits found for repo {repo}.")
        except Exception as e:
            print(f"Failed to fetch commits for {repo}: {e}")
