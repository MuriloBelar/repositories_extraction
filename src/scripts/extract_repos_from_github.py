import os
import requests
from dotenv import load_dotenv
from datetime import date
from typing import Any, Dict, List, Optional
from utils import TrinoClient

load_dotenv()

GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN")
LIMIT: int = os.getenv("LIMIT")

HEADERS: Dict[str, str] = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

def discover_repositories_with_full_metadata(
    limit: int = 10,
    created_after: Optional[date] = None, ##Can be controlled by Airflow
    pushed_after: Optional[date] = None
) -> List[Dict[str, Any]]:
    url = "https://api.github.com/search/repositories"

    query = "stars:>1000"
    if created_after:
        query += f" created:>{created_after.isoformat()}"
    if pushed_after:
        query += f" pushed:>{pushed_after.isoformat()}"

    params = {
        "q": query,
        "sort": "stars",
        "order": "desc",
        "per_page": limit
    }

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    items = response.json().get("items", [])
    
    return items

if __name__ == "__main__":
    client = TrinoClient()

    repos: List[Dict[str, Any]] = discover_repositories_with_full_metadata(limit=LIMIT)

    # Insert raw payload, using "full_name" as the ID field
    client.insert_raw_payloads(
        table_name="iceberg.landing.repositories",
        rows=repos,
        id_field="full_name"
    )

    print(f"Retrieved and inserted {len(repos)} repositories with full metadata (raw schema).\n")
