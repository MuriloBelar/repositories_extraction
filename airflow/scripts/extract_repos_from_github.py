import os
import requests
from dotenv import load_dotenv
from datetime import date
from typing import Any, Dict, List, Optional
from utils import TrinoClient

load_dotenv()

GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN")
GITHUB_API_URL = "https://api.github.com"
LIMIT: int = int(os.getenv("LIMIT"))

HEADERS: Dict[str, str] = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}


def discover_repositories_with_full_metadata(
    limit: int = 10,
    created_after: Optional[date] = None,
    pushed_after: Optional[date] = None
) -> List[Dict[str, Any]]:
    url = f"{GITHUB_API_URL}/search/repositories"
    query = "stars:>1000"

    if created_after:
        query += f" created:>{created_after.isoformat()}"
    if pushed_after:
        query += f" pushed:>{pushed_after.isoformat()}"

    items: List[Dict[str, Any]] = []
    per_page = 100
    page = 1

    while len(items) < limit:
        remaining = limit - len(items)
        params = {
            "q": query,
            "sort": "stars",
            "order": "desc",
            "per_page": min(per_page, remaining),
            "page": page
        }

        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()
        batch = response.json().get("items", [])

        if not batch:
            break
        
        items.extend(batch)
        page += 1
        if page == 11: ## Rate Limit in the api
            break

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
