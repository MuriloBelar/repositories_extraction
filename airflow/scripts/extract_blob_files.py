import os
import asyncio
import aiohttp
from dotenv import load_dotenv
from datetime import date
from typing import Optional, List, Dict
from utils import TrinoClient, MinioClient
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN")
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3.raw"
}


async def fetch_file_content(
    session: aiohttp.ClientSession,
    file_metadata: Dict[str, str]
) -> Optional[bytes]:

    owner_repo = file_metadata["repo_id"]
    commit_sha = file_metadata["commit_sha"]
    file_path = file_metadata["file_path"]

    url = f"https://api.github.com/repos/{owner_repo}/contents/{file_path}"
    params = {"ref": commit_sha}

    print(f"Fetching file '{file_path}' at commit '{commit_sha}' from repo '{owner_repo}'...")

    async with session.get(url, params=params) as response:
        if response.status != 200:
            print(f"Failed to fetch file '{file_path}' at commit '{commit_sha}' in repo '{owner_repo}': {response.status}")
            return None
        content = await response.read()
        return content


def fetch_files_from_trino(client: TrinoClient, ingestion_date: str) -> List[Dict[str, str]]:
    query = f"""
        SELECT 
            id AS object_name,
            file_path,
            commit_sha,
            repo_id
        FROM iceberg.curated.commit_files
        WHERE ingestion_date = DATE'{ingestion_date}'
        """
    file_list = client.read_sql(query).to_dict(orient="records")
    return file_list


async def upload_file(
    minio_client: MinioClient,
    bucket_name: str,
    object_name: str,
    data_bytes: bytes,
    executor: ThreadPoolExecutor
):
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(
            executor,
            minio_client.upload_bytes,
            bucket_name,
            object_name,
            data_bytes,
            "application/octet-stream"
        )
        print(f"Uploaded '{object_name}' to MinIO bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Failed to upload '{object_name}' to MinIO: {e}")


async def process_file(
    session: aiohttp.ClientSession,
    minio_client: MinioClient,
    bucket_name: str,
    file_metadata: Dict[str, str],
    executor: ThreadPoolExecutor
):
    content = await fetch_file_content(session, file_metadata)
    if content:
        safe_file_path = file_metadata["file_path"].replace("/", "_")
        object_name = f"{file_metadata['repo_id'].replace('/', '_')}/{file_metadata['commit_sha']}/{safe_file_path}"
        await upload_file(minio_client, bucket_name, object_name, content, executor)
    else:
        print(f"Skipping upload for '{file_metadata['object_name']}' due to fetch failure.")


async def main():
    trino_client = TrinoClient()
    minio_client = MinioClient()
    today_str = date.today().isoformat()
    bucket_name = "repositories"

    file_list = fetch_files_from_trino(trino_client, today_str)
    print(f"Found {len(file_list)} files to process...")

    connector = aiohttp.TCPConnector(limit=20)  # Limit concurrency of HTTP connections
    timeout = aiohttp.ClientTimeout(total=60)

    executor = ThreadPoolExecutor(max_workers=10)  # For MinIO uploads (if blocking)

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector, timeout=timeout) as session:
        tasks = [
            process_file(session, minio_client, bucket_name, file_meta, executor)
            for file_meta in file_list
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
