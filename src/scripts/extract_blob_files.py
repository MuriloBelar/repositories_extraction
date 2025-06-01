import os
import requests
from dotenv import load_dotenv
from datetime import date
from typing import Optional, List
from utils import TrinoClient, MinioClient

load_dotenv()

GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN")
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3.raw"
}



import requests

def fetch_file_content(file_metadata: dict) -> Optional[bytes]:

    owner_repo = file_metadata["repo_id"]
    commit_sha = file_metadata["commit_sha"]
    file_path = file_metadata["file_path"]
    
    url = f"https://api.github.com/repos/{owner_repo}/contents/{file_path}"
    params = {"ref": commit_sha}

    print(f"Fetching file '{file_path}' at commit '{commit_sha}' from repo '{owner_repo}'...")

    response = requests.get(url, headers=HEADERS, params=params)
    try:
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch file '{file_path}' at commit '{commit_sha}' in repo '{owner_repo}': {e}")
        return None

def fetch_files_from_trino(client: TrinoClient, ingestion_date: str)-> List[str]:
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

trino_client = TrinoClient()
minio_client = MinioClient()
today_str = date.today().isoformat()
bucket_name = "repositories"

file_list = fetch_files_from_trino(trino_client, today_str)
for file_meta_data in file_list:
    content = fetch_file_content(file_meta_data)
    if content:
        safe_file_path = file_meta_data["file_path"].replace("/", "_")
        object_name = f"{file_meta_data['repo_id'].replace('/', '_')}/{file_meta_data['commit_sha']}/{safe_file_path}"

        try:
            minio_client.upload_bytes(
                bucket_name=bucket_name,
                object_name=object_name,
                data_bytes=content,
                content_type="application/octet-stream"
            )
            print(f"Uploaded '{object_name}' to MinIO bucket '{bucket_name}'.")
        except Exception as e:
            print(f"Failed to upload '{object_name}' to MinIO: {e}")
    else:
        print(f"Skipping upload for '{file_meta_data['object_name']}' due to fetch failure.")