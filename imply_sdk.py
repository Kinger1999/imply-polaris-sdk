import numpy as np
import math
import json
import requests
from requests import Response
import concurrent.futures
import time


class ImplyAuthenticator:

    def __init__(self, org_name=None, client_id=None, client_secret=None):
        self.ORG_NAME = org_name

        self.CLIENT_ID = client_id
        self.CLIENT_SECRET = client_secret

        self.access_token = None

        # Imply OAuth API config
        self.TOKEN_ENDPOINT = f"https://id.imply.io/auth/realms/{org_name}/protocol/openid-connect/token"


    def authenticate(self):

        params = {
            "client_id": self.CLIENT_ID,
            "client_secret": self.CLIENT_SECRET,
            "grant_type": "client_credentials",
        }

        response = requests.post(self.TOKEN_ENDPOINT, data=params)
        response.raise_for_status()

        self.access_token = response.json()['access_token']


    def get_access_token(self):
        return self.access_token

    def get_headers(self):
        return {
            "Authorization": "Bearer {token}".format(token=self.access_token),
            'Accept': 'application/json'
        }


class ImplyTableApi:

    def __init__(self, auth: ImplyAuthenticator):
        self.auth = auth
        self.TABLE_ENDPOINT = "https://api.imply.io/v1/tables"

    def create(self, table_request: str = None) -> Response:
        response = requests.post(url=self.TABLE_ENDPOINT, data=table_request, headers=self.auth.get_headers())
        return response

    def delete(self, table_id: str) -> Response:
        url = f"{self.TABLE_ENDPOINT}/{table_id}"
        response = requests.delete(url=url, headers=self.auth.get_headers())
        return response

    def delete_data(self, table_id: str) -> Response:
        url = f"{self.TABLE_ENDPOINT}/{table_id}/data"
        response = requests.delete(url=url, headers=self.auth.get_headers())
        return response

    def delete_data_by_interval(self, table_id: str, start: str, end: str) -> Response:
        data = {
            "interval": f"{start}/{end}"
        }
        url = f"{self.TABLE_ENDPOINT}/{table_id}/data/interval"
        response = requests.delete(url=url, data=data, headers=self.auth.get_headers())
        return response

    def get(self, table_id: str = None, detail: str = "summary") -> Response:
        params = {
            "detail": detail
        }

        table_id = table_id.hex
        url = f"{self.TABLE_ENDPOINT}/{table_id}"
        response = requests.get(url=url, params=params, headers=self.auth.get_headers())
        return response

    def list(self, name: str = None, detail: str = "summary") -> Response:

        params = {
            "detail": detail
        }

        if name is not None:
            params["name"] = name

        response = requests.get(url=self.TABLE_ENDPOINT, params=params, headers=self.auth.get_headers())
        return response

    def update(self, table_id: str, table_request: str = None) -> Response:
        url = f"{self.TABLE_ENDPOINT}/{table_id}"
        response = requests.put(url=url, data=table_request, headers=self.auth.get_headers())
        return response

    def disable_streaming(self, table_id: str) -> Response:
        url = f"{self.TABLE_ENDPOINT}/{table_id}/ingestion/streaming"
        response = requests.delete(url=url, headers=self.auth.get_headers())
        return response

    def enable_streaming(self, table_id: str) -> Response:
        url = f"{self.TABLE_ENDPOINT}/{table_id}/ingestion/streaming"
        response = requests.post(url=url, headers=self.auth.get_headers(), json={})
        return response


class ImplyPerformanceApi:

    def __init__(self, auth: ImplyAuthenticator):
        self.auth = auth
        self.PERFORMANCE_ENDPOINT = "https://api.imply.io/v1/performance"

    def get_storage(self) -> Response:
        url = f"{self.PERFORMANCE_ENDPOINT}/storage"
        response = requests.post(url=self.PERFORMANCE_ENDPOINT, headers=self.auth.get_headers())
        return response


class ImplyIngestionApi:

    def __init__(self, auth: ImplyAuthenticator):
        self.auth = auth
        self.INGESTION_ENDPOINT = "https://api.imply.io/v1/ingestionJobs"

    def list(self) -> Response:
        response = requests.post(url=self.INGESTION_ENDPOINT, headers=self.auth.get_headers())
        return response

class ImplyEventApi:

    def __init__(self, auth: ImplyAuthenticator, table_id: str = None):
        self.auth = auth
        self.TABLE_ID = table_id
        self.EVENTS_ENDPOINT = f"https://api.imply.io/v1/events/{table_id}"
        self.session = requests.Session()

    def push_list(self, messages: list, split_size=750000, threads=5) -> list:

        responses = []
        input_length = len(json.dumps(messages))
        num_splits = math.ceil(input_length/split_size)
        splits = np.array_split(messages, num_splits)
        payloads = []

        for i, split in enumerate(splits):
            payload = ""
            for message in split:
                payload += json.dumps(message)
                payload += "\n"

            payloads.append(payload)

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
            responses = pool.map(self.push, payloads)

        return responses

    def push(self, payload: str) -> Response:
        start = int(time.time())
        response = self.session.post(url=self.EVENTS_ENDPOINT,
                                     data=payload.encode("utf-8"),
                                     headers=self.auth.get_headers())
        end = int(time.time())
        # print(f"Request took {end-start} seconds to push {len(payload)} bytes")
        return response


class ImplyFileApi:

    def __init__(self, auth: ImplyAuthenticator):
        self.auth = auth
        self.FILE_ENDPOINT = "https://api.imply.io/v1/files"

    def upload(self, filename: str) -> Response:
        file = {"file": open(filename, "rb")}
        response = requests.post(self.FILE_ENDPOINT, headers=self.auth.get_headers(), files=file)
        return response

    def delete(self, filename: str) -> Response:
        url = f"{self.FILE_ENDPOINT}/{filename}"
        response = requests.delete(url=url, headers=self.auth.get_headers())
        return response

    def get(self, filename: str) -> Response:
        url = f"{self.FILE_ENDPOINT}/{filename}"
        response = requests.get(url=url, headers=self.auth.get_headers())
        return response

    def list(self, limit=100, token=None, pattern=None) -> Response:
        params = {
            "limit": limit,
            "cont": token,
            "pattern": pattern
        }
        url = self.FILE_ENDPOINT
        response = requests.get(url=url, params=params, headers=self.auth.get_headers())
        return response


class TableIngestionApi:

    def __init__(self, auth: ImplyAuthenticator):
        self.TABLE_INGESTION_ENDPOINT = "https://api.imply.io/v1"
        self.auth = auth

    def launch(self, table_id: str = None, ingestion_spec: str = None) -> Response:
        url = f"{self.TABLE_INGESTION_ENDPOINT}/{table_id}/ingestion/jobs"
        response = requests.post(url=url, json=ingestion_spec, headers=self.auth.get_headers())
        return response

    def get_progress(self, table_id: str = None, job_id: str = None) -> Response:
        params = {
            "simple": False
        }
        url = f"{self.TABLE_INGESTION_ENDPOINT}/{table_id}/ingestion/jobs/{job_id}/progress"
        response = requests.get(url=url, params=params, headers=self.auth.get_headers())
        return response

    def get_info(self, table_id: str = None, job_id: str = None) -> Response:
        url = f"{self.TABLE_INGESTION_ENDPOINT}/{table_id}/ingestion/jobs/{job_id}/description"
        response = requests.get(url=url, headers=self.auth.get_headers())
        return response

    def cancel(self, table_id: str = None, job_id: str = None) -> Response:
        url = f"{self.TABLE_INGESTION_ENDPOINT}/{table_id}/ingestion/jobs/{job_id}/cancel"
        response = requests.POST(url=url, headers=self.auth.get_headers())
        return response

    def list(self, table_id: str = None):
        url = f"{self.TABLE_INGESTION_ENDPOINT}/{table_id}/ingestion/jobs/"
        response = requests.get(url=url, headers=self.auth.get_headers())
        return response

    def list_templates(self, table_id: str = None):
        url = f"{self.TABLE_INGESTION_ENDPOINT}/tables/{table_id}/ingestion/templates"
        response = requests.get(url=url, headers=self.auth.get_headers())
        return response

    def update_template(self, table_id: str = None, template_id: str = None, template_spec: str = None):
        url = f"{self.TABLE_INGESTION_ENDPOINT}/tables/{table_id}/ingestion/templates/{template_id}"
        response = requests.post(url=url, json=template_spec, headers=self.auth.get_headers() )
        return response

    def get_template(self, table_id: str = None, template_id: str = None):
        url = f"{self.TABLE_INGESTION_ENDPOINT}/tables/{table_id}/ingestion/templates/{template_id}"
        response = requests.get(url=url, headers=self.auth.get_headers())
        return response

