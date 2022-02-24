import requests
from requests import Response


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


class TableRequest:

    def get(self):
        return {}


class ImplyTableApi:

    def __init__(self, auth: ImplyAuthenticator):
        self.auth = auth
        self.TABLE_ENDPOINT = "https://api.imply.io/v1/tables"

    def create(self, table_request: TableRequest) -> Response:
        response = requests.post(url=self.TABLE_ENDPOINT, data=table_request.get(), headers=self.auth.get_headers())
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

    def get(self, table_id: uuid, detail: str = "summary") -> Response:
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

        response = requests.get(url=self.TABLE_ENDPOINT, params=params)
        return response

    def update(self, table_id: str, table_request: TableRequest) -> Response:
        url = f"{self.TABLE_ENDPOINT}/{table_id}"
        response = requests.put(url=url, data=table_request.get(), headers=self.auth.get_headers())
        return response


class ImplyPerformanceApi:

    def __init__(self, auth: ImplyAuthenticator):
        self.auth = auth
        self.INGESTION_ENDPOINT = "https://api.imply.io/v1/performance"

    def get_storage(self) -> Response:
        url = f"{self.INGESTION_ENDPOINT}/storage"
        response = requests.post(url=self.INGESTION_ENDPOINT, headers=self.auth.get_headers())
        return response


class ImplyIngestionApi:

    def __init__(self, auth: ImplyAuthenticator):
        self.auth = auth
        self.INGESTION_ENDPOINT = "https://api.imply.io/v1/ingestionJobs"

    def list(self) -> Response:
        response = requests.post(url=self.INGESTION_ENDPOINT, headers=self.auth.get_headers())
        return response

class ImplyEventApi:

    def __init__(self, auth: ImplyAuthenticator, table_id=None):
        self.auth = auth
        self.TABLE_ID = table_id
        self.EVENTS_ENDPOINT = f"https://api.imply.io/v1/events/{table_id}"

    def push(self, message: dict) -> Response:
        response = requests.post(url=self.EVENTS_ENDPOINT, json=message, headers=self.auth.get_headers())
        return response



class ImplyFileApi:

    def __init__(self, auth: ImplyAuthenticator):
        self.FILE_ENDPOINT = "https://api.imply.io/v1/files"
        self.auth = auth

    def upload(self, filename: str) -> Response:
        file = {"file": open(filename, "rb")}
        headers = self.auth.get_headers()
        response = requests.post(self.FILE_ENDPOINT, headers=headers, files=file)
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