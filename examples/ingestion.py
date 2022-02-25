from imply_sdk import ImplyAuthenticator, ImplyIngestionApi


# Authenticated with Imply Polaris

auth = ImplyAuthenticator(
    org_name="<your orgname here>",
    client_id="<your client id here>",
    client_secret="<your client secret here"
)
auth.authenticate()  # get and set the bearer access token

# list all ingestion tasks
ingestion_api = ImplyIngestionApi(auth=auth)
response = ingestion_api.list()




