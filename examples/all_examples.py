from imply_sdk import ImplyAuthenticator, ImplyTableApi


# Authenticated with Imply Polaris

auth = ImplyAuthenticator(
    org_name="<your orgname here>",
    client_id="<your client id here>",
    client_secret="<your client secret here"
)
auth.authenticate()  # get and set the bearer access token


# List tables
table_api = ImplyTableApi(auth=auth)
results = table_api.list()
