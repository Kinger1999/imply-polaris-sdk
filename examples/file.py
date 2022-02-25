from imply_sdk import ImplyAuthenticator, ImplyFileApi


# Authenticated with Imply Polaris

auth = ImplyAuthenticator(
    org_name="<your orgname here>",
    client_id="<your client id here>",
    client_secret="<your client secret here"
)
auth.authenticate()  # get and set the bearer access token


# create and upload a file
filename = "data.json"
file_api = ImplyFileApi(auth=auth)
response = file_api.upload(filename=filename)

# list files
filename = "data.json"
file_api = ImplyFileApi(auth=auth)
response = file_api.list(limit=100, pattern="data.*", token="")

# get file metadata
filename = "data.json"
file_api = ImplyFileApi(auth=auth)
response = file_api.get(filename=filename)

# delete file
filename = "data.json"
file_api = ImplyFileApi(auth=auth)
response = file_api.delete(filename=filename)



