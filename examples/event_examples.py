from imply_sdk import ImplyAuthenticator, ImplyEventApi


# Authenticated with Imply Polaris

auth = ImplyAuthenticator(
    org_name="<your orgname here>",
    client_id="<your client id here>",
    client_secret="<your client secret here"
)
auth.authenticate()  # get and set the bearer access token

data = {
    "__time": "2022-01-01T00:00:00Z",
    "state": "CA"
}
event_api = ImplyEventApi(auth=auth)
response = event_api.push(message=data)




