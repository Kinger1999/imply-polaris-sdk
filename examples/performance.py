from imply_sdk import ImplyAuthenticator, ImplyPerformanceApi


# Authenticated with Imply Polaris

auth = ImplyAuthenticator(
    org_name="<your orgname here>",
    client_id="<your client id here>",
    client_secret="<your client secret here"
)
auth.authenticate()  # get and set the bearer access token

performance_api = ImplyPerformanceApi(auth=auth)
response = performance_api.get_storage()



