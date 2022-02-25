from imply_sdk import ImplyAuthenticator, ImplyTableApi


# Authenticated with Imply Polaris

auth = ImplyAuthenticator(
    org_name="<your orgname here>",
    client_id="<your client id here>",
    client_secret="<your client secret here"
)
auth.authenticate()  # get and set the bearer access token


# Create table
table_def = {
    "name": "blah",
    "inputSchema": [
        {
            "name": "channel",
            "type": "string"
        },
        {
            "name": "cityName",
            "type": "string"
        }
    ]
}
table_api = ImplyTableApi(auth=auth)
response = table_api.create(table_request=table_def)

# List all tables
table_api = ImplyTableApi(auth=auth)
response = table_api.list()

# Delete table
table_id = ""
table_api = ImplyTableApi(auth=auth)
response = table_api.delete(table_id=table_id)

# Drop data in table
table_id = ""
table_api = ImplyTableApi(auth=auth)
response = table_api.delete_data(table_id=table_id)

# Drop data in table
table_id = ""
table_api = ImplyTableApi(auth=auth)
response = table_api.delete_data_by_interval(table_id=table_id, start="2000-01-01", end="2001-01-01")

# Get table metadata
table_id = ""
table_api = ImplyTableApi(auth=auth)
response = table_api.get(table_id=table_id, detail="detail")
response = table_api.get(table_id=table_id, detail="summary")

# Update a table
table_def = {
    "name": "blah",
    "inputSchema": [
        {
            "name": "channel",
            "type": "string"
        },
        {
            "name": "cityName",
            "type": "string"
        },
        {
            "name": "state",
            "type": "string"
        }
    ]
}
table_id = ""
table_api = ImplyTableApi(auth=auth)
response = table_api.update(table_id=table_id, table_request=table_def)


