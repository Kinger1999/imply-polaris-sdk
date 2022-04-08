from imply_sdk import ImplyAuthenticator, ImplyTableApi, ImplyEventApi
import time
import datetime
import json
import random


BATCH_SIZE = 100000
column_counts = [5, 10, 20, 30, 40, 50]
threads = range(1, 10)
run_count = range(4)

auth = ImplyAuthenticator(
    org_name="oneworldsync",
    client_id="Test",
    client_secret="235a2a90-bffb-49c4-a6bb-266ceb166462"
)
auth.authenticate()  # set the access token
headers = auth.get_headers()


table_api = ImplyTableApi(auth=auth)
response = table_api.list(name="mytest")

if response.status_code == 200:

    table_id = response.json()['values'][0]["id"]

    for column_count in column_counts:

        for thread_count in threads:

            for run in run_count:

                events = []

                while len(events) < BATCH_SIZE:

                    event = {"__time": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')}

                    for j in range(column_count):
                        event[f"col{j}"] = random.randint(1, 10000000)

                    events.append(event)

                start = int(time.time())
                event_api = ImplyEventApi(auth=auth, table_id=table_id)
                responses = event_api.push_list(messages=events, threads=thread_count)
                end = int(time.time())

                error_count = 0
                for response in responses:
                    if response.status_code != 200:
                        error_count += 1
                        # print(response.status_code, response.reason, response.text)

                output = {
                    "column_count": column_count,
                    "run": run,
                    "seconds": end - start,
                    "threads": thread_count,
                    "error_count": error_count,
                    "Bytes": len(",".join(json.dumps(events)))
                }

                output['bytes_per_second'] = int(output['Bytes']/output['seconds'])
                print(output)
