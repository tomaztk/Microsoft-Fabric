# credentials are not included
import sys 
import json
import logging

import requests
import msal


config = json.load(open(sys.argv[1]))
app = msal.ConfidentialClientApplication(
    config["client_id"], authority=config["authority"],
    client_credential=config["secret"],
    )


result = None
result = app.acquire_token_silent(config["scope"], account=None)

if not result:
    logging.info("No suitable token exists in cache. Let's get a new one from AAD.")
    result = app.acquire_token_for_client(scopes=config["scope"])

if "access_token" in result:
    # Calling graph using the access token
    graph_data = requests.get(  # Use token to call downstream service
        config["endpoint"],
        headers={'Authorization': 'Bearer ' + result['access_token']}, ).json()
    print("Graph API call result: ")
    print(json.dumps(graph_data, indent=2))
else:
    print(result.get("error"))
    print(result.get("error_description"))
    print(result.get("correlation_id"))  # You may need this when reporting a bug


api_response = requests.get("https://api.fabric.microsoft.com/v1/workspaces/1860beee-5b6a-48cc-9276-1a8d699b92e1/lakehouses/a574d1a3-f10e-498e-9202-d95e18c7128f")
data = api_response.text
parse_json = json.loads(data)
print(parse_json)