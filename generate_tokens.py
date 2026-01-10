#This makes the strava_tokens json file referenced in the load_activities_to_db script. You only need to run this script once.
#You will need to get a client_id, client_secret, and code from Strava by registering an application on their site.
import requests, json

response = requests.post(
    url = 'https://www.strava.com/oauth/token',
    data = {
            'client_id': '<your_client_id>',
            'client_secret': '<your_client_secret>',
            'code': '<your_code>',
            'grant_type': 'authorization_code'
            }
)

#Save json response as a variable
strava_tokens = response.json()

#Save tokens to file
with open('strava_tokens.json', 'w') as outfile:
    json.dump(strava_tokens, outfile)