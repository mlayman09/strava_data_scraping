import geopandas as gpd, json, pandas as pd, requests, shapely.geometry, sys
from sqlalchemy import create_engine, text
from geoalchemy2 import Geometry


#Get tokens from file to connect to Strava
with open('strava_tokens.json') as json_file:
    strava_tokens = json.load(json_file)

#Make Strava auth API call with current refresh token
response = requests.post(
                        url = 'https://www.strava.com/oauth/token',
                        data = {
                            'client_id': '<your_client_id>'',
                            'client_secret': '<your_client_secret>',
                            'grant_type': 'refresh_token',
                            'refresh_token': strava_tokens['refresh_token']
                            }
                        )
#Save response as json
new_strava_tokens = response.json()

#Save new tokens to file
with open('strava_tokens.json', 'w') as outfile:
    json.dump(new_strava_tokens, outfile)

#Ensure new token is used in request headers
access_token = new_strava_tokens['access_token']
header = {'Authorization': 'Bearer ' + access_token}

#Connect to strava postgres db
postgresDB = create_engine('postgresql://postgres:YourDBPassword@localhost:5432/YourDBName')

#Create a list of activities already in the strava_activities table in the db
with postgresDB.connect() as conn:
    existingActivities = conn.execute(text('SELECT id FROM public.strava_activities'))

idList = [r[0] for r in existingActivities]

#Set activity API url
url = "https://www.strava.com/api/v3/activities"

#Request activities. Note that the page limit is 200, but Strava's API only let's you make 100 requests per 15 minutes (1000 total per day)
requestActivities = requests.get(url + '?access_token=' + access_token + '&per_page=100&page=1')
requestActivitiesJSON = requestActivities.json()

#Create dataframe from the activities request and add a geometry column to hold coordinates from the Stream requests that follow
df = pd.json_normalize(requestActivitiesJSON)
activityDF = df[['name', 'distance', 'moving_time', 'elapsed_time', 'total_elevation_gain', 'type', 'sport_type', 'id', 'start_date_local', 'timezone', 'kudos_count', 'athlete_count', 'commute', 'manual', 'gear_id', 'start_latlng', 'average_speed']].copy()
activityDF["geometry"] = ''

#Pare down the activityDF dataframe to only activities that are not yet in the strava_activities table
activitiesToAdd = []
activitiesToDrop = []
for index, row in activityDF.iloc[:100].iterrows():
    #This takes the "id" column and compares it to the id column in the strava_activities table.
    #If it doesn't find a match, it's added to the list of activities to be added to the table.
    #Else, it drops the activity from the dataframe
    if row['id'] not in idList:
        activitiesToAdd.append(row['id'])
        print(row['name'] + " will be added to the database")
    else:
       activitiesToDrop.append(index)
       activityDF = activityDF.drop(index)

print("Script found " + str(len(activitiesToAdd)) + " new activities to be added to the database")

#This kills the script right here if there are no new activities to be added to the database
if len(activitiesToAdd) == 0:
    sys.exit()

#Iterate through activities to be added to the strava_activities table, and get their associated geometries
for activity in activitiesToAdd:
    #Gets the dataframe index of the activities that need to be added to the strava_activities table
    dfIndex = activityDF.index[activityDF['id']==activity].tolist()
    #This creates the request URL for the activity
    activityURL = url + r"/"+ str(activity) + r"/streams"
    #Request the geometry for each activity. If no coordinates are present (manual workouts, weight training, etc.), then that activity has no geometry written for it
    try:
        activityStream = requests.get(activityURL + "?keys=latlng&key_by_type=True", headers = header)
        activityCoords = activityStream.json()['latlng']['data']
    except KeyError:
        continue

    #Flip the coordinates from XY to YX so that the activities are plotted correctly in geographic space
    newCoords = []
    for x in activityCoords:
        newValList = []
        newValList.append(x[1])
        newValList.append(x[0])
        newCoords.append(newValList)  
    #This creates WKT from the geometry so that it can be ingested by the db later on
    activityDF.at[dfIndex[0], 'geometry'] = newCoords     
    geometry = activityDF['geometry'].apply(lambda x: shapely.geometry.LineString(x))

#Create geodataframe from dataframe for expediting writing to the database
gdf = gpd.GeoDataFrame(activityDF[:100], crs='EPSG:4326', geometry=geometry)
gdf.rename_geometry('activity_geometry', inplace = True)

#Write new activities to the database
gdf.to_postgis('strava_activities', postgresDB, if_exists='append', index=False, dtype={'geom': Geometry(geometry_type='LINESTRING', srid= 4326)})


