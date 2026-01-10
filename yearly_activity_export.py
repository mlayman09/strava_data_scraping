##This script exports data from the PostgreSQL db to various formats and performs
##a backup of the database. It uses ogr2ogr for exporting to files, and pg_dump for
##the database backup, so be sure to have these installed on your machine.

import os, sys
from pathlib import Path
from datetime import datetime

#Get the year the user wants to exports
year = input("What year do you want to export?")

print(sys.path[0])
#Set output directory for exports organized in folders by year
dir = sys.path[0] + "/strava_yearly_files/"

#Make folder for yearly files (if it doesn't exist already)
if os.path.exists(dir + year) == False:
    os.mkdir(dir + year)
else:
    print(year + " folder already exists, moving on")

#Define ogr2ogr parameters
shpOGRcmd = r'ogr2ogr -f "ESRI Shapefile" '
geopackageOGRcmd = r'ogr2ogr -f "GPKG" '
postgresDB = r' PG:"host=localhost user=postgres dbname=YourDBName password=YourPassword" "strava_activities" '
sql = r""" -sql "SELECT * FROM public.strava_activities WHERE date_part('year', start_date_local) = """

#Export a geopackage of all activities
os.system(geopackageOGRcmd+dir+"all_strava_activities.gpkg "+postgresDB+r'-sql "SELECT * FROM public.strava_activities"')

#Export a geopackage and shapefile for the year of your choice
os.system(geopackageOGRcmd+dir+str(year)+"/"+str(year)+"_strava_activities.gpkg "+postgresDB+sql+str(year)+'"')
os.system(shpOGRcmd+dir+str(year)+"/"+str(year)+"_strava_activities.shp "+postgresDB+sql+str(year)+ '"')

#Export a backup of the PostGreSql database and delete the previous backup
current_date = datetime.now().strftime("%m%d%Y")
backup_dir = Path("/path/to/your/backup/directory/")

for file in backup_dir.glob("*.tar"):
    file.unlink()

#Path to postgres installalation
os.system("cd '/path/to/your/postgres/bin/'")
#Perform backup
os.system("pg_dump -U postgres -d strava -F tar -f /path/to/db/backup/directory/"+current_date+"_db_backup.tar")


