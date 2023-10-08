#
#
# This script has been crafted for Python 3.6, which is admittedly a little old but
# at the moment I can't upgrade.
#
# RPMs required - all for Pillow
# ------------------------------
# zlib-devel               
# libjpeg-turbo-devel      
# freetype-devel libjpeg-devel libpng-devel
#
# Libraries required for Pillow
# -----------------------------
# Pillow is the Python image module
# As I am installing on CentOS7 I need to install the following packages. For more notes see https://pillow.readthedocs.io/en/latest/installation.html
# yum install libtiff-devel libjpeg-devel openjpeg2-devel zlib-devel freetype-devel lcms2-devel libwebp-devel tcl-devel tk-devel harfbuzz-devel fribidi-devel libraqm-devel libimagequant-devel libxcb-devel
#
#
# Following modules need to be installed:
# ---------------------------------------
# ==== ENSURE THE REQUIRED rpms are installed FIRST====
#
# protobuf==3.19.6                  - This is actually required for mysql-connector running on Python 3.6
#                                     - This MUST BE INSTALLED BEFORE mysql
# mysql-connector-python==8.0.27    - MariaDB/MySQL connector. This version is required
#                                     as the mysql connector dropped support for Python 3.6
# pytz                              - Time zone. This is required as we are running Python 3.6
#                                     - Isn't required after 3.9 as it is baked in
# requests    
# pillow==8.4.0                     - Image drawing module    
# haversine                         - This contains functions for latitude and logitude calculations         
#
#
# Secrets config file
# -------------------
# This is the config file for confidential details
# This should look like:
#
#        [Database]
#        dbHostname: 102.200.45.12
#        dbName: wordpress-prd
#        dbUser: wpuser
#        dbUserPassword: secretPASSwd
#
#        [at_api]
#        tAPISubscriptionKey: 38095ab894363fd8d2e2392154387
#
#
# Misc notesexi
# ==========
# Odometer   - This is measured in meters NOT kilometers
#
# Geographical coordinates
#   Longitude = East <> West
#   Latitude = North <> South
#

import os
import csv
import json
import math
import requests
import mysql.connector
import datetime
import pytz
import configparser
from requests.exceptions import ConnectionError
from PIL import Image, ImageDraw, ImageColor, ImageFont
from haversine import haversine, Unit       # Used to work out meters to latitude/longitude
from mysql.connector import errorcode

#
# User properties
#
secretsConfFilename = 'C:/Temp/find_my_train.ini'
trackDetailsFilename = "Auckland track details.csv"
trackMapImgFilename = "track_map.png"
specialTrainsFilename = 'Special Trains.csv'
trainRoutesFilename = 'routes.csv'
mapWidthPoints = 4000 
imgMarginPercent = 5
lineWidthPercent = 0.2
legendFontSize = 40  # Pixels
legendRowSpace = 5   # Pixels
legendFontFilename = 'NotoSans-Regular.ttf'
legendBoxWidth = 40
legendBoxMargin = 10
legendBoxHeightOffset = 7
legendRightMargin = 5
lineEndMarginPercent = 0.5
maxSearchRadius = 5
timeZoneStr = 'Pacific/Auckland'
timeRetainMostRecentDataMinutes = 60  
multiTrainDetailsMaxRetentionCount = 5  # Info retention period for a train that is/was part of 6 carridge train. Period measured in number of track sections                  

#
# Load secrets from ini file
#
secretsConfig = configparser.ConfigParser()
secretsConfig.read(secretsConfFilename)

# Set properties from secrets
dbHostname = secretsConfig['Database']['dbHostname']
dbName = secretsConfig['Database']['dbName']
dbUser = secretsConfig['Database']['dbUser']
dbUserPassword = secretsConfig['Database']['dbUserPassword']

mapHeaderToKeys = {
                    'ID':'id',
                    'Line':'line',
                    'Title':'title',
                    'Color Name':'color_name',
                    'Color Hex':'color_hex',
                    'Points':'points_str',
                    'Section Type':'type',
                    'Bearing To Britomart':'bearing_to_britomart',
                    }
atVehiclePosURL = 'https://api.at.govt.nz/realtime/legacy/vehiclelocations'
atAPISubscriptionKey = secretsConfig['at_api']['tAPISubscriptionKey']

mapSpecialTrainHeaderToKeys = {
                                'Train Number':'train_number',
                                'Custom Name':'custom_name',
                                'Image URL':'train_image',
                                }
mapRouteDetailsHeaderToKeys = {
                                'ID':'route_id',
                                'AT route id':'at_route_id',
                                'Full Route Name':'full_route_name',
                                }
trainDetails = {
                    'train':{},
                    'section':{},
                }

# Check the required files exist
for filePath in [trackDetailsFilename, specialTrainsFilename, legendFontFilename, trainRoutesFilename]:
    if not os.path.isfile(filePath):
        print('#')
        print('# A required file is missing:')
        print('#')
        print('# \'' + filePath + '\'.')
        print('#')
        exit(1)

# Create DB connection
try:
    DBConnection = mysql.connector.MySQLConnection(user=dbUser, 
                                    password=dbUserPassword,
                                    host=dbHostname,
                                    database=dbName)
except mysql.connector.Error as err:
    print('#')
    print('# Error setting a DB connection:')
    print('#')
    print('# ' + str(err))
    print('#')
    exit(1)



#
# Derived properties
#
primaryMarginSize = int((mapWidthPoints*imgMarginPercent)/100)
lineWidthPt = int((mapWidthPoints*lineWidthPercent)/100)

#
# Calculate degrees between two angles
#
def smallestAngleBetween(a,b):
    diffOne = abs(a - b)
    if a > b:
        diffTwo = b + (360 - a)
    else:
        diffTwo = a + (360 - b)
    if diffOne < diffTwo:
        smallestDiff = diffOne
    else:
        smallestDiff = diffTwo

    return smallestDiff

#
# Perform additional train calculations
#
# This performs additional algorithms to work out extra details
# which were not immediately obvious
#
def additionalCalculations(trainDetails,routeDetails):

    #
    # Get current train details from the DB
    #
    cursorTrainDetails = DBConnection.cursor(dictionary=True)
    sqlQuery = 'select * from fmt_train_details'
    try:
        cursorTrainDetails.execute(sqlQuery)
    except mysql.connector.Error as err:
        print('#')
        print('# Error querying \'fmt_train_details\', in database, while running additionalCalculations()')
        print('#')
        print('# ' + str(err))
        print('#')
        exit(1)
    currentDBTrainDetails = {}
    for currDBTrain in cursorTrainDetails:
        currentDBTrainDetails.update({currDBTrain['train_number']:currDBTrain})


    #
    # Work out if trains are part of a multi-train
    #
    # Imagine a train is at a normal station, which is in the table 'fmt_track_sections' and has
    # a value of 'S' for 'type'.
    # IF we have two trains like this at a normal station and they are both going in the same
    # direction, as in to or from Britomart, then we will assume they both for a 6 car train.
    #
    # This doesn't work at an 'Interchange', such as Britomart, where there could be multiple
    # trains going the same direction but not in the same service and not part of a 6 car. The same
    # is true for yards.
    #
    for currSection in trainDetails['section']:
        sectionType = trainDetails['section'][currSection]['detail']['type']
        
        
        if (sectionType == 'S') and (len(trainDetails['section'][currSection]['trains']) > 1):

            #
            # Look at the trains going in each direction - to or from Britomart
            #
            # If there are two, or more, trains going in the same direction then this must
            # be a multi-train, ie. a 6 carridge train.
            #
            for goingToBritomart in ('Y', 'N'):
                sectionTrainRouteID = 0
                multitrainListConnectedTrains = ''
                multitrainNoConnectedTrains = 0
                for sectionTrain in trainDetails['section'][currSection]['trains']:
                    trainDetails['train'][sectionTrain].update({'currently_part_of_multi-train':False})

                    if trainDetails['section'][currSection]['trains'][sectionTrain]['heading_to_britomart'] == goingToBritomart:

                        # Keep a count of how many trains in this set - 'multitrainNoConnectedTrains'
                        multitrainNoConnectedTrains += 1
                        
                        # Create a string that is a list of trains in this set - 'multitrainListConnectedTrains'
                        trainFriendlyName = trainDetails['section'][currSection]['trains'][sectionTrain]['friendly_name']
                        if multitrainListConnectedTrains  == '':
                            multitrainListConnectedTrains = trainFriendlyName
                        else:
                            multitrainListConnectedTrains += ' and ' + trainFriendlyName

                        # If this train is the one with 'trip' details then record the routeID value - 'sectionTrainRouteID'
                        if 'trip' in trainDetails['section'][currSection]['trains'][sectionTrain]['vehicle']:
                            ATRouteID = trainDetails['section'][currSection]['trains'][sectionTrain]['vehicle']['trip']['route_id']
                            if ATRouteID in routeDetails['at_route_id']:
                                sectionTrainRouteID = routeDetails['at_route_id'][ATRouteID]['route_id']
                            else:
                                #
                                # This is a new train route we don't know about
                                #
                                print('#')
                                print('# The AT train route of \'' + ATRouteID + '\' is unknown so stopping')
                                print('#')
                                exit(1)

                if (multitrainNoConnectedTrains > 1) and (sectionTrainRouteID != 0):
                    #
                    # If this is a multi-train where we have been able to identify the route, then update both trains details
                    #
                    for sectionTrain in trainDetails['section'][currSection]['trains']:
                        if trainDetails['section'][currSection]['trains'][sectionTrain]['heading_to_britomart'] == goingToBritomart:
                            trainDetails['train'][sectionTrain].update({'most_recent_route_id':sectionTrainRouteID})
                            trainDetails['train'][sectionTrain].update({'most_recent_list_connected_trains':multitrainListConnectedTrains})
                            trainDetails['train'][sectionTrain].update({'most_recent_no_connected_trains':multitrainNoConnectedTrains})

                        #
                        # We can define the front train simarly to below:
                        # - If the train is going to Britomart then the front train is the one on the Britomart end
                        # - If the train is going away from Britomart then the front train is one at the non-Britomart end
                        #
                        # Looing at the train details it is MOST LIKELY, though not 100% guarantee, that the front train is the one
                        # that has "trip" details and the back train does not.
                        #
                        # Thus we can have a good guess at which end each train is
                        #
                        if goingToBritomart == 'Y':
                            if 'trip' in trainDetails['section'][currSection]['trains'][sectionTrain]['vehicle']:
                                currTrainAtBritomartEnd = True
                            else:
                                currTrainAtBritomartEnd = False
                        if goingToBritomart == 'N':
                            if 'trip' in trainDetails['section'][currSection]['trains'][sectionTrain]['vehicle']:
                                currTrainAtBritomartEnd = False
                            else:
                                currTrainAtBritomartEnd = True
                        trainDetails['train'][sectionTrain].update({'train_at_britomart_end':currTrainAtBritomartEnd})
                        trainDetails['train'][sectionTrain].update({'multi_train_most_recent_section':currSection})
                        trainDetails['train'][sectionTrain].update({'multi_train_most_recent_section_count':0})
                        trainDetails['train'][sectionTrain].update({'currently_part_of_multi-train':True})
    
    #
    # Having identified all the 6 carridge trains, we now need to go through all the trains a second time looking at 
    # trains that aren't part of a 6. We need to do our best to collect the details for these trains
    # 
    for currTrain in  trainDetails['train']:
        trainFriendlyName = trainDetails['train'][currTrain]['friendly_name']

        #
        # If this train IS NOT part of a 6 carridge
        # 
        if not trainDetails['train'][currTrain]['currently_part_of_multi-train']:      

            #
            # Does this train have 'trip' detail
            #
            if 'trip' in trainDetails['train'][currTrain]['vehicle']:

                # If this train does have trip detail work out the routeID
                ATRouteID = trainDetails['train'][currTrain]['vehicle']['trip']['route_id']
                if ATRouteID in routeDetails['at_route_id']:
                    currTrainRouteID = routeDetails['at_route_id'][ATRouteID]['route_id']
                    noConnectedTrains = 1
                    listConnectedTrains = trainFriendlyName
                    multitrainSectionCount = 1
                else:
                    #
                    # This is a new train route we don't know about
                    #
                    print('#')
                    print('# The AT train route of \'' + ATRouteID + '\' is unknown so stopping')
                    print('#')
                    exit(1)
            else:
                # 
                # If we get here it means for this api call, this train was NOT identified as
                # being part of a 6 carridge train.
                #
                # Also this train doesn't have any trip information
                #
                # This could mean:
                # - It is part of a 6 carridge train, but just isn't at a station or for some reason the trains with the
                #   trip details didn't fit into this api window
                # - It doesn't have trip details for some reason
                # - It could have been in a 6, split in half and this one is on it's way back to the yard
                #
                # At this point it gets tricky, we could look at the database and see it previously was part of a 6
                # and assume it still is and retain that detail.
                #
                # But what if the train was a 6 but now has been split into two 3 carridges, and potentially this half is
                # travelling to another place or stationed in a yard.
                #
                # We need a way to timeout or otherwise put a limit on how long we keep saying it is part of a 6
                #

                if (currentDBTrainDetails[currTrain]['multi_train_most_recent_section_count'] < multiTrainDetailsMaxRetentionCount) and \
                (currentDBTrainDetails[currTrain]['most_recent_no_connected_trains'] > 1):
                    #
                    # For this train we currently have some historical 6 carridge details and we haven't yet maxed out the timeout for the
                    # the number of section changes 'multiTrainDetailsMaxRetentionCount'. So just retain the current DB details
                    #
                    #               --- ONLY make a change if the section has changed ---
                    #
                    if currentDBTrainDetails[currTrain]['multi_train_most_recent_section'] == trainDetails['train'][currTrain]['section']['id']:
                        # We are still in the same section so don't change anything from what is currently in the DB
                        multitrainSectionCount = currentDBTrainDetails[currTrain]['multi_train_most_recent_section_count']
                    else:
                        # We have changed section so increment count 
                        multitrainSectionCount = currentDBTrainDetails[currTrain]['multi_train_most_recent_section_count'] + 1

                    # Rest of the details are unchanged - Note 'multi_train_most_recent_section' will fix itself as that is always
                    # set to the current section
                    noConnectedTrains = currentDBTrainDetails[currTrain]['most_recent_no_connected_trains']
                    multitrainSectionCount = currentDBTrainDetails[currTrain]['multi_train_most_recent_section_count']
                    currTrainRouteID = currentDBTrainDetails[currTrain]['most_recent_route_id']
                    listConnectedTrains = currentDBTrainDetails[currTrain]['most_recent_list_connected_trains']


                else:
                    #
                    # If this train has been through 'multiTrainDetailsMaxRetentionCount' number of sections then we 
                    # can no-longer assume it is still part of the same 6 carriage train
                    #
                    noConnectedTrains = 1
                    multitrainSectionCount = 9999
                    currTrainRouteID = routeDetails['at_route_id']['na']['route_id']   # We don't know the route id so set to the unknown route
                    listConnectedTrains = trainFriendlyName

            #
            # Update train details
            #
            trainDetails['train'][currTrain].update({'most_recent_list_connected_trains':listConnectedTrains})
            trainDetails['train'][currTrain].update({'most_recent_no_connected_trains':noConnectedTrains})
            trainDetails['train'][currTrain].update({'most_recent_route_id':currTrainRouteID})
            trainDetails['train'][currTrain].update({'multi_train_most_recent_section': trainDetails['train'][currTrain]['section']['id']})
            trainDetails['train'][currTrain].update({'multi_train_most_recent_section_count':multitrainSectionCount})

                          

    print(json.dumps(trainDetails, indent=4, sort_keys=True, default=str))
    print('additionalCalculations() - Just prior DB update of Train details')




    #
    # Step through the current trains and update the DB where necessary
    #
    cursorUpdateTrains = DBConnection.cursor(dictionary=True)
    for currTrain in trainDetails['train']:

        odometer = currentDBTrainDetails[currTrain]['odometer']  
        if 'odometer' in trainDetails['train'][currTrain]['vehicle']:
            odometer = trainDetails['train'][currTrain]['vehicle']['odometer']
            
        #print(json.dumps(routeDetails, indent=4, sort_keys=True, default=str))

        try:
            updateQuery = ''' UPDATE 
                                fmt_train_details 
                                SET 
                                odometer = %s,
                                most_recent_route_id = %s,
                                most_recent_list_connected_trains = %s,
                                most_recent_no_connected_trains = %s,
                                multi_train_most_recent_section = %s,
                                multi_train_most_recent_section_count = %s
                                WHERE 
                                train_number = %s'''
            updateValues = (odometer,
                            trainDetails['train'][currTrain]['most_recent_route_id'],
                            trainDetails['train'][currTrain]['most_recent_list_connected_trains'],
                            trainDetails['train'][currTrain]['most_recent_no_connected_trains'],         
                            trainDetails['train'][currTrain]['multi_train_most_recent_section'],
                            trainDetails['train'][currTrain]['multi_train_most_recent_section_count'],
                            currTrain,
                            )
            cursorUpdateTrains.execute(updateQuery, updateValues)
            DBConnection.commit()
        except mysql.connector.Error as err:
            print('#')
            print('# Error updating train details, in \'fmt_train_details\', in database:')
            print('#')
            print('# ' + str(err))
            print('#')
            exit(1)


            # print('Section type = ' + trainDetails['section'][currSection]['detail']['type'])

            # print('#section = ' + str(currSection))
            #print(json.dumps(currentDBTrainDetails, indent=4, sort_keys=True, default=str))

    return trainDetails


#
# Load special train details
#
# There are a number of trains which are "special"
#
# This is because they are wrapped, or maybe have adversising
# screens or something else. 
#
# For these trains we specify custom photos or descriptions. These details are
# kept in a csv file which this function will load.
#
# Note there is one extra special train, number 0, this is the settings to
# apply to trains which aren't special.
#
def loadSpecialTrainDetails():

    print('Running loadSpecialTrainDetails()')

    #
    # There needs to be a row for default train details - as in trains that aren't special
    # The default train has a number '0'
    #
    defaultTrainFound = False
    
    #
    # Load csv into dict
    #
    with open(specialTrainsFilename, mode='r') as specialTrainDetailsCSV:
        specialTrainsDetailsReader = csv.DictReader(specialTrainDetailsCSV)

        # Remap header names to dict keys
        remappedHeaders = []
        for headerName in specialTrainsDetailsReader.fieldnames:
            remappedHeaders.append(mapSpecialTrainHeaderToKeys[headerName])
        specialTrainsDetailsReader.fieldnames = remappedHeaders

        specialTrainDetails = {}

        # Load rows
        for currRow in specialTrainsDetailsReader:
            specialTrainDetails.update({currRow['train_number']:currRow})
            if currRow['train_number'] == '0':
                defaultTrainFound = True

        if not defaultTrainFound:
                print('#')
                print('# No \'default\' train found in \'' + specialTrainsFilename + '\'.')
                print('#')
                print('# Ensure this file has a row with a \'Train Number\' of 0.' )
                print('# This is the default train')
                print('#')
                exit(1)
            
    return specialTrainDetails

#
# Ensure the fmt_routes table is correct
#
def loadTrainRoutes():

    print('Running loadTrainRoutes()')

    #
    # There needs to be a row for default train details - as in trains that aren't special
    # The default train has a number '0'
    #
    unknownRouteFound = False
    
    #
    # Load csv into dict
    #
    with open(trainRoutesFilename, mode='r') as routeDetailsCSV:
        trainRouteDetailsReader = csv.DictReader(routeDetailsCSV)

        # Remap header names to dict keys
        remappedRouteHeaders = []
        for headerName in trainRouteDetailsReader.fieldnames:
            remappedRouteHeaders.append(mapRouteDetailsHeaderToKeys[headerName])
        trainRouteDetailsReader.fieldnames = remappedRouteHeaders

        routeDetails = {
                        'route_id':{},
                        'at_route_id':{},
                        }
        
        # Load rows
        for currRow in trainRouteDetailsReader:
            # Check the row is valid
            if (currRow['route_id'] == "") or (currRow['at_route_id'] == "") or (currRow['full_route_name'] == "") \
                or (currRow['route_id'] in routeDetails['route_id']) or (currRow['at_route_id'] in routeDetails['at_route_id']):
                print('#')
                print('# There is a problem for \'ID\' in \'' + specialTrainsFilename + '\'.')
                print('#')
                print('# ' +  json.dumps(currRow, indent=4, sort_keys=True, default=str))
                print('#')
                print('# Check all columns have values.')
                print('# Check all \'ID\' values are unique.')
                print('# Check all \'AT route id\' values are unique.')
                print('#')
                print('# routeDetails: ' + json.dumps(routeDetails, indent=4, sort_keys=True, default=str))
                print('#')
                exit(1)
            routeDetails['route_id'].update({currRow['route_id']:currRow})
            routeDetails['at_route_id'].update({currRow['at_route_id']:currRow})
            if currRow['at_route_id'] == 'na':
                unknownRouteFound = True
        
        if not unknownRouteFound:
                print('#')
                print('# No \'na\' route found in \'' + specialTrainsFilename + '\'.')
                print('#')
                print('# This is the route description that will be used where no route id has been given.')
                print('#')
                print('# Ensure this file has a row with a \'AT route id\' of \'na\'.' )
                print('#')
                exit(1)

    #
    # Get a list of all routes in the "fmt_routes" table
    #
    cursorRoutesList = DBConnection.cursor(dictionary=True)
    sqlQuery = 'select * from fmt_routes'
    try:
        cursorRoutesList.execute(sqlQuery)
    except mysql.connector.Error as err:
                    print('#')
                    print('# Error querying \'fmt_routes\', in database:')
                    print('#')
                    print('# ' + str(err))
                    print('#')
                    exit(1)
    knownRoutes = {}
    for currRoute in cursorRoutesList:
        knownRoutes.update({currRoute['id']:currRoute})

    #
    # Update the DB if required
    #
    # NOTE if an 'ID' exists in the DB but not in the csv file then it 
    #      won't be touched on the basis that it once existed
    # 
    #   
    #print('\n\nknownRoutes:\n' + json.dumps(knownRoutes, indent=4, sort_keys=True, default=str))
    #print('\n\nrouteDetails:\n' + json.dumps(routeDetails, indent=4, sort_keys=True, default=str))
    for routeID in routeDetails['route_id']:
        #print(str(routeID))
        if int(routeID) in knownRoutes:
            #
            # Update the row only if it is not correct
            #
            if (routeDetails['route_id'][routeID]['at_route_id'] != knownRoutes[int(routeID)]['at_route_id']) or \
            (routeDetails['route_id'][routeID]['full_route_name'] != knownRoutes[int(routeID)]['full_route_name']):
                try:                    
                    updateQuery = ''' UPDATE fmt_routes SET at_route_id = %s, full_route_name = %s WHERE id = %s'''
                    updateValues = (routeDetails['route_id'][routeID]['at_route_id'],
                                    routeDetails['route_id'][routeID]['full_route_name'],
                                    routeID,
                                    )
                    cursorRoutesList.execute(updateQuery, updateValues)
                    DBConnection.commit()
                except mysql.connector.Error as err:
                    print('#')
                    print('# Error updating route details, in \'fmt_routes\', in database:')
                    print('#')
                    print('# ' + str(err))
                    print('#')
                    exit(1)
        else:
            #
            # Current details aren't in the table so add them
            #
            try:
                insertQuery = ''' INSERT INTO fmt_routes 
                                (id,
                                at_route_id,
                                full_route_name
                                )
                                VALUES ( %s, %s, %s)'''
                insertValues = (routeID,
                                routeDetails['route_id'][routeID]['at_route_id'],
                                routeDetails['route_id'][routeID]['full_route_name'],
                                )
                cursorRoutesList.execute(insertQuery, insertValues)
                DBConnection.commit()
            except mysql.connector.Error as err:
                print('#')
                print('# Error inserting new route details, in \'fmt_routes\', in database:')
                print('#')
                print('# ' + str(err))
                print('#')
                exit(1)

    return routeDetails


#
# Convert api timestamps to datetime
#
def posixtoDateTime(posixDate):
    return datetime.datetime.fromtimestamp(posixDate, pytz.timezone(timeZoneStr))

#
# Get current details about vehicles
#
def getCurrVehicleDetails(specialTrainDetail):

    print('# Running getCurrVehicleDetails()')

    #
    # Get vehicle positions via api call
    # 
    try:
        headers = {'content-type': 'application/json','Ocp-Apim-Subscription-Key':atAPISubscriptionKey}
        response = requests.get(atVehiclePosURL, headers=headers) 
    except ConnectionError as err:
                        print('#')
                        print('# Error calling AT api :' + atVehiclePosURL)
                        print('#')
                        print('# Full error:')
                        print('# ' + str(err))
                        print('#')
                        exit(1)
    if response.status_code != 200:
        print('#')
        print('# Error: Status code ' + str(response.status_code))
        print('#')
        exit(1)
    
    #
    # Get a list of all trains in the "fmt_train_details" table
    #
    cursorTrainList = DBConnection.cursor(dictionary=True)
    sqlQuery = 'select train_number from fmt_train_details'
    cursorTrainList.execute(sqlQuery)
    knownTrains = []
    for currTrain in cursorTrainList:
        knownTrains.append(currTrain['train_number'])
    #print('\n\nknownTrains:\n' + json.dumps(knownTrains, indent=4, sort_keys=True, default=str))



    #
    # Load train data
    #
    # With little alternative we will determine that the vehicle is a Train it has an 'id' tha begins 59
    # Also it has a 'label' that begins with 'AMP ' - note the space and uppercase.
    #
    for currVehicle in response.json()['response']['entity']:
        #
        # Determine if this vehicle is a train
        # - We used to look at if the vehicle id was 5 digits and started with 59
        #   As the train numbers are already at 59958 I think it could easily go past
        #   1000 and that would break our validation, instead I am checking if the 'label'
        #   starts 'AMP '
        #
        if 'label' in currVehicle['vehicle']['vehicle']:
            if currVehicle['vehicle']['vehicle']['label'][:4] == 'AMP ':
            # if len(currVehicle['id']) == 5:
            #     if currVehicle['id'][:2] == '59':
                #
                # If it is a train
                #
                currTrainNo = currVehicle['id'][2:]
                trainDetails['train'].update({currTrainNo:currVehicle})

                # Initiall set this train as not a part of a multi-part train
                trainDetails['train'][currTrainNo]['currently_part_of_multi-train'] = False
                
                headingToBritomart = 'na'

                #
                # It seems sometimes the "bearing" value is an int and sometimes a stg in the json response :-(
                #
                if 'bearing' in currVehicle['vehicle']['position']:
                    trainDetails['train'][currTrainNo]['vehicle']['position']['bearing'] = str(currVehicle['vehicle']['position']['bearing'])

                trainLabel = trainDetails['train'][currTrainNo]['vehicle']['vehicle']['label']
                currLatitude = trainDetails['train'][currTrainNo]['vehicle']['position']['latitude']
                currLongitude = trainDetails['train'][currTrainNo]['vehicle']['position']['longitude']
                print('Label: ' + trainLabel)
                print('Latitude: ' + str(currLatitude))
                print('Logitude: ' + str(currLongitude))
                imgCoords = geographicLocToImgLoc(currLatitude, currLongitude, trackDetails)
                print('imgCoords: ' + str(imgCoords))

                #
                # Python pillow isn't perfect and sometimes when it draws bends there are
                # slithers of white where it doesn't draw. 
                #
                # To accomodate this, if we are at a point which has no color, is white, we need to
                # look one or two pixels either side, just to be sure.
                #
                # To do this we step one pixel away from the search point and look for a non-while color
                # and if we still don't find non-white, then step another pixel away - I call this the
                # radius.
                #
                xCoord = int(imgCoords[0])
                yCoord = int(imgCoords[1])
                for searchRadius in range(0,(maxSearchRadius+1)):
                    currSearchRadius = searchRadius
                    for yNewPos in range((yCoord - searchRadius),(yCoord + searchRadius + 1)):
                        for xNewPos in range((xCoord - searchRadius),(xCoord + searchRadius + 1)):
                            print('xNewPos: ' + str(xNewPos))
                            print('yNewPos: ' + str(yNewPos))
                            print('currSearchRadius: ' + str(currSearchRadius))
                            rgbValue = mapContext.getpixel((xNewPos,yNewPos)) 
                            hexValue = '#{:02x}{:02x}{:02x}'.format(*rgbValue).lower()     # Lowercase for searching
                            if hexValue != '#ffffff':
                                break
                        if hexValue != '#ffffff':
                            break
                    if hexValue != '#ffffff':
                        break
                if hexValue in list(trackDetails['hex_values']):
                    trainDetails['train'][currTrainNo].update({'section': trackDetails['hex_values'][hexValue]})
                    trainDetails['train'][currTrainNo].update({'search_radius':currSearchRadius})
                    currSectionBearing = trackDetails['hex_values'][hexValue]['bearing_to_britomart_int']
                    print('Current section bearing: ' + str(currSectionBearing))
                    print('Title: ' + trackDetails['hex_values'][hexValue]['title'])

                    #
                    # Work out which direction the train is going
                    #
                    # Only do this if both
                    # - This sections bearing has been defined - currSectionBearing != -1
                    # - This vehicle's data has a valid bearing value defined - trainHasValidBearing = True
                    #
                    trainHasValidBearing = False
                    if 'bearing' in trainDetails['train'][currTrainNo]['vehicle']['position']:
                        currTrainBearingStr =  trainDetails['train'][currTrainNo]['vehicle']['position']['bearing']
                        if currTrainBearingStr.isdigit():
                            currTrainBearing = int(currTrainBearingStr)
                            trainHasValidBearing = True
                        else:
                            print('Trains bearing is not a digit: \'' + str(currTrainBearingStr) + '\'')
                    else:
                        print('Train does not have a \'bearing\' value.')
                    if (currSectionBearing != -1) and trainHasValidBearing:
                        bearingDelta = smallestAngleBetween(currTrainBearing,currSectionBearing)
                        headingToBritomart = 'N'
                        if bearingDelta < 90:
                            #
                            # If the difference between the trains bearing and the bearing of the track section to the city,
                            # is less than 90 degrees, in other words if the train is more or less pointing in the same direction
                            # as the direction to the city for this section of the track, 
                            #
                            print('Bearing delta: ' + str(bearingDelta))
                            headingToBritomart = 'Y'
                        trainDetails['train'][currTrainNo].update({'heading_to_britomart':headingToBritomart})
                        trainDetails['train'][currTrainNo].update({'bearing_delta_between_section_and_train':bearingDelta})
                

                    #
                    # Update the database train details
                    #
                    friendlyName = trainDetails['train'][currTrainNo]['vehicle']['vehicle']['label'].replace(' ', '')
                    trainDetails['train'][currTrainNo].update({'friendly_name':friendlyName})
                    trainID = trainDetails['train'][currTrainNo]['vehicle']['vehicle']['id']
                    trainLabel = trainDetails['train'][currTrainNo]['vehicle']['vehicle']['label']
                    trainOdometer = -1
                    if 'odometer' in trainDetails['train'][currTrainNo]['vehicle']['position']:
                        trainDetails['train'][currTrainNo]['vehicle']['position']['odometer']
                    customName = friendlyName
                    imageURL = specialTrainDetail['0']['train_image']
                    if currTrainNo in specialTrainDetail:
                        customName = specialTrainDetail[currTrainNo]['custom_name']
                        imageURL = specialTrainDetail[currTrainNo]['train_image']
                    if currTrainNo in knownTrains:
                        
                        try:
                            
                            updateQuery = ''' UPDATE fmt_train_details SET vehicle_id = %s, vehicle_label = %s, friendly_name = %s,
                                            odometer = %s, image_url = %s, custom_name =%s WHERE train_number = %s'''
                            updateValues = (trainID,
                                            trainLabel,
                                            friendlyName,
                                            trainOdometer,
                                            imageURL,
                                            customName,
                                            currTrainNo,
                                            )
                            cursorTrainList.execute(updateQuery, updateValues)
                            DBConnection.commit()
                        except mysql.connector.Error as err:
                            print('#')
                            print('# Error updating train details, in \'fmt_train_details\', in database:')
                            print('#')
                            print('# ' + str(err))
                            print('#')
                            exit(1)
                    else:
                        try:
                            insertQuery = ''' INSERT INTO fmt_train_details 
                                            (vehicle_id,
                                            vehicle_label,
                                            friendly_name,
                                            odometer,
                                            image_url,
                                            custom_name,
                                            train_number
                                            )
                                            VALUES ( %s, %s, %s, %s, %s, %s, %s)'''
                            insertValues = (trainID,
                                            trainLabel,
                                            friendlyName,
                                            trainOdometer,
                                            imageURL,
                                            customName,
                                            currTrainNo,
                                            )
                            cursorTrainList.execute(insertQuery, insertValues)
                            DBConnection.commit()
                        except mysql.connector.Error as err:
                            print('#')
                            print('# Error inserting new train details, in \'fmt_train_details\', in database:')
                            print('#')
                            print('# ' + str(err))
                            print('#')
                            exit(1)

                    #
                    # Determine initial details to insert or update
                    #
                    dbRouteID= routeDetails['at_route_id']['na']['route_id']
                    #print(json.dumps(trainDetails['train'][currTrainNo]['vehicle'], indent=4, sort_keys=True, default=str))
                    #print('Exiting now 22')
                    if 'trip' in trainDetails['train'][currTrainNo]['vehicle']:
                        ATRouteID = trainDetails['train'][currTrainNo]['vehicle']['trip']['route_id']
                        currTrainRouteID = routeDetails['at_route_id'][ATRouteID]['route_id']
                        # print(json.dumps(routeDetails, indent=4, sort_keys=True, default=str) + '\n\n')
                        # print(json.dumps(routeDetails['route_id'], indent=4, sort_keys=True, default=str))
                        # print('currTrainRouteID = ' + str(currTrainRouteID))
                        # print('Exiting now 23')
                        if currTrainRouteID in routeDetails['route_id']:
                            dbRouteID= routeDetails['route_id'][currTrainRouteID]['route_id']
                        else:
                            print('Route NOT KNOWN')

                    dbTrainNumber = currTrainNo
                    dbSectionID = trainDetails['train'][currTrainNo]['section']['id']
                    dbLastUpdatedPosix = dbFirstUpdatedPosix = currTimestampPosix = trainDetails['train'][currTrainNo]['vehicle']['timestamp']
                    dbLastUpdated = dbFirstUpdated = currTrainTimestamp = posixtoDateTime(currTimestampPosix)
                    
                    dbTripID = "na"
                    if 'trip' in trainDetails['train'][currTrainNo]['vehicle']:
                        dbTripID = trainDetails['train'][currTrainNo]['vehicle']['trip']['trip_id']
                    dbLatestOdometer = -1
                    if 'odometer' in trainDetails['train'][currTrainNo]['vehicle']['position']:
                        dbLatestOdometer = trainDetails['train'][currTrainNo]['vehicle']['position']['odometer']
                    dbLatestSpeed = -1
                    if 'speed' in trainDetails['train'][currTrainNo]['vehicle']['position']:
                        dbLatestSpeed = trainDetails['train'][currTrainNo]['vehicle']['position']['speed']
                    dbHeadingToBritomart = headingToBritomart
                    
                    
                    
                    #
                    # Update location details for this train
                    #
                    # Assumptions
                    # ===========
                    # - This script runs regularly enough that if the latest record for this train is for the
                    #   same section of the tracks then the train has not gone to another section and then come back.
                    #
                    cursorLocations = DBConnection.cursor(dictionary=True, buffered=True)
                    cursorUpdateLocations = DBConnection.cursor(dictionary=True, buffered=True)
                    sqlQuery = 'select * from fmt_locations fl where train_number = %s order by last_updated desc limit 1'
                    sqlVaues =(dbTrainNumber,)
                    try:
                        cursorLocations.execute(sqlQuery, sqlVaues)
                    except mysql.connector.Error as err:
                                    print('#')
                                    print('# Error querying \'fmt_locations\', in database:')
                                    print('#')
                                    print('# ' + str(err))
                                    print('#')
                                    exit(1)
                    currLocationRow = cursorLocations.fetchone()
                    dbRowID = -1
                    insertNewRow = False
                    if cursorLocations.rowcount < 1:
                        insertNewRow = True
                        print('###### Row count = -1')
                    else:
                        dbRowID = currLocationRow['id']
                        print('currLocationRow[\'section_id\'] ' + str(currLocationRow['section_id']) )
                        print('dbSectionID ' + str(dbSectionID))
                        if (currLocationRow['section_id'] == int(dbSectionID)):
                            print('############# CURRENT SECTION IS SAME    CCCCCCCCCCCCC')
                            print('currLocationRow[\'last_updated_posix\'] = ' + str(currLocationRow['last_updated_posix']))
                            print('currTimestampPosix ' + str(currTimestampPosix))
                            if (currLocationRow['last_updated_posix'] != currTimestampPosix):
                                #
                                # If we get here then there is at least one row of data and
                                # that row is for the same track section as where the train
                                # is currently and the timestamp is different.
                                #
                                # So we need to UPDATE this row - rather than insert
                                #
                                print('############ TIMESTAMP IS DIFFERENT    XXXXXX')

                                #
                                # There are a number of columns that should be updated, so long as the new
                                # value is not unknown - eg. "na" or "-1"
                                #
                                # In the case where the value is either 'na' or '-1' then we still do an update
                                # but we update it to the existing value in the DB. Its more robust than modifying
                                # the sql update.
                                #
                                if dbTripID == 'na':
                                    dbTripID = currLocationRow['trip_id']
                                if dbLatestOdometer == -1:
                                    dbLatestOdometer = currLocationRow['latest_odometer']
                                if dbLatestSpeed == -1:
                                    dbLatestSpeed = currLocationRow['latest_speed']
                                if dbHeadingToBritomart == 'na':
                                    dbHeadingToBritomart = currLocationRow['heading_to_britomart']
                                if dbRouteID == routeDetails['at_route_id']['na']['route_id']:
                                    dbRouteID = currLocationRow['route_id']

                                #
                                # Note that some things like 'last_updated' will always be changed
                                #
                                print('# Updating existing location row')
                                try:                    
                                    updateQuery = ''' UPDATE fmt_locations 
                                                        SET 
                                                        last_updated = %s, 
                                                        trip_id = %s,
                                                        latest_odometer = %s,
                                                        latest_speed = %s,
                                                        heading_to_britomart = %s,
                                                        route_id = %s,
                                                        last_updated_posix = %s
                                                        WHERE id = %s'''
                                    
                                    updateValues = (
                                                    dbLastUpdated,
                                                    dbTripID,
                                                    dbLatestOdometer,
                                                    dbLatestSpeed,
                                                    dbHeadingToBritomart,
                                                    dbRouteID,
                                                    dbLastUpdatedPosix,
                                                    dbRowID
                                                    )
                                    cursorUpdateLocations.execute(updateQuery, updateValues)
                                    DBConnection.commit()
                                except mysql.connector.Error as err:
                                    print('#')
                                    print('# Error updating route details, in \'fmt_routes\', in database:')
                                    print('#')
                                    print('# ' + str(err))
                                    print('#')
                                    exit(1)
                            else:
                                # Timestamp is the same so do nothing
                                print('############ TIMESTAMP IS THE SAME     YYYYYY')
                        else:
                            # The section is different so we need to insert a new row
                            insertNewRow = True
                            print('############# CURRENT SECTION IS DIFFERENT   AAAAAAAAAAAAA')

                    #
                    print('Row count = ' + str(cursorLocations.rowcount))
                    print('currLocationRow = ' + str(currLocationRow))




                    
                    
                    
                    
                    print('\n### Location details')
                    print('dbRowID = ' + str(dbRowID))
                    print('dbTrainNumber = ' + str(dbTrainNumber))
                    print('dbSectionID = ' + str(dbSectionID))
                    print('dbFirstUpdated = ' + str(dbFirstUpdated))
                    print('dbLastUpdated = ' + str(dbLastUpdated))
                    print('dbTripID = ' + dbTripID)
                    print('dbLatestOdometer = ' + str(dbLatestOdometer))
                    print('dbLatestSpeed = ' + str(dbLatestSpeed))
                    print('dbHeadingToBritomart = ' + dbHeadingToBritomart)
                    print('dbRouteID = ' + str(dbRouteID))
                    print('dbFirstUpdatedPosix = ' + str(dbFirstUpdatedPosix))
                    print('dbLastUpdatedPosix = ' + str(dbLastUpdatedPosix))
                    if insertNewRow:
                        #
                        # Insert new row
                        #
                        print('# Inserting new location row')
                        try:
                            insertQuery = ''' INSERT INTO fmt_locations 
                                            (train_number,
                                            section_id,
                                            first_updated,
                                            last_updated,
                                            trip_id,
                                            latest_odometer,
                                            latest_speed,
                                            heading_to_britomart,
                                            route_id,
                                            first_updated_posix,
                                            last_updated_posix
                                            )
                                            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                            insertValues = (dbTrainNumber,
                                            dbSectionID,
                                            dbFirstUpdated,
                                            dbLastUpdated,
                                            dbTripID,
                                            dbLatestOdometer,
                                            dbLatestSpeed,
                                            dbHeadingToBritomart,
                                            dbRouteID,
                                            dbFirstUpdatedPosix,
                                            dbLastUpdatedPosix
                                            )
                            cursorTrainList.execute(insertQuery, insertValues)
                            DBConnection.commit()
                        except mysql.connector.Error as err:
                            print('#')
                            print('# Error inserting new train details, in \'fmt_train_details\', in database:')
                            print('#')
                            print('# ' + str(err))
                            print('#')
                            exit(1)
                    print('### End location details\n\n')

                    currSectionID = trainDetails['train'][currTrainNo]['section']['id']
                    if currSectionID not in trainDetails['section']:
                        trainDetails['section'].update({
                                                        currSectionID:{
                                                            'trains':{},
                                                            'detail':trainDetails['train'][currTrainNo]['section'],
                                                        }})
                    trainDetails['section'][currSectionID]['trains'].update({currTrainNo:trainDetails['train'][currTrainNo]})

                else:
                    print('HEX Value NOT FOUND')
                print('\n\n')


    return trainDetails

#
# Convert latitude and logitude details to image location
#
def geographicLocToImgLoc(currLatitude, currLongitude, trackDetails):
    xPos = (currLongitude - trackDetails['details']['minLongitude'])/trackDetails['details']['widthDegreesPerMapPoint'] + \
            trackDetails['details']['primaryMarginSize'] + trackDetails['details']['legendTotalWidth']
    yPos =  trackDetails['details']['mapHeightPointsFull'] - ((currLatitude - trackDetails['details']['minLatitude'])/trackDetails['details']['heightDegreesPerMapPoint'] + \
            trackDetails['details']['primaryMarginSize'])

    return (xPos,yPos)

#
# Create the map as an image
#
def drawMap():

    print('Running drawMap()')

    #
    # Set properties
    #
    trackDetails = {
                        'track_sections':{},
                        'hex_values':{}
                    }
    minMaxSet = False
    mapFont = ImageFont.truetype(legendFontFilename, legendFontSize)
    legendTextMaxWidth = 0
    legendTextMaxHeight = 0
    legendScratchPad = Image.new('RGB', (1000,500), ImageColor.getrgb("#ffffff"))
    legendScratchPadContext = ImageDraw.Draw(legendScratchPad)
    sectionCnt = 0

    # Create cursor for updating section details
    cursorUpdateSectionDetails = DBConnection.cursor(dictionary=True)

    #
    # Load the 'fmt_track_sections' table into a dictionary
    #
    cursorSectionList = DBConnection.cursor(dictionary=True)
    sqlQuery = 'select * from fmt_track_sections'
    cursorSectionList.execute(sqlQuery)
    knownSections = {}
    for currSection in cursorSectionList:
        knownSections.update({str(currSection['id']):currSection})
    #print('\n\nknownSections:\n' + json.dumps(knownSections, indent=4, sort_keys=True, default=str))

    #
    # Load track details csv into dict
    #
    with open(trackDetailsFilename, mode='r') as trackDetailsCSV:
        detailsReader = csv.DictReader(trackDetailsCSV)

        # Remap header names to dict keys
        remappedHeaders = []
        for headerName in detailsReader.fieldnames:
            remappedHeaders.append(mapHeaderToKeys[headerName])
        detailsReader.fieldnames = remappedHeaders
        for currRow in detailsReader:
            sectionCnt += 1
            currRowID = int(currRow['id'])
            currRowIDStr = str(currRow['id'])

            # Check current ID hasn't been duplicated
            if currRowID in trackDetails['track_sections']:
                print('\n\n#')
                print('# Error with input details for \'' + trackDetailsFilename + '\'.')
                print('#')
                print('# There are at least two rows in this file with the ID \'' + currRowIDStr + '\'.')
                print('#')
                exit(1)

            # Check current hex hasn't been duplicated
            if currRow['color_hex'].lower() in trackDetails['hex_values']:
                print('\n\n#')
                print('# Error with input details for \'' + trackDetailsFilename + '\'.')
                print('#')
                print('# There are at least two rows in this file with the hex value \'' + currRow['color_hex'].lower() + '\'.')
                print('#')
                exit(1)

            #
            # Check station type is one of:
            #
            # N : Normal section of track
            # S : Station
            # Y : Yard
            # I : Interchange
            # E : End of line
            #
            if currRow['type'] not in ['N', 'S', 'Y','I','E']:
                print('\n\n#')
                print('# Error with input details for \'' + trackDetailsFilename + '\'.')
                print('#')
                print('# The row with ID \'' + currRowIDStr + '\' has a value of \'' + currRow['type'] + '\', for Section Type.')
                print('# This should be either \'N\' for Normal, \'S\' for Station, \'I\' for Interchange, or \'Y\' for Yard.')
                print('#')
                exit(1)

            trackDetails['track_sections'].update({currRowID:currRow})
            trackDetails['track_sections'][currRowID].update({'section_points':{}})
            sectionTitle = trackDetails['track_sections'][currRowID]['title']
            
            # bearing must either be between 0 - 359 or 'na
            bearingOK = False
            if currRow['bearing_to_britomart'].isdigit() or currRow['bearing_to_britomart'] == '-1':
                sectionBearing = int(currRow['bearing_to_britomart'])
                if ((sectionBearing >= 0) and ( sectionBearing <= 360)) or ( sectionBearing == -1):
                    bearingOK = True
                    bearingInt = int(sectionBearing)
            if not bearingOK:
                print('\n\n#')
                print('# Error with input details for \'' + trackDetailsFilename + '\'.')
                print('#')
                print('# The problem was the line with \'ID\': ' + currRowIDStr)
                print('# The value for bearing: \'' + currRow['bearing_to_britomart'] + '\'.')
                print('#')
                print('# This should either be a value from 0 to 360 or \'-1\'')
                print('#')
                exit(1)
            
            # Save under hex values so we can search via hex
            trackDetails['hex_values'].update({
                currRow['color_hex'].lower():{
                    'color_name':currRow['color_name'],
                    'id':currRow['id'],
                    'line':currRow['line'],
                    'title':currRow['title'],
                    'type':currRow['type'],
                    'bearing_to_britomart':currRow['bearing_to_britomart'],
                    'bearing_to_britomart_int':bearingInt,
                    'color_hex':currRow['color_hex'],
                }
            })

            #
            # Get text sizes for this section 
            # - The width of the title will be different for each section, so need to work this out
            #
            titleSize = legendScratchPadContext.textbbox((0,0),sectionTitle, font=mapFont)
            titleWidth = titleSize[2] - titleSize[0]
            titleHeight = titleSize[3] - titleSize[0]
            if titleWidth > legendTextMaxWidth:
                legendTextMaxWidth = titleWidth
            if titleHeight > legendTextMaxHeight:
                legendTextMaxHeight = titleHeight
            
            #
            # At this point we are looking at one segment of the track
            # The "points_str" key is a string of point tupples of the form:
            #
            #      '-36.86784253513259, 174.60252827919632;-36.86822630052527, 174.60521925145653'
            #
            # We need to change this to float so we can do calculations with it
            #
            pointCnt = 0
            for currPoint in currRow['points_str'].split(';'):
                #
                # Be careful of trailing semicolons
                #
                if len(currPoint.strip()) > 0:
                    pointCnt += 1
                    currPointSplit = currPoint.split(',')

                    # Check there is both a latitude and longitude value
                    if len(currPointSplit) != 2:
                        print('\n\n#')
                        print('# Error with input details for \'' + trackDetailsFilename + '\'.')
                        print('#')
                        print('# The problem was the line with \'ID\': ' + currRowIDStr)
                        print('# Points value causing an issue was: \'' + currPoint + '\'.')
                        print('#')
                        print('# There should have been exactly two comma separated values but there weren\'t')
                        print('#')
                        exit(1)
                    #
                    # Check this point contains valid details
                    # As in they must be valid float values
                    #
                    try:
                        a = float(currPointSplit[0].strip())
                        a = float(currPointSplit[1].strip())
                    except ValueError as err:
                        print('\n\n#')
                        print('# Error with input details for \'' + trackDetailsFilename + '\'.')
                        print('#')
                        print('# The problem was the line with \'ID\': ' + currRowID)
                        print('# Points value causing an issue was: \'' + currPoint + '\'.')
                        print('#')
                        print('The error returned was ' + str(err))
                        exit(1)

                    currLatitude = float(currPointSplit[0].strip())
                    currLongitude = float(currPointSplit[1].strip())
                    trackDetails['track_sections'][currRowID]['section_points'].update({pointCnt:{'latitiude':currLatitude,'longitude':currLongitude}})

                    #
                    # Get min and max values for latitude and longitude
                    #
                    if not minMaxSet:
                        minLatitude = maxLatitude = currLatitude
                        minLongitude = maxLongitude = currLongitude
                        minMaxSet = True
                    else:
                        if currLatitude < minLatitude:
                            minLatitude = currLatitude
                        if currLatitude > maxLatitude:
                            maxLatitude = currLatitude
                        if currLongitude < minLongitude:
                            minLongitude = currLongitude
                        if currLongitude > maxLongitude:
                            maxLongitude = currLongitude
        
            #
            # Update or add this to the fmt_track_sections table as required
            #
            if currRowIDStr in knownSections:
                knownSectionDetails = knownSections[currRowIDStr]
                if  (knownSectionDetails['bearing_to_britomart'] != bearingInt) or \
                    (knownSectionDetails['title'] != currRow['title']) or \
                    (knownSectionDetails['type'] != currRow['type']):
                    print('Discrepency found for ' + str(currRowIDStr) + ' = ' + str(knownSectionDetails))
                    print('currRow[\'bearing_to_britomart\'] = ' + str(bearingInt))
                    print('currRow[\'title\'] = ' + str(currRow['title']))
                    print('currRow[\'type\'] = ' + str(currRow['type']))
                    try:
                        
                        updateQuery = ''' UPDATE fmt_track_sections SET title = %s, type = %s, bearing_to_britomart = %s
                                        WHERE id = %s'''
                        updateValues = (currRow['title'],
                                        currRow['type'],
                                        bearingInt,
                                        int(currRowIDStr),
                                        )
                        cursorUpdateSectionDetails.execute(updateQuery, updateValues)
                        DBConnection.commit()
                    except mysql.connector.Error as err:
                        print('#')
                        print('# Error updating section details, in \'fmt_track_sections\', in database:')
                        print('#')
                        print('# ' + str(err))
                        print('#')
                        exit(1)
            else:
                print('NOT FOUND section \'' + currRowIDStr + '\'')
                try:                        
                    insertQuery = ''' INSERT INTO fmt_track_sections 
                                      (id,
                                       title,
                                       type,
                                       bearing_to_britomart
                                      ) 
                                      VALUES ( %s, %s, %s, %s )'''
                    insertValues = (currRowID,
                                    currRow['title'],
                                    currRow['type'],
                                    bearingInt,
                                    )
                    cursorUpdateSectionDetails.execute(insertQuery, insertValues)
                    DBConnection.commit()
                except mysql.connector.Error as err:
                    print('#')
                    print('# Error updating section details, in \'fmt_track_sections\', in database:')
                    print('#')
                    print('# ' + str(err))
                    print('#')
                    exit(1)




    trackDetails.update({'minLatitude':minLatitude})  
    trackDetails.update({'maxLatitude':maxLatitude})
    trackDetails.update({'minLongitude':minLongitude})
    trackDetails.update({'maxLongitude':maxLongitude})

    #
    # The map co-ordinates/points are in latitude and longitude, but 
    # we need to print it in meters. Thus
    #
    latitudeKmPerDegree = haversine((minLatitude,minLongitude),((minLatitude + 1),minLongitude), unit=Unit.KILOMETERS)
    longitudeKmPerDegree = haversine((minLatitude,minLongitude),(minLatitude,(minLongitude + 1)), unit=Unit.KILOMETERS)

    mapWidthKm = longitudeKmPerDegree*(trackDetails['maxLongitude'] - trackDetails['minLongitude'])
    mapPointSizeKm = mapWidthKm/mapWidthPoints
    mapHeightKm = latitudeKmPerDegree*(trackDetails['maxLatitude'] - trackDetails['minLatitude'])
    heightDegreesPerMapPoint = mapPointSizeKm/latitudeKmPerDegree
    widthDegreesPerMapPoint = mapPointSizeKm/longitudeKmPerDegree
    mapHeightPoints = mapHeightKm/mapPointSizeKm
    mapHeightPointsFull = int(mapHeightPoints + (primaryMarginSize*2))
               
    if (minLatitude == maxLatitude) or (minLongitude == maxLongitude):
        print('\n\n#')
        print('# Error with input details for \'' + trackDetailsFilename + '\'.')
        print('#')
        print('# Something is wrong, the width or height of the map is zero.')
        print('#')
        print('minLatitude: ' + str(minLatitude))
        print('maxLatitude: ' + str(maxLatitude))
        print('minLongitude: ' + str(minLongitude))
        print('maxLongitude: ' + str(maxLongitude))
        exit(1)
  
    print('longitudeKmPerDegree: ' + str(longitudeKmPerDegree))
    print('latitudeKmPerDegree:' + str(latitudeKmPerDegree))
    print('mapWidthKm: ' + f'{mapWidthKm:f}')
    print('mapPointSizeKm: ' + f'{mapPointSizeKm:f}')
    print('map width degrees ' + str((trackDetails['maxLongitude'] - trackDetails['minLongitude'])))
    print('mapHeightKm: ' + str(mapHeightKm))
    print('heightDegreesPerMapPoint: ' + str(heightDegreesPerMapPoint))
    print('mapHeightPoints: ' + str(mapHeightPoints))

    #
    # Create the image
    #

    # First legend stuff
    legendRowHeight = legendTextMaxHeight + legendRowSpace
    legendColumnWidth = legendTextMaxWidth + (legendBoxWidth + legendBoxMargin + legendRightMargin)
    legendRowsPerColumn = math.floor(mapHeightPoints/(legendTextMaxHeight + legendRowSpace))
    legendColumnCnt = math.ceil(sectionCnt/legendRowsPerColumn)
    legendTotalWidth = (legendColumnCnt*legendColumnWidth) + primaryMarginSize
    
    print('legendRowsPerColumn: ' + str(legendRowsPerColumn))
    print('legendColumnCnt: ' + str(legendColumnCnt))

    imgTotalWidth = mapWidthPoints + (primaryMarginSize*2) + legendTotalWidth
    imgFullHeight = mapHeightPointsFull
    trackMap = Image.new('RGB', (imgTotalWidth,imgFullHeight), ImageColor.getrgb("#ffffff"))
    trackMapContext = ImageDraw.Draw(trackMap)

    #
    # Update trackDetails dict with important details
    #
    trackDetails.update({
        'details':{
            'minLongitude':minLongitude,
            'minLatitude':minLatitude,
            'widthDegreesPerMapPoint':widthDegreesPerMapPoint,
            'heightDegreesPerMapPoint':heightDegreesPerMapPoint,
            'primaryMarginSize':primaryMarginSize,
            'legendTotalWidth':legendTotalWidth,
            'mapHeightPointsFull':mapHeightPointsFull,
        }
    })

    # Step through all the track section
    for currSection in trackDetails['track_sections']:
        #print('\n ' + trackDetails['track_sections'][currSection]['title'])
        currSectionPoints = []
        for currPoints in range(1, len(trackDetails['track_sections'][currSection]['section_points']) + 1):
            currLatitude = trackDetails['track_sections'][currSection]['section_points'][currPoints]['latitiude']
            currLongitude = trackDetails['track_sections'][currSection]['section_points'][currPoints]['longitude']
            lineColor = trackDetails['track_sections'][currSection]['color_hex']
            imgCoords = geographicLocToImgLoc(currLatitude, currLongitude, trackDetails)
            currSectionPoints.append(imgCoords)

        trackMapContext.line(currSectionPoints, fill=ImageColor.getrgb(lineColor), width=lineWidthPt, joint='curve')

    # 
    # Draw the legend
    #
    sectionCnt = -1
    for currRowID in trackDetails['track_sections']:
        sectionCnt += 1
        yPosOffset = (sectionCnt % legendRowsPerColumn)*legendRowHeight   # Remember "5 % 3" means 5 modulus 3
        xPosOffset = int(sectionCnt/legendRowsPerColumn)*(legendColumnWidth + legendBoxMargin)
        sectionTitle = trackDetails['track_sections'][currRowID]['title']
        sectionColor = trackDetails['track_sections'][currRowID]['color_hex']
        trackMapContext.text(((primaryMarginSize + xPosOffset + legendBoxWidth + legendBoxMargin),(primaryMarginSize + yPosOffset)), sectionTitle, font=mapFont, 
                             fill =ImageColor.getrgb('black'))
        titleSize = trackMapContext.textbbox((0,0),sectionTitle , font=mapFont)
        titleWidth = titleSize[2] - titleSize[0]
        titleHeight = titleSize[3] - titleSize[0]

        trackMapContext.rectangle((((primaryMarginSize + xPosOffset), (primaryMarginSize + yPosOffset + legendBoxHeightOffset)),
                                    ((primaryMarginSize+ xPosOffset + legendBoxWidth),(primaryMarginSize + yPosOffset + legendBoxHeightOffset + legendBoxWidth))), 
                                    fill=ImageColor.getrgb(sectionColor), outline='black', width=1)

        yPosOffset += legendTextMaxHeight + legendRowSpace  
    trackMap.save(trackMapImgFilename)

    return trackDetails, trackMap

routeDetails = loadTrainRoutes()
specialTrainDetail = loadSpecialTrainDetails()
# Draw the map image
trackDetails, mapContext = drawMap() 
trainDetails = getCurrVehicleDetails(specialTrainDetail)
trainDetails = additionalCalculations(trainDetails,routeDetails)

#print('\n\ncurrVehicleDetails:\n' + json.dumps(currVehicleDetails, indent=4, sort_keys=True, default=str))

#updateDB(trainDetails)
#print('\n\nrouteDetails:\n' + json.dumps(routeDetails, indent=4, sort_keys=True, default=str))
#print('\n\ntrainDetails:\n' + json.dumps(trainDetails, indent=4, sort_keys=True, default=str))


#
# Close things off
#
DBConnection.close()



# trackDetails, mapContext = drawMap()  
# #print('\n\ntrackdetails:\n' + json.dumps(trackDetails, indent=4, sort_keys=True, default=str))      
# #latWidth = (trackDetails['maxLatitude'] - trackDetails['minLatitude'])/mapWidthPoints
# #print('latWidth = ' + f'{latWidth:f}')
# currLatitude = -36.893725
# currLongitude = 174.70295666666667
# imgCoords = geographicLocToImgLoc(currLatitude, currLongitude, trackDetails)
# print('Coordinates:' + str(currLatitude) + ', ' + str(currLongitude))
# print('imgCoords: ' + str(imgCoords))
# #mapContext.show()
# rgbValue = mapContext.getpixel(imgCoords )
# print('rgbValue: ' + str(rgbValue))
# hexValue = '#{:02x}{:02x}{:02x}'.format(*rgbValue).lower()     # Lowercase for searching

# if hexValue in list(trackDetails['hex_values']):
#     print('Location: ' + trackDetails['hex_values'][hexValue]['title'])
# else:
#     print('HEX Value NOT FOUND')