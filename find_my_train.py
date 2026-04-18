#
# This script has been crafted for Python 3.6, which is admittedly a little old but
# at the moment I can't upgrade on my CentOS host.
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
#
#                  ==== ENSURE THE REQUIRED rpms are installed FIRST====
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
# Pip install
# -----------
# Module install should thus look like:
#
#     pip install protobuf==3.19.6 mysql-connector-python==8.0.27 pytz requests pillow==8.4.0 haversine
#
# Secrets config file
# -------------------
# This is the config file for confidential details
#
# You should locate this in a folder outside of where you have checked out this and any other
# repos to prevent accidentially committing this file to git.
# 
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
# Misc notes
# ==========
# Odometer   - This is measured in meters NOT kilometers
#
# Geographical coordinates
# ------------------------
#   Longitude = East <> West
#   Latitude = North <> South
#
# Crontab
# -------
# If we set this as "*/10 * * * *", then it will fire every tenth minute, so not 
# from now but at say 6:00, 6:10, 6:20, and so on
#
# Saving configurations to the DB
# -------------------------------
# This script is designed to work hand in hand with the "Find My Train" post in WordPress.
# There are configurations that need to make their way to post, most notably the default train setting.
# Although it's not the typical way of doing things, the most pragmatic way of passing configuration is via the DB because
# the scripts on the page can easily read from the DB.
# 

import os
import csv
import json
import math
import requests
import mysql.connector
import datetime
from datetime import datetime, timedelta
import pytz
import configparser
import inspect
import time
import traceback
import copy
import random
from urllib.parse import quote
from requests.exceptions import ConnectionError
from PIL import Image, ImageDraw, ImageColor, ImageFont
from haversine import haversine, Unit                       # Used to work out meters to latitude/longitude
import mysql.connector 
import re

#
# User properties
#
secretsConfFilename = os.path.dirname(os.getcwd()) + '/find_my_train.ini'
trackDetailsFilename = "Auckland track details.csv"
trackMapImgFilename = "track_map.png"
specialTrainsFilename = 'Special Trains.csv'
stationsFilename = 'stations.csv'  # CSV file containing station details for import
mapWidthPoints = 4000 
imgMarginPercent = 5
lineWidthPixels = 1  # Width of track lines in pixels
legendFontSize = 40  # Pixels
legendRowSpace = 5   # Pixels
legendFontFilename = 'NotoSans-Regular.ttf'
legendBoxWidth = 40
legendBoxMargin = 10
legendBoxHeightOffset = 7
legendRightMargin = 5
lineEndMarginPercent = 0.5
maxSearchRadius = 100   # was 5
stdSearchRadius = 10
maxTimestampDiffBetweenMultiTrainsSec = 90
timeZoneStr = 'Pacific/Auckland'
timeRetainMostRecentDataMinutes = 60  
refreshStopDetailsSec = 100
endOfTripTimeoutMin = 5  # If a train has been assigned a trip id, we will move that train to Out Of Service if it has not been seen for this number of minutes. 
defaultTrainNumber = "714"
defaultLocation = "89" # Waitemata
artificialLocations = ['-36.84448,174.76915',]  # For some reason AT set these locations, which are clearly not the actual locations of the trains
locationHistoryRetentionPeriodMin = 10  # How many minutes of historical location data to retain in the DB
commonTimestampOffsetSec = 60  # Offset in seconds for calculating common historical timestamp (e.g., 60 = 1 minute ago)
maxMetersBetweenTrainsInASet = 300 # Maximum distance in meters between trains to be considered part of the same train set
maxRetensionTrainSetMinutes = 2880  # Truncate fmt_train_sets where last updated is over this many minutes ago
maxTrainSetHistoryEntries = 10  # Maximum number of historical train set entries to retain per train
minSeparationForFrontTrainsMeters = 10  # When determining the train in front ignore results where trains are separated by this many meters or less.
maxPrevFrontTrainRecordsToKeep = 75 # The maximum number of previous front train numbers
parkedTrainInactivityMin =10 # We need to do cleanups of parked trains, but it can be difficult to work out if a train is parked. If a train has been stationary for this number of minutes then it's parked 
sectionTypesToIgnoreForTrainSets = ['I', 'Y', 'E']  # Section types to ignore when identifying train sets: 'I' (Interchange), 'Y' (Stabling Yard), 'E' (End of Line)    

atVehiclePosURL = 'https://api.at.govt.nz/realtime/legacy/vehiclelocations'
atAllStopsURL = 'https://api.at.govt.nz/gtfs/v3/stops'
tripUpdatesURL = 'https://api.at.govt.nz/realtime/legacy/'
baseTripsURL = 'https://api.at.govt.nz/gtfs/v3/trips/'
routesURL = 'https://api.at.govt.nz/gtfs/v3/routes'

# Info retention period for a train that is/was part of 6 carridge train. 
# Period measured in number of track sections  
multiTrainDetailsMaxRetentionCount = 4  

# Frequency of api calls, ie. how many seconds between api calls
freqApiCallsSec = 30 
totalScriptTimeMin = 10

# How much buffer we want at the end of a cycle of api calls, this is to 
# prevent overlap between crontab runs, should be greater than the expected 
# run time for the api processing
scriptBufferTimeSec = 10   
retainLocationRowsDays = 7

# How long to keep trip details - this should never be more than 1 day
retainTripDetailsDays = 1


#
# Misc 
#
trainSets = {}     # Dictionary to store identified train sets
                   # Structure: {set_id: {'trains': [train1, train2, ...], 'heading': 'Y'/'N', 'section': section_id}}
specialTrainDetails = {}
nextEventID = -1
apiTimestampPosix = 0
rawTrainDetails = {'train':{}}
fullUp2DateTrainLocations = {}  # Dictionary to store historical positions for each train
trainSetCriteria = {
                        # This part of the critea involves looking at the train sets this train has been in.
                        # Imagine the train set looks like:
                        #
                        # "previous_train_sets": [
                        #                             [
                        #                                 "334",
                        #                                 "471",
                        #                                 "524",
                        #                                 "347"
                        #                             ],
                        #                             [
                        #                                 "157",
                        #                                 "701"
                        #                             ],
                        #                             [
                        #                                 "162",
                        #                                 "524"
                        #                             ],
                        #                             [
                        #                                 "157",
                        #                                 "524"
                        #                             ]
                        #                         ],
                        #
                        # Imagine the rule was that for a train to be considered in a set with our train it would 
                        # need to have been in 2 of the 3 our trains most recent train sets. Using that rule only train
                        # 524 would qualify.
                        # 
                        # In the above example 'no_prev_sets_to_consider' would be set to 3, and 'min_no_sets_to_qualify'
                        # would be set to 2.
                        #
                        'no_prev_sets_to_consider':10, 
                        'min_no_sets_to_qualify':4,  
                    }  
#
# Define the rules for when a train is in front.
#
# It is a series of strings that look like '2/3'
# For example '2/3' means that 2 of the 3 most recent front trains as defined
# by newTrainSetHistory.
# newTrainSetHistory would look something like:
#
#  newTrainSetHistory = '578,578,644,578,644'
#
#  Order the rules in the sequence you want them evaluated
#  =======================================================
#
#  Once a rule has been satisified the rule evaluations stop.
#
frontTrainRules = ['4/6/*','2/3/+','2/2/~'] 
trackDetails = {
                    'track_sections':{},
                    'hex_values':{}
                }
trainDetails = {
                    'train':{},
                    'section':{},
                }
eventLog =  {
                'error':{
                    'maxRowsRetainTotal':30,
                    'maxRowsRetainPerTitle':3,
                },
                'warn':{
                    'maxRowsRetainTotal':30,
                    'maxRowsRetainPerTitle':3,
                },
                'info':{
                    'maxRowsRetainTotal':70,
                    'maxRowsRetainPerTitle':-1,   # If -1 don't truncate based on title
                },
            }

# There is a problem with string replacements for example if we want to replace 'Brit' with 'Waitemata'
# but what happens if the string "Britomart" is in the headsign? We don't want to change that to "Waitemataomart"
# Since we want to replace "Britomart" with "Waitemata" we need to do the longer strings first.
headsignStringReplacementa = {
                                'Britomart': 'Waitemata',
                                'NKT': 'Newmarket',
                                'PNR': 'Penrose',
                                'ELL': 'Ellerslie',
                                'GRN': 'Greenlane',
                                'REM': 'Remuera',
                                'OHU': 'Otahuhu',
                                'ONE': 'Onehunga',
                                'PPK': 'Papakura',
                                'Brit': 'Waitemata',
                                ' ,' : ',',  # Remove space before comma
                            }
logInfoMsg = ''
lastApiCallStartTime = None

#
# Load secrets from ini file
#
print('secretsConfFilename = ' + str(secretsConfFilename) )    # Have to use "print" because "eventLogger" is not available yet
secretsConfig = configparser.ConfigParser()
secretsConfig.read(secretsConfFilename)

# Set properties from secrets
dbHostname = secretsConfig['Database']['dbHostname']
dbName = secretsConfig['Database']['dbName']
dbUser = secretsConfig['Database']['dbUser']
dbUserPassword = secretsConfig['Database']['dbUserPassword']

# Create DB connection
try:
    DBConnection = mysql.connector.MySQLConnection(user=dbUser, 
                                    password=dbUserPassword,
                                    host=dbHostname,
                                    database=dbName)
except mysql.connector.Error as err:
    eventMsg = str(err)
    eventLogger('error', eventMsg, 'Error setting a DB connection', str(inspect.currentframe().f_lineno))

#
# Calculate distance between two lat/lon points using Haversine formula
#
def calculate_distance_km(lat1, lon1, lat2, lon2):
    """Calculate distance between two lat/lon points using Haversine formula"""
    R = 6371  # Earth's radius in kilometers
    
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

#
# Convert api timestamps to datetime
#
def posixtoDateTime(posixDate):
    return datetime.fromtimestamp(posixDate, pytz.timezone(timeZoneStr))


#
# Update the 'fmt_event_log' table in the DB
#
# Note that 'columnDetails' is dictionary
#
#    columnDetails = {
#                       'column_name_a':columnNameA,
#                       'column_name_b':columnNameB,
#                       'column_name_c':columnNameC,
#                    }
#
def updateEventLogInDB(columnDetails):
    
    global apiTimestampPosix
    global eventLog
    global lastApiCallStartTime
    global nextEventID

    apiTimestampDateTime = posixtoDateTime(apiTimestampPosix)
    rawTrainDetailsPretty = json.dumps(rawTrainDetails, indent=4, sort_keys=True, default=str)
    trainDetailsPretty = json.dumps(trainDetails, indent=4, sort_keys=True, default=str)

    #
    # Create a event log entry in the table fmt_event_log
    #      
    # We always add the posix time, datetime, and rawTrainDetails, to each event
    # log row
    #   
    placeholders = '%(api_timestamp_posix)s, %(api_timestamp_datetime)s, %(raw_train_details)s, %(train_details)s, %(api_cycle_start)s, %(event_id)s'
    colNameList = 'api_timestamp_posix, api_timestamp_datetime, raw_train_details, train_details, api_cycle_start, event_id'
    columnData = {
                    'api_timestamp_posix':apiTimestampPosix, 
                    'api_timestamp_datetime':apiTimestampDateTime, 
                    'raw_train_details':rawTrainDetailsPretty,
                    'train_details':trainDetailsPretty,
                    'api_cycle_start':lastApiCallStartTime,
                    'event_id':nextEventID,
                    }
    for currColumn in columnDetails:
        placeholders += ', %(' + currColumn + ')s'
        colNameList += ', ' + currColumn
    for currColumnDetail in columnDetails:
        columnData.update({currColumnDetail:columnDetails[currColumnDetail]})
    insertQuery = 'INSERT INTO fmt_event_log (' + colNameList + ') VALUES (' + placeholders + ')'

    #
    # Note we are not trapping these DB errors as this function is the mechanism that
    # saves error events - so catch 22
    #
    # If this fails then it will fail to log to the DB which means we won't see it
    # and will have to run from the command line anyway :-(
    # 
    eventCursor = DBConnection.cursor()
    eventCursor.execute(insertQuery, columnData)
    DBConnection.commit()

    #
    # The nextEventID should increment correct but if for some reason
    # we do more than one insert to the fmt_event_log table for an api call loop we need to make sure
    # the event_id is unique.
    #
    # Just to be safe we will increment it here as well
    #
    nextEventID += 1

    logType = columnDetails['event_type'] 

    #
    # Truncate similar records
    #
    if eventLog[logType]['maxRowsRetainPerTitle'] != -1:
        eventLogRowsToRetain = eventLog[logType]['maxRowsRetainPerTitle']
        trucateQuery = '''
                        DELETE FROM fmt_event_log  
                        WHERE event_type = %s  
                        AND event_title = %s
                        AND event_id <= (
                            SELECT event_id 
                            FROM (
                                SELECT * 
                                FROM fmt_event_log 
                                WHERE event_type = %s 
                                AND event_title = %s
                                ORDER BY event_id 
                                DESC LIMIT %s,1
                            )
                        AS oldest_record
                        );'''
        truncateValues = (  
                            logType,
                            columnDetails['event_title'],
                            logType,
                            columnDetails['event_title'],
                            eventLogRowsToRetain,
                        )
        eventCursor.execute(trucateQuery, truncateValues)
        DBConnection.commit()

    
    #
    # Truncate old records
    #
    eventLogRowsToRetain = eventLog[logType]['maxRowsRetainTotal']
    trucateQuery = '''
                    DELETE FROM fmt_event_log  
                    WHERE event_type = %s AND 
                        event_id  < (
                            SELECT event_id 
                            FROM (
                                SELECT * 
                                FROM fmt_event_log 
                                WHERE event_type = %s 
                                ORDER BY event_id DESC 
                                LIMIT %s,1) as oldest_record)'''
    truncateValues = (  
                        logType,
                        logType,
                        eventLogRowsToRetain,
                     )
    eventCursor.execute(trucateQuery, truncateValues)
    DBConnection.commit()
    eventCursor.close()

#
# Master logger mechanism
#
def eventLogger(eventType, eventMsg, eventTitle, eventLineNo):

    global logInfoMsg

    #
    # Because this script has to work with Python 3.6 we don't
    # have access to the 'case' statement :-(
    #
    if eventType == 'info':
        #
        # 'info_update'
        #
        # This is the equivalent of the console log which would be included in 
        # any other event types.
        #
        # This event type simply prints to the console and updates the info string
        #
        logInfoMsg += '\n' + eventMsg
        print(eventMsg)

    elif eventType ==  'info_close':
        #
        # This should only be run at the end of a cycle, so at the end of
        # processing one api call
        #
        # Title and message are non configurable
        #
        logInfoMsg += '\n' + eventMsg
        eventTitle = "Successfully completed"

        currColumnDetails = {
                            'event_type':'info',
                            'event_title':eventTitle,
                            'event_msg':logInfoMsg,
                            }

        updateEventLogInDB(currColumnDetails)

        # As we are now doing multiple api  cycles we need to clean up
        logInfoMsg = ''

    elif eventType ==  'error':
        #
        # An error has occurred so log the details and
        # exit
        #
        errorMessage =  '\n' + \
                        '#\n' + \
                        '# Fatal Error: ' + eventTitle + '\n' + \
                        '# =============' + '='*len(eventTitle) + '\n' + \
                        '# Error reported at line ' + eventLineNo + ' of this script.\n' + \
                        '#\n'
        
        for thisLine in eventMsg.split('\n'):
            errorMessage += '# ' + thisLine + '\n'
        errorMessage += '# '
        logInfoMsg += '\n' + errorMessage
        currColumnDetails = {
                            'event_type':'error',
                            'event_title':eventTitle,
                            'event_msg':logInfoMsg,
                            }
        updateEventLogInDB(currColumnDetails)

        print(errorMessage)
        exit(1)

    elif eventType ==  'warn':
        #
        # An warning has occurred so log the details 
        #
        errorMessage =  '\n' + \
                        '#\n' + \
                        '# Warning: ' + eventTitle + '\n' + \
                        '# =========' + '='*len(eventTitle) + '\n' + \
                        '# Warning reported at line ' + eventLineNo + ' of this script.\n' + \
                        '#\n'
        
        for thisLine in eventMsg.split('\n'):
            errorMessage += '# ' + thisLine + '\n'
        errorMessage += '# '
        logInfoMsg += '\n' + errorMessage
        currColumnDetails = {
                            'event_type':'warn',
                            'event_title':eventTitle,
                            'event_msg':logInfoMsg,
                            }
        updateEventLogInDB(currColumnDetails)


#
# Enable full script 'try' block
# ==============================
#
# Because this script is running from cron jobs we need to capture any completely
# unexpected failure and put this into the event log table.
#
# Thus this block cover most of the script
#
# Its not full proof, obviously, since if there is an error with the DB connection we
# won't even be able to log an entry to the event log table.
#
try:

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


    ################
    #
    # Load the api keys
    # I have multiples keys because they have a quota which sometimes I go over, so I
    # have multiple keys as a standby
    #
    # The master list of keys is in the ini file
    # However the DB retains details about which keys have expired
    #
    # The process I follow is a little clunky but it's easy to follow and is robust
    #

    # Load the keys from the ini file
    apiIniFileKeys = {'keys':{}}
    baseListAllKeys = ''
    for currKey in secretsConfig.items('at_api_keys'):
        apiIniFileKeys['keys'].update({
            currKey[0]:{
                'value':currKey[1],
            }
        })
        if baseListAllKeys == '':
            baseListAllKeys += '"' + currKey[0] + '"'
        else:
            baseListAllKeys += ',"' + currKey[0] + '"'

    # Delete all api keys in the DB that are not listed in the ini file
    apiKeyCursor = DBConnection.cursor(dictionary=True)
    sqlQuery = '''  DELETE FROM 
	                    fmt_api_keys
                    WHERE 
	                    api_key_name NOT IN (''' + baseListAllKeys + ''') '''
    try:
        apiKeyCursor.execute(sqlQuery)
        DBConnection.commit()
    except mysql.connector.Error as err:
        eventMsg = str(err)
        eventLogger('error', eventMsg, 'Error removing api key from database table \'fmt_api_keys\' if these are not in the ini file.', str(inspect.currentframe().f_lineno))
        exit(1)

    # Get all keys currently in the DB
    sqlQuery = '''  SELECT
                        *
                    FROM 
                        fmt_api_keys'''
    try:
        apiKeyCursor.execute(sqlQuery)
    except mysql.connector.Error as err:
        eventMsg = str(err)
        eventLogger('error', eventMsg, 'Error getting a list of api keys in the DB \'fmt_api_keys\'.', str(inspect.currentframe().f_lineno))
        exit(1)
    apiKeyDetails = {}
    for currKey in apiKeyCursor:
        apiKeyDetails.update({currKey['api_key_name']:{
            'api_key_name':currKey['api_key_name'],
            'live_after_posix':currKey['live_after_posix'],
            'key_value':currKey['key_value'],
        }})
    
    #
    # If a key value in the ini file is different to the DB that means
    # the key value has been changed.
    #
    # In this case delete the row from the DB and reinsert it
    #
    keysToDeleteList = []
    for currKey in apiIniFileKeys['keys']:
        if currKey in apiKeyDetails:
            if apiKeyDetails[currKey]['key_value'] != apiIniFileKeys['keys'][currKey]['value']:
                # Key value has changed
                # We can't delete here because that would mean we change the dictionary object
                # that we are iterating through - which is not allowed for obvious reasons
                keysToDeleteList.append(currKey)
    for keyToDelete in keysToDeleteList:
        del apiKeyDetails[keyToDelete]
        # Delete from DB
        try:
            deleteQuery = '''   DELETE FROM 
                                    fmt_api_keys 
                                WHERE 
                                    api_key_name = %s'''
            insertValues = (keyToDelete, )
            apiKeyCursor.execute(deleteQuery, insertValues)
            DBConnection.commit()              
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error deleting api keys in table \'fmt_api_keys\'', str(inspect.currentframe().f_lineno))
            exit(1)      

    # Insert all missing keys
    for currKey in apiIniFileKeys['keys']:
        if currKey not in apiKeyDetails:
            try:
                insertQuery = ''' INSERT INTO fmt_api_keys
                                (api_key_name,
                                live_after_posix,
                                key_value
                                )
                                VALUES ( %s, 0, %s)'''
                insertValues = (currKey, apiIniFileKeys['keys'][currKey]['value'])
                apiKeyCursor.execute(insertQuery, insertValues)
                DBConnection.commit()
                apiKeyDetails.update({currKey:{
                    'api_key_name':currKey,
                    'live_after_posix':0,
                    'key_value':apiIniFileKeys['keys'][currKey]['value'],
                }})                
            except mysql.connector.Error as err:
                eventMsg = str(err)
                eventLogger('error', eventMsg, 'Error inserting new api keys in table \'fmt_api_keys\'', str(inspect.currentframe().f_lineno))
                exit(1)             

    mapSpecialTrainHeaderToKeys = {
                                    'Train Number':'train_number',
                                    'Custom Name':'custom_name',
                                    'Featured Image URL':'train_featured_img_url',
                                    'Small Image URL':'train_small_img_url',
                                    'Description':'train_description',
                                    }
    mapRouteDetailsHeaderToKeys = {
                                    'ID':'route_id',
                                    'AT route id':'at_route_id',
                                    'Route Name To Britomart':'route_name_to_britomart',
                                    'Route Name From Britomart':'route_name_from_britomart',
                                    }
    stopDetails = {}




    #
    # we need to ensure the fmt_trips table has an entry for out of service trains
    # We also need to be able to update the values of this entry, which is why we update the details
    # if a record already exists
    #
    def ensureOOSTripRecordExists():
        cursorOOSQuery = DBConnection.cursor(dictionary=True)
        oosTripIDStr = 'oos'
        outOfServiceTripDetails = (oosTripIDStr, 'Out Of Service', 'Out Of Service', 'Out Of Service', quote('Out Of Service'), routeDetails['at_route_id']['oos']['route_id'], 0)
        sqlQuery = 'SELECT * FROM fmt_trips WHERE trip_id = \'' + oosTripIDStr + '\';'
        try:
            cursorOOSQuery.execute(sqlQuery)
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error querying database table \'fmt_trips\' for out of service trip.', str(inspect.currentframe().f_lineno))
        oosTripRecord = cursorOOSQuery.fetchone()
        if oosTripRecord is None:
            # No out of service trip exists so create it
            try:
                insertOOSQuery = ''' INSERT INTO fmt_trips
                                    (trip_id,
                                    trip_headsign,
                                    trip_headsign_short,
                                    trip_headsign_full,
                                    headsign_hash,
                                    route_id,
                                    direction_id
                                    )
                                    VALUES ( %s, %s, %s, %s, %s, %s, %s)'''
                
                cursorOOSQuery.execute(insertOOSQuery, outOfServiceTripDetails)
                DBConnection.commit()             
            except mysql.connector.Error as err:
                eventMsg = str(err)
                eventLogger('error', eventMsg, 'Error inserting \'Out Of Service\' record in table \'fmt_trips\'', str(inspect.currentframe().f_lineno))
        else:
            # Update existing record
            # the purpose of this update is to catch any changes to what we want the oos record to look like, so
            # it basically resets it to our desired values
            try:
                updateOOSQuery = ''' UPDATE fmt_trips
                                    SET 
                                        trip_id = %s,
                                        trip_headsign = %s,
                                        trip_headsign_short = %s,
                                        trip_headsign_full = %s,
                                        headsign_hash = %s,
                                        route_id = %s,
                                        direction_id = %s   
                                    WHERE trip_id = %s'''
                cursorOOSQuery.execute(updateOOSQuery, outOfServiceTripDetails + (oosTripIDStr,))
                DBConnection.commit()             
            except mysql.connector.Error as err:
                eventMsg = str(err)
                eventLogger('error', eventMsg, 'Error updating \'Out Of Service\' record in table \'fmt_trips\'', str(inspect.currentframe().f_lineno))
        cursorOOSQuery.close()
        

    # Check the required files exist
    for filePath in [trackDetailsFilename, specialTrainsFilename, legendFontFilename]:
        if not os.path.isfile(filePath):
            eventMsg = 'The file \'' + filePath + '\' was expected but not found.'
            eventLogger('error', eventMsg, 'A required file is missing', str(inspect.currentframe().f_lineno))

    #
    # Derived properties
    #
    primaryMarginSize = int((mapWidthPoints*imgMarginPercent)/100)
    lineWidthPt = lineWidthPixels
    #
    # This is to allow calculations for timestamps
    # The input string needs to be in the form "<hours>:<minutes>:<seconds>"
    #
    def timestrToSeconds(inputTimeStr):
        currHour = int(inputTimeStr[:2])
        currMin = int(inputTimeStr[3:5])
        currSec = int(inputTimeStr[6:8])
        return ((currHour*3600) + (currMin*60) + currSec)

    #
    # There are tasks that need to be done after the train updates are complete, management
    # tasks if you like
    #
    # These are typically tasks that would be difficult to implement in other functions but straight
    # forward as a post update task
    #
    def postUpdateTasks():

        ################
        #
        # Only one train in a set will have trip details, 'trip_id'.
        # Thus if it is a 6 carridge train only one of the two trains will have
        # trip details. This makes it difficult to work out if it is out of service,
        # and also means only one train will display trip info, such as the timetable.
        #
        # So first thing to do is work out if any trains in a set have trip details and then ensure
        # all trains in that set get the same details
        #

        # first get all train details
        cursorTrainDetails = DBConnection.cursor(dictionary=True)
        sqlQuery = 'select * from fmt_train_details'
        try:
            cursorTrainDetails.execute(sqlQuery)
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error querying database table \'fmt_train_details\' during postUpdateTasks().', str(inspect.currentframe().f_lineno))

        currentDBTrainDetails = {}
        for currDBTrain in cursorTrainDetails:            
            currentDBTrainDetails.update({currDBTrain['train_number']:currDBTrain})

        ################
        #
        # We need to work out the current delay for each trip
        #
        ###################
                    
        # First step get a list of all active trips
        sqlQuery = 'SELECT DISTINCT trip_id FROM fmt_train_details ftd WHERE trip_id != \"\" '
        try:
            cursorTrainDetails.execute(sqlQuery)
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error querying database table \'fmt_train_details\' to get list of all active train trips.', str(inspect.currentframe().f_lineno))

        activeTripIDs = []
        for currTrip in cursorTrainDetails:            
            activeTripIDs.append(currTrip['trip_id'])

        # Get all trip updates
        tripUpdatesResponse = apiRequest(tripUpdatesURL, True, 'Trip updates')
        eventMsg = 'Updating \'fmt_trips\' details...'
        eventLogger('info', eventMsg, 'Updating \'fmt_trips\' details...', str(inspect.currentframe().f_lineno))
        for currTripUpdate in tripUpdatesResponse['response']['entity']:
            currTripID = currTripUpdate['id']
            currTripRouteID = ''
            currTripDirectionID = 0
            if 'trip_update' in currTripUpdate:

                if 'trip' in currTripUpdate['trip_update']:
                        if 'route_id' in currTripUpdate['trip_update']['trip']:
                            currTripRouteID = currTripUpdate['trip_update']['trip']['route_id']
                        if 'direction_id' in currTripUpdate['trip_update']['trip']:
                            currTripDirectionID = currTripUpdate['trip_update']['trip']['direction_id']
                if 'delay' in currTripUpdate['trip_update']:
                    #
                    # Important to note we are not displaying seconds, that is ridiculous
                    # so we need to round our delays for minutes
                    #
                    # Also, for the same reason, if a delay is less than 30 seconds then
                    # we will consider it on time
                    #
                    currTripDelay = currTripUpdate['trip_update']['delay'] 
                    delayMsg = 'on time'
                    delayTimeRemaining = abs(currTripDelay)
                    if delayTimeRemaining > 30:
                        delayMsgTime = ''
                        if delayTimeRemaining >= 3600:
                            delayHours = int(delayTimeRemaining/3600)
                            hoursSuffix = ''
                            if delayHours > 1:
                                hoursSuffix = 's'
                            delayMsgTime = str(delayHours) + ' hour' + hoursSuffix
                            delayTimeRemaining = delayTimeRemaining - (delayHours*3600)
                        if delayTimeRemaining > 30:
                            delayMin = int(round(delayTimeRemaining/60))
                            minutesSuffix = ''
                            if delayMin > 1:
                                minutesSuffix = 's'
                            if delayMsgTime != '':
                                delayMsgTime += ' and ' + str(delayMin) + ' minute' + minutesSuffix
                            else:
                                delayMsgTime = str(delayMin) + ' minute' + minutesSuffix
                        if currTripDelay > 0:
                            delayMsg = 'delayed by ' + delayMsgTime
                        else:
                            delayMsg = 'early by ' + delayMsgTime
                    


                    #
                    # Check if this is an active trip
                    # and if so update the DB
                    #
                    if currTripID in activeTripIDs:
                        eventMsg = 'Updating \'fmt_trips\' record... ' + str(currTripID)
                        eventLogger('info', eventMsg, 'Trip: ' + currTripID + ',  delay: ' + str(currTripDelay), str(inspect.currentframe().f_lineno))
                        try:
                            updateQuery = ''' UPDATE 
                                                fmt_trips 
                                              SET 
                                                trip_delay = %s,
                                                trip_delay_msg = %s,
                                                route_id = %s,
                                                direction_id = %s
                                              WHERE 
                                                trip_id = %s'''
                            updateValues = (currTripDelay,
                                            delayMsg,
                                            currTripRouteID,
                                            currTripDirectionID,
                                            currTripID,
                                            )
                            cursorTrainDetails.execute(updateQuery, updateValues)
                            DBConnection.commit()
                        except mysql.connector.Error as err:
                            eventMsg = str(err)
                            eventLogger('error', eventMsg, 'Error updating train \'trip_delay\' in database table \'fmt_trips\'.', str(inspect.currentframe().f_lineno))

    #
    # Call an AT api
    #
    def apiRequest(requestURL, failOnError, requestDesc):
                
        global apiKeyDetails

        #
        # Because an api key may expire at any point we need
        # to loop around until we either succeed, completely fail
        # or run out of api keys
        #
        callSucceeded = False
        while not callSucceeded:

            requestResultOK = True
            requestErrorMsg =''

            # Work out the best subscription key to use
            activeKeyName = ''
            currPosixDate = datetime.timestamp(datetime.now())
            for currKey in apiKeyDetails:
                #
                # The api value 'live_after_posix' needs to be either 0 or a date at least
                # 30 minutes in the future - aka 1800 seconds
                #
                # Also remember that a 'live_after_posix' value of -1 means the key is invalid
                # so don't try it again
                #
                currKeyPosix = apiKeyDetails[currKey]['live_after_posix']            
                if ((currKeyPosix == 0) or (currPosixDate > (currKeyPosix + 1800))) and (currKeyPosix != -1) :
                    activeKeyName = apiKeyDetails[currKey]['api_key_name']
                    activeKeyValue = apiKeyDetails[currKey]['key_value']
                    break

            if activeKeyName == '':
                # Get list of keys and details
                msgKeyDetails = 'Key name                      Posix Date       Date \n'
                for currKey in apiKeyDetails:
                    currKeyPosix = apiKeyDetails[currKey]['live_after_posix']
                    currKeyDateTime = posixtoDateTime(currKeyPosix).strftime('%e/%m/%Y, %I:%M:%S %p') 
                    msgKeyDetails += (str(apiKeyDetails[currKey]['api_key_name']) + ' '*50)[:30]
                    msgKeyDetails += (str(currKeyPosix) + ' '*50)[:16]
                    if currKeyPosix == -1:
                        msgKeyDetails += 'Key invalid'
                    elif currKeyPosix == 0:
                        msgKeyDetails += 'Key valid immediately, quota not exceeded'
                    else:
                        msgKeyDetails += currKeyDateTime
                    msgKeyDetails += '\n'
                    
                eventMsg = 'Failed to find valid api key\n\n' + msgKeyDetails
                eventLogger('error', eventMsg, 'No available key was found' , str(inspect.currentframe().f_lineno))
                exit(1)
            
            # Try the api call
            try:
                headers = {'content-type': 'application/json','Ocp-Apim-Subscription-Key':activeKeyValue}
                response = requests.get(requestURL, headers=headers) 
            except ConnectionError as err:
                eventMsg =  'Connection error calling Auckland Transport api :' + requestURL + '\n\n' + \
                            'Response: ' + str(err) 
                if failOnError:                           
                    eventLogger('error', eventMsg, 'Connection error calling AT api' , str(inspect.currentframe().f_lineno))
                    exit(1)
                else:
                    eventLogger('info', eventMsg, 'Connection error calling AT api', str(inspect.currentframe().f_lineno))
                    requestResultOK = False
                    requestErrorMsg = str(err) 

            if requestResultOK:
                #
                # Find out if it failed because the apikey has exhausted it's quota
                #
                if response.status_code == 403:  
                    #
                    # We are assuming a key is exhausted if it is a 403 and 'Retry-After' header exists
                    # and is greater than 0
                    #
                    retrySeconds = 0
                    if 'Retry-After' in response.headers:
                        retrySeconds = int(response.headers['Retry-After'])
                    if retrySeconds > 0:
                        #
                        # Token has expired
                        #
                        liveAfterPosix = currPosixDate + retrySeconds

                        # Update 'apiKeyDetails' and the DB to reflect this
                        cursorApiKeys = DBConnection.cursor(dictionary=True)
                        sqlQuery = '''  UPDATE
                                            fmt_api_keys 
                                        SET 
                                            live_after_posix = %s
                                        WHERE 
                                            api_key_name = %s'''
                        updateValues = (liveAfterPosix,
                                        activeKeyName,
                                        )
                        try:
                            cursorApiKeys.execute(sqlQuery, updateValues)
                            DBConnection.commit()
                        except mysql.connector.Error as err:
                            eventMsg = str(err)
                            eventLogger('error', eventMsg, 'Error updating database table \'fmt_api_keys\' with new  \'live_after_posix\'.', str(inspect.currentframe().f_lineno))

                        apiKeyDetails[activeKeyName]['live_after_posix'] = liveAfterPosix
                        eventMsg = 'Api token \'' + activeKeyName + '\' has reached its quota. It will be usable until ' + posixtoDateTime(liveAfterPosix).strftime('%e/%m/%Y, %I:%M:%S %p') + '.'
                        eventLogger('info', eventMsg, 'Connection error calling AT api', str(inspect.currentframe().f_lineno))
                        
                #
                # if status code is 401 that means it's an invalid key
                # set 'live_after_posix' to -1 to reflect it's invalid
                #
                if response.status_code == 401:  
                    eventMsg = 'Api token \'' + activeKeyName + '\' is invalid. It has been given a value of \'-1\' to mark it as invalid.'
                    eventLogger('info', eventMsg, 'Api token \'' + activeKeyName + '\' is invalid.', str(inspect.currentframe().f_lineno))

                    # Update 'apiKeyDetails' and the DB to reflect this
                    cursorApiKeys = DBConnection.cursor(dictionary=True)
                    sqlQuery = '''  UPDATE
                                        fmt_api_keys 
                                    SET 
                                        live_after_posix = %s
                                    WHERE 
                                        api_key_name = %s'''
                    updateValues = (-1,
                                    activeKeyName,
                                    )
                    try:
                        cursorApiKeys.execute(sqlQuery, updateValues)
                        DBConnection.commit()
                    except mysql.connector.Error as err:
                        eventMsg = str(err)
                        eventLogger('error', eventMsg, 'Error updating database table \'fmt_api_keys\' with new  \'live_after_posix\'.', str(inspect.currentframe().f_lineno))

                    apiKeyDetails[activeKeyName]['live_after_posix'] = -1        

                if response.status_code == 200:
                    # Call was successful
                    callSucceeded = True
                    eventMsg = 'Api call successful for token \'' + activeKeyName + '\' to \'' + requestURL + '\'.'
                    eventLogger('info', eventMsg, 'Api token call successful.', str(inspect.currentframe().f_lineno))

                if response.status_code not in (200, 403, 401):  
                    eventMsg =  'Return status error calling Auckland Transport api :' + requestURL + '\n\n' + \
                                'Status code ' + str(response.status_code) + '\n' + \
                                'Response: ' + json.dumps(response.json() , indent=4, sort_keys=True, default=str)
                    if failOnError:                               
                        eventLogger('error', eventMsg, 'Status error calling AT api' , str(inspect.currentframe().f_lineno))
                        exit()
                    else:
                        eventLogger('info', eventMsg, 'Status error calling AT api' , str(inspect.currentframe().f_lineno))
                        requestResultOK = False
                        requestErrorMsg =   'The return status code was not 200, it was ' + str(response.status_code) + '. ' + \
                                            'The return json was: ' + json.dumps(response.json() , indent=4, sort_keys=True, default=str)

            responseJson = {}
            if requestResultOK:
                responseJson = response.json()
            responseJson.update({'request_result_ok':requestResultOK, 'request_error_msg':requestErrorMsg})

        return responseJson

    #
    # Update Trip Stop and Time details
    #
    def updateTripStopDetails():
        global stopDetails
        eventMsg = 'Running updateTripStopDetails()'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

        #
        # Step through all the trains and collect the stop details
        #
        cursorTripDetails = DBConnection.cursor(dictionary=True)
        for currTrain in  trainDetails['train']:
            if 'trip' in trainDetails['train'][currTrain]['vehicle']:

                currTripId = trainDetails['train'][currTrain]['vehicle']['trip']['trip_id']

                #
                # First find if the trip details are already in the DB
                #                
                sqlQuery = ''' SELECT * FROM fmt_trips ft WHERE trip_id = %s'''
                sqlVaues =(currTripId,)
                try:
                    cursorTripDetails.execute(sqlQuery, sqlVaues)
                except mysql.connector.Error as err:
                    eventMsg = str(err)
                    eventLogger('error', eventMsg, 'Error querying database table \'fmt_trips\' for trip id ' + currTripId, str(inspect.currentframe().f_lineno))

                cursorTripDetails.fetchone()

                #
                # We need to check if this trip id is in the database and if it is
                # not then add it
                #
                if cursorTripDetails.rowcount < 1:
                    #
                    # This Trip Id is not in the DB so create it
                    #
                    eventMsg = 'Getting trip details for trip \'' + str(currTripId) + '\''
                    eventLogger('info', eventMsg, 'Getting trip details' + str(currTrain), str(inspect.currentframe().f_lineno))

                    # Get the stop times for the trip by calling an API
                    stopTimesURL = baseTripsURL + str(currTripId) + '/stoptimes'
                    stopTimesDetail = apiRequest(stopTimesURL, True, 'Stop times')

                    # We also need to get the 'trip_headsign' details as this is the correct and current
                    # name for this trip. Much more robust than having a set list of routes
                    # though we use the same base url, stopTimesURL, it is without the "/stoptimes" and unfortunately
                    # we can only get this with another api call.
                    # Because we only call the trips api, aka. this sectiion, if we don't already have the trip details
                    # I don't think it will significantly increase the numbeer of api calls
                    tripHeadsignURL = baseTripsURL + str(currTripId) 
                    tripHeadsignDetail = apiRequest(tripHeadsignURL, True, 'Get headsign details')
                    currTripHeadsignStr = 'Trip Details Unknown'
                    if "trip_headsign" in tripHeadsignDetail['data']['attributes']:
                        currTripHeadsignStr = tripHeadsignDetail['data']['attributes']['trip_headsign']


                    #
                    # Create shortened headsign variable 'currTripHeadsignShortStr' from 'currTripHeadsignStr'
                    # and tidy up
                    #
                    currTripHeadsignFullStr = currTripHeadsignStr
                    
                    # Replace any digits with null
                    currTripHeadsignFullStr = re.sub(r'\d', '', currTripHeadsignFullStr)
                    
                    # Apply word replacements
                    for old_word, new_word in headsignStringReplacementa.items():
                        currTripHeadsignFullStr = currTripHeadsignFullStr.replace(old_word, new_word)
                    
                    # Tidy up spaces
                    currTripHeadsignFullStr = currTripHeadsignFullStr.strip().replace('  ',' ')
                    
                    # Truncate to first occurrence of ' via ' if it exists to create shortened headsign
                    currTripHeadsignShortStr = currTripHeadsignFullStr
                    tripHeadsSplitParts = re.split(r' via ', currTripHeadsignShortStr, flags=re.IGNORECASE)
                    if len(tripHeadsSplitParts) > 1:
                        currTripHeadsignShortStr = tripHeadsSplitParts[0].strip().replace('  ',' ')
                    
                    # Create URL-encoded headsign hash for safe use in URLs
                    currHeadsignHash = quote(currTripHeadsignFullStr)
                                          

                    #
                    # Create a string with stop details
                    #
                    # This is a semicolon delimited lists of stops with the stop details being comma separated
                    #
                    # First stage collect a list of stop and stop details
                    #
                    currTripStopDetailsJson =   {}
                    for currStop in stopTimesDetail['data']:
                        stopNumber = int(currStop['attributes']['stop_sequence'])
                        # Get stop name
                        stopName = 'Stop name unknown'
                        stopID = currStop['attributes']['stop_id']
                        if stopID in stopDetails:
                            stopName = stopDetails[stopID]['attributes']['stop_name']
                            
                        departTimeStr = currStop['attributes']['departure_time']
                        departTimeSecPastMidnight = timestrToSeconds(departTimeStr)

                        currTripStopDetailsJson.update({ stopNumber:{
                                                            'stop_name':stopName.replace(',','').replace(';',''),       # Note we want to remove commas and semi colons from names
                                                            'arrival_time_str':departTimeStr,'depart_time_sec_past_midnight':departTimeSecPastMidnight
                                                        }})

                                            
                    #
                    # Second stage concatinate all stops details into one string
                    #
                    currStopDetailsStr = ''
                    tripEndSecPastMidnight = 0  # calculate current date time in unix timestamp format
                    for currStop in sorted(list(currTripStopDetailsJson)):
                        if currStopDetailsStr != '':
                            currStopDetailsStr += ';'
                        currStopDetailsStr += str(currStop) + ',' + currTripStopDetailsJson[currStop]['stop_name'] + ',' + str(currTripStopDetailsJson[currStop]['depart_time_sec_past_midnight']) + \
                                            ',' + currTripStopDetailsJson[currStop]['arrival_time_str']    
                        if currTripStopDetailsJson[currStop]['depart_time_sec_past_midnight'] > tripEndSecPastMidnight:
                            tripEndSecPastMidnight = currTripStopDetailsJson[currStop]['depart_time_sec_past_midnight']    

                    #
                    # Insert the record into the DB
                    #
                    try:
                        insertQuery = ''' INSERT INTO fmt_trips
                                        (trip_id,
                                        stop_details_str,
                                        trip_headsign,
                                        trip_headsign_short,
                                        trip_headsign_full,
                                        headsign_hash,
                                        trip_end_sec_past_midnight
                                        )
                                        VALUES ( %s, %s, %s, %s, %s, %s, %s)'''
                        insertValues = (currTripId,
                                        currStopDetailsStr,
                                        currTripHeadsignStr,
                                        currTripHeadsignShortStr,
                                        currTripHeadsignFullStr,
                                        currHeadsignHash,
                                        tripEndSecPastMidnight
                                        )
                        cursorTripDetails.execute(insertQuery, insertValues)
                        DBConnection.commit()
                    except mysql.connector.Error as err:
                        eventMsg = 'Error inserting new trip details, in table \'fmt_trips\'.' + '\n\n' + \
                                    'trip_id                         : ' + str(currTripId) + '\n' + \
                                    'stop_details_str                      : ' + str(currStopDetailsStr) + '\n' + \
                                    str(err)
                        eventLogger('error', eventMsg, 'Error inserting new trip details, in table \'fmt_trips\'', str(inspect.currentframe().f_lineno))

        #
        # Clean up historical trips
        #
        try:
            trucateQuery = '''
                            DELETE FROM fmt_trips  
                            WHERE updated < now() - interval %s DAY'''
            truncateValues = (  
                                retainTripDetailsDays,
                            )
            cursorTripDetails.execute(trucateQuery, truncateValues)
            DBConnection.commit()
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error truncating rows in database table \'fmt_trips\'.', str(inspect.currentframe().f_lineno))


    #
    # Get all stop details
    #
    def getStopDetails():
        global stopDetails
        eventMsg = 'Running getStopDetails()'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
        
        # Get details of all stops via api call
        apiTimestampPosix = apiRequest(atAllStopsURL, True, 'Stop details')['data'] 
        for currStop in apiTimestampPosix:
            stopDetails.update({currStop['id']:currStop})
        
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
    # Get the latest event log id from "fmt_event_log"
    #
    def getLatestEventID():
        cursorEventLogID = DBConnection.cursor(dictionary=True)
        sqlQuery = '''  SELECT event_id
                        FROM fmt_event_log 
                        ORDER BY event_id 
                        DESC LIMIT 1'''
        try:
            cursorEventLogID.execute(sqlQuery)
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error querying database table \'fmt_event_log\' to find latest \'event_id\'.', str(inspect.currentframe().f_lineno))

        latestEventID = 0
        for currentEventRecord in cursorEventLogID:
            latestEventID = int(currentEventRecord['event_id'])

        return latestEventID

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

        global specialTrainDetails

        eventMsg = 'Running loadSpecialTrainDetails()' 
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

        #
        # There needs to be a row for default train details - as in trains that aren't special
        # The default train has a number '0'
        #
        defaultTrainFound = False
        
        #
        # Load csv into dict
        #
        with open(specialTrainsFilename, mode='r', encoding='windows-1252') as specialTrainDetailsCSV:
            specialTrainsDetailsReader = csv.DictReader(specialTrainDetailsCSV)

            # Remap header names to dict keys
            remappedHeaders = []
            for headerName in specialTrainsDetailsReader.fieldnames:
                remappedHeaders.append(mapSpecialTrainHeaderToKeys[headerName])
            specialTrainsDetailsReader.fieldnames = remappedHeaders

            

            # Load rows
            for currRow in specialTrainsDetailsReader:
                specialTrainDetails.update({currRow['train_number']:currRow})
                specialTrainDetails[currRow['train_number']].update({'special_train': True})
                if currRow['train_number'] == '0':
                    defaultTrainFound = True
                    # we have a key 'special_train' this is not in the CSV so we have to update here
                    specialTrainDetails[currRow['train_number']].update({'special_train': False})

            if not defaultTrainFound:
                    eventMsg = 'No \'default\' train found in \'' + specialTrainsFilename + '\'.' + '\n\n' + \
                               'Ensure this file has a row with a \'Train Number\' with a value of 0.' + '\n' + \
                               'This is the default train'
                    eventLogger('error', eventMsg, 'No \'default\' train found', str(inspect.currentframe().f_lineno))

        #
        # Update train details in the database in case we have changed any details like URLs or names
        #
        cursorUpdateSpecialTrains = DBConnection.cursor(dictionary=True)

        #
        # First update the special train details
        #
        try:
            specialTrainList = ''
            for currTrain in specialTrainDetails:
                currTrainNo = int(specialTrainDetails[currTrain]['train_number'])
                if currTrainNo != 0:
                    if specialTrainList == '':
                        specialTrainList = str(currTrainNo)
                    else:
                        specialTrainList += ',' + str(currTrainNo)
                    updateQuery = ''' 
                                    UPDATE fmt_train_details 
                                    SET 
                                        train_featured_img_url = %s,
                                        train_small_img_url = %s,
                                        train_description = %s,
                                        custom_name = %s,
                                        special_train = %s
                                    WHERE 
                                        train_number = %s
                                '''
                    updateValues = (specialTrainDetails[currTrain]['train_featured_img_url'],
                                    specialTrainDetails[currTrain]['train_small_img_url'],
                                    specialTrainDetails[currTrain]['train_description'],
                                    specialTrainDetails[currTrain]['custom_name'],
                                    specialTrainDetails[currTrain]['special_train'],
                                    currTrainNo,
                                    )
                    cursorUpdateSpecialTrains.execute(updateQuery, updateValues)
                    DBConnection.commit()

        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error updating special train details in database table \'fmt_train_details\'.', str(inspect.currentframe().f_lineno))
      

        #
        # Now update the details for all non-special trains using the default details
        #
        # Note: we need to deal with the scenario where there are zero special trains
        #
        if specialTrainList == '':
            whereClause = ''
        else:
            whereClause = 'WHERE train_number NOT IN (' + specialTrainList + ')'

        try:
            updateQuery = ''' 
                            UPDATE fmt_train_details 
                            SET 
                                train_featured_img_url = %s,
                                train_small_img_url = %s,
                                train_description = %s,
                                custom_name = friendly_name,
                                special_train = false
                        ''' + whereClause
            updateValues = (specialTrainDetails['0']['train_featured_img_url'],
                            specialTrainDetails['0']['train_small_img_url'],
                            specialTrainDetails['0']['train_description'],
                            )
            cursorUpdateSpecialTrains.execute(updateQuery, updateValues)
            DBConnection.commit()
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error updating non-special train details in database table \'fmt_train_details\'.', str(inspect.currentframe().f_lineno))
                
        return specialTrainDetails

    #
    # Import stations from CSV file into fmt_stations table
    #
    def importStationsFromCSV():
        """
        Import station data from stations.csv file into fmt_stations table.
        Validates data against track details and requires mandatory default record.
        """
        eventMsg = 'Importing stations from stations.csv file'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
        
        stationsCursor = None
        try:
            # Check if CSV file exists first
            if not os.path.exists(stationsFilename):
                eventMsg = f'The stations file \'{stationsFilename}\' was not found.'
                eventLogger('error', eventMsg, 'Stations CSV file missing', str(inspect.currentframe().f_lineno))
            
            # Use trackDetails that was populated during map drawing for validation
            if not trackDetails or 'track_sections' not in trackDetails:
                eventMsg = 'Track details not available for station validation. Map drawing may have failed.'
                eventLogger('error', eventMsg, 'Track details not available', str(inspect.currentframe().f_lineno))
            
            # Create cursor and start explicit transaction
            stationsCursor = DBConnection.cursor()
            stationsCursor.execute("START TRANSACTION")
            
            # Clear existing station data (part of transaction)
            deleteQuery = "DELETE FROM fmt_stations"
            stationsCursor.execute(deleteQuery)
            
            # Read and validate stations from CSV
            validationErrors = []
            stationCount = 0
            rowNumber = 1  # Track row number for error reporting
            defaultRecordFound = False  # Track if mandatory default record exists
            
            with open(stationsFilename, 'r', encoding='utf-8') as csvfile:
                csvreader = csv.DictReader(csvfile)
                
                for row in csvreader:
                    rowNumber += 1
                    rowErrors = []
                    
                    # Validate section_id (mandatory, must exist in track details OR be the special default)
                    section_id_str = row['section_id'].strip() if (row['section_id'] is not None) else ''
                    if not section_id_str:
                        rowErrors.append(f"on row {rowNumber}: section_id is mandatory but missing")
                        section_id_int = None
                    else:
                        try:
                            section_id_int = int(section_id_str)
                            
                            # Check for mandatory default record exception
                            if section_id_int == -1:
                                section_name = row['section_name'].strip() if row['section_name'] else ''
                                if section_name == 'default':
                                    defaultRecordFound = True
                                    # Skip track details validation for this special case
                                else:
                                    # section_id = -1 but not 'default' name
                                    if section_id_int not in trackDetails['track_sections']:
                                        rowErrors.append(f"on row {rowNumber}: section_id '{section_id_int}' does not exist in {trackDetailsFilename}")
                            else:
                                # Normal validation for all other section_ids
                                if section_id_int not in trackDetails['track_sections']:
                                    rowErrors.append(f"on row {rowNumber}: section_id '{section_id_int}' does not exist in {trackDetailsFilename}")
                        except ValueError:
                            rowErrors.append(f"on row {rowNumber}: section_id '{section_id_str}' is not a valid integer")
                            section_id_int = None
                    
                    # Validate section_name (mandatory, must match track details OR be special default)
                    section_name = row['section_name'].strip() if row['section_name'] else ''
                    if not section_name:
                        rowErrors.append(f"on row {rowNumber}: section_name is mandatory but missing")
                    elif section_id_int == -1 and section_name == 'default':
                        # Special case - skip validation for default record
                        pass
                    elif section_id_int is not None and section_id_int in trackDetails['track_sections']:
                        expected_name = trackDetails['track_sections'][section_id_int]['title']
                        if section_name != expected_name:
                            rowErrors.append(f"on row {rowNumber}: section_name '{section_name}' does not match expected '{expected_name}' for section_id {section_id_int}")
                    
                    # Validate featured (mandatory, must be uppercase Y or N)
                    featured = row['featured'].strip() if row['featured'] else ''
                    if not featured:
                        rowErrors.append(f"on row {rowNumber}: featured is mandatory but missing")
                    elif featured not in ['Y', 'N']:
                        rowErrors.append(f"on row {rowNumber}: featured '{featured}' must be uppercase 'Y' or 'N'")
                    
                    # Validate primary_image (mandatory)
                    primary_image = row['primary_image'].strip() if row['primary_image'] else ''
                    if not primary_image:
                        rowErrors.append(f"on row {rowNumber}: primary_image is mandatory but missing")
                    
                    # Validate secondary_image (optional)
                    secondary_image = row['secondary_image'].strip() if row['secondary_image'] else None
                    
                    # Description is optional, no validation needed beyond stripping
                    description = row['description'].strip() if row['description'] else None
                    
                    # Collect any validation errors for this row
                    if rowErrors:
                        validationErrors.append(f"Row {rowNumber}: {'; '.join(rowErrors)}")
                    
                    # Insert the record even if there are validation errors (to continue processing)
                    insertQuery = '''INSERT INTO fmt_stations 
                                    (section_id, section_name, featured, primary_image, secondary_image, description)
                                    VALUES (%s, %s, %s, %s, %s, %s)'''
                    insertValues = (
                        section_id_int,
                        section_name if section_name else None,
                        featured if featured else 'N',
                        primary_image if primary_image else None,
                        secondary_image,
                        description
                    )
                    
                    stationsCursor.execute(insertQuery, insertValues)
                    stationCount += 1
            
            # Check if mandatory default record was found
            if not defaultRecordFound:
                validationErrors.append('MANDATORY ERROR: The default station record (section_id=-1, section_name=default) is missing from the stations.csv file')
            
            # Check for validation errors BEFORE committing
            if validationErrors:
                # Rollback transaction due to validation errors
                stationsCursor.execute("ROLLBACK")
                errorMsg = f'Station import failed due to {len(validationErrors)} validation error(s):\n\n'
                errorMsg += '\n'.join(validationErrors)
                errorMsg += f'\n\nValidation failed. Please correct the errors in {stationsFilename} and ensure data matches {trackDetailsFilename}'
                eventLogger('error', errorMsg, 'Station data validation failed', str(inspect.currentframe().f_lineno))
            
            # Only commit if validation passed
            stationsCursor.execute("COMMIT")
            
            eventMsg = f'Successfully imported {stationCount} stations from {stationsFilename}'
            eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                
        except mysql.connector.Error as err:
            # Rollback transaction on database error
            if stationsCursor is not None:
                try:
                    stationsCursor.execute("ROLLBACK")
                except:
                    pass  # Rollback might fail if no transaction is active
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error importing stations to fmt_stations table', str(inspect.currentframe().f_lineno))
        except FileNotFoundError as err:
            # Rollback transaction on file error
            if stationsCursor is not None:
                try:
                    stationsCursor.execute("ROLLBACK")
                except:
                    pass
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Stations CSV file not found', str(inspect.currentframe().f_lineno))
        except Exception as err:
            # Rollback transaction on any other error
            if stationsCursor is not None:
                try:
                    stationsCursor.execute("ROLLBACK")
                except:
                    pass
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Unexpected error importing stations', str(inspect.currentframe().f_lineno))
        finally:
            if stationsCursor is not None:
                stationsCursor.close()

    #
    # Load train routes from AT API instead of CSV file
    #
    def loadTrainRoutesFromAPI():
        """
        Load train routes from AT API instead of CSV file.
        This replaces the routes.csv system with live API data.
        """
        
        eventMsg = 'Running loadTrainRoutesFromAPI()'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

        # Make the API call to get all routes
        routesResponse = apiRequest(routesURL, True, 'Routes from API')
        
        # Initialize the same structure as the original function
        routeDetails = {
            'at_route_id': {},
        }
        
        # Check if response has the expected structure
        # The API returns data directly, not wrapped in 'response'
        apiData = routesResponse.get('data', routesResponse.get('response', {}).get('data', []))
        
        # Always include the special 'na' and 'oos' routes for unknown/out-of-service
        specialRoutes = [
            {
                'route_id': '0',
                'at_route_id': 'na',
                'route_short_name': 'N/A',
                'route_long_name': 'Not Available',
                'agency_id': 'AM'
            },
            {
                'route_id': 'oos', 
                'at_route_id': 'oos',
                'route_short_name': 'OOS',
                'route_long_name': 'Out of Service',
                'agency_id': 'AM'
            }
        ]
        
        # Add special routes first
        for route in specialRoutes:
            routeDetails['at_route_id'][route['at_route_id']] = route
        
        # Process API response and convert to our format
        # Only include train routes (agency_id == 'AM')
        if apiData:
            for route_data in apiData:
                if 'attributes' in route_data:
                    attrs = route_data['attributes']
                    
                    # Only process train routes (agency_id == 'AM')
                    if attrs.get('agency_id', '') != 'AM':
                        continue
                    
                    # Create route record in our format
                    route_record = {
                        'route_id': attrs.get('route_id', ''),
                        'at_route_id': attrs.get('route_id', ''),
                        'route_short_name': attrs.get('route_short_name', ''),
                        'route_long_name': attrs.get('route_long_name', ''),
                        'agency_id': attrs.get('agency_id', ''),
                        'route_color': attrs.get('route_color', ''),
                        'route_text_color': attrs.get('route_text_color', ''),
                        'route_type': attrs.get('route_type', '')
                    }
                    
                    # Add to our route details structure
                    routeDetails['at_route_id'][attrs.get('route_id', '')] = route_record

        eventMsg = f'Loaded {len(routeDetails["at_route_id"])} routes from AT API'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

        return routeDetails


    

    #
    # Make the api call to get current details about vehicles
    #
    def getCurrVehicleDetails():
        global apiTimestampPosix
        global trainDetails
        global fullUp2DateTrainLocations
        global specialTrainDetails

        # Get current system time for calculating historical positions
        currentTime = int(time.time())

        # By definition almost 'commonLocationTargetTimestamp' must be earlier than the current datetime
        # so we set it to be 'commonTimestampOffsetSec' seconds before the current datetime.
        commonLocationTargetTimestamp = currentTime - commonTimestampOffsetSec

        eventMsg = 'Running getCurrVehicleDetails()'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

        #
        # get a copy of the train details table in one dict so
        # we can reference that regards trains changing direction
        #
        print('= 1698 load all details from fmt_train_details into allDBTrainDetails')
        sqlQuery = 'SELECT * FROM fmt_train_details'
        cursorAllTrainDetails= DBConnection.cursor(dictionary=True)
        try:
            cursorAllTrainDetails.execute(sqlQuery)
            allDBTrainDetailsRaw = cursorAllTrainDetails.fetchall() 
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error all details from table \'fmt_train_details\'.', str(inspect.currentframe().f_lineno))
        allDBTrainDetails = {}
        for currTrainDetails in allDBTrainDetailsRaw:
            allDBTrainDetails.update({currTrainDetails['train_number']:currTrainDetails})

        # 
        # Load historical position data from database
        #
        cursorPositionHistory = DBConnection.cursor(dictionary=True)
        sqlQuery = 'SELECT train_number, position_history FROM fmt_train_details'
        try:
            cursorPositionHistory.execute(sqlQuery)
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error querying position_history from table \'fmt_train_details\'.', str(inspect.currentframe().f_lineno))

        for trainRecord in cursorPositionHistory:
            trainNumber = trainRecord['train_number']
            if trainRecord.get('position_history') is not None and trainRecord.get('position_history') != '':
                try:
                    # Parse JSON from position_history column
                    positionHistory = json.loads(trainRecord['position_history'])
                    
                    # Check if data is already in new nested structure (has 'history' key)
                    if 'history' in positionHistory and isinstance(positionHistory['history'], dict):
                        # Already in new format - convert string timestamp keys to integers
                        restructuredData = {'history': {}}
                        for ts, data in positionHistory['history'].items():
                            try:
                                restructuredData['history'][int(ts)] = data
                            except ValueError:
                                # Keep non-numeric keys as strings (shouldn't happen in history)
                                restructuredData['history'][ts] = data
                        # Copy any top-level keys (like 'location_1_min_ago')
                        for key in positionHistory:
                            if key != 'history':
                                restructuredData[key] = positionHistory[key]
                    else:
                        # Old flat format - restructure: integer timestamps go under 'history', special keys stay at top level
                        restructuredData = {'history': {}}
                        for ts, data in positionHistory.items():
                            try:
                                # Integer timestamp keys go into 'history' sub-dict
                                restructuredData['history'][int(ts)] = data
                            except ValueError:
                                # Non-numeric keys (like 'location_1_min_ago') stay at top level
                                restructuredData[ts] = data
                    # Store in fullUp2DateTrainLocations with train number as key
                    fullUp2DateTrainLocations[trainNumber] = restructuredData
                    # Ensure previous_train_sets and last_time_in_train_set exist (initialize if missing)
                    if 'previous_train_sets' not in fullUp2DateTrainLocations[trainNumber]:
                        fullUp2DateTrainLocations[trainNumber]['previous_train_sets'] = []
                    if 'last_time_in_train_set' not in fullUp2DateTrainLocations[trainNumber]:
                        fullUp2DateTrainLocations[trainNumber]['last_time_in_train_set'] = None
                    if 'last_time_in_train_set_str' not in fullUp2DateTrainLocations[trainNumber]:
                        fullUp2DateTrainLocations[trainNumber]['last_time_in_train_set_str'] = None
                except (json.JSONDecodeError, TypeError) as e:
                    # If JSON parsing fails, initialize with empty history sub-dict
                    fullUp2DateTrainLocations[trainNumber] = {'history': {}, 'previous_train_sets': [], 'last_time_in_train_set': None, 'last_time_in_train_set_str': None}
                    eventMsg = f'Failed to parse position_history for train {trainNumber}: {str(e)}'
                    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
            else:
                # No position history exists, initialize with empty history sub-dict and previous_train_sets
                fullUp2DateTrainLocations[trainNumber] = {'history': {}, 'previous_train_sets': [], 'last_time_in_train_set': None, 'last_time_in_train_set_str': None}
        eventMsg = 'Finished loading historical position data for trains from database'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
   


        #
        # Get vehicle positions via api call
        # 
        vehiclePositionsResponse = apiRequest(atVehiclePosURL, True, 'Vehicle positions')
        apiTimestampPosix = vehiclePositionsResponse['response']['header']['timestamp']
        
        #
        # Get a list of all trains in the "fmt_train_details" table - known trains as it were.
        #
        cursorTrainList = DBConnection.cursor(dictionary=True)
        sqlQuery = 'select train_number from fmt_train_details'
        cursorTrainList.execute(sqlQuery)
        knownTrains = []
        for currTrain in cursorTrainList:
            knownTrains.append(currTrain['train_number'])

        #
        # Load train data from api call
        #
        #        
        for currVehicle in vehiclePositionsResponse['response']['entity']:
            #
            # Determine if this vehicle is a train
            # - We used to look at if the vehicle id was 5 digits and started with 59
            #   As the train numbers are already at 59958 I think it could easily go past
            #   1000 and that would break our validation, instead I am checking if the 'label'
            #   starts 'AMP '
            #
            if 'label' in currVehicle['vehicle']['vehicle']:
                if currVehicle['vehicle']['vehicle']['label'][:4] == 'AMP ':
                    #
                    # If it is a train
                    #
                    # With little alternative we will determine that the vehicle is a Train it has an 'id' tha begins 59
                    # Also it has a 'label' that begins with 'AMP ' - note the space and uppercase.
                    #
                    currTrainNo = currVehicle['vehicle']['vehicle']['label'][4:].strip()
                    trainDetails['train'].update({currTrainNo:currVehicle})
                    rawTrainDetails['train'].update({currTrainNo:copy.deepcopy(currVehicle)})


                    # Initiall set this train as not a part of a multi-part train
                    trainDetails['train'][currTrainNo]['currently_part_of_multi-train'] = False

                    # 
                    # We want a flag to say the train data for this train is not valid
                    # Currently this would only happen if we couldn't find it's position, but moving forward
                    # there may be other reasons
                    #
                    trainDetails['train'][currTrainNo]['train_data_is_valid'] = True                    
                    headingToBritomart = 'na'

                    #
                    # It seems sometimes the "bearing" value is an int and sometimes a stg in the json response :-(
                    # so we convert to a string for consistency, but we will still need to convert to an int later when we want to do calculations with it.
                    #
                    if 'bearing' in currVehicle['vehicle']['position']:
                        trainDetails['train'][currTrainNo]['vehicle']['position']['bearing'] = str(currVehicle['vehicle']['position']['bearing'])

                    currLatitude = trainDetails['train'][currTrainNo]['vehicle']['position']['latitude'] 
                    currLongitude = trainDetails['train'][currTrainNo]['vehicle']['position']['longitude']
                    imgCoords = geographicLocToImgLoc(currLatitude, currLongitude, trackDetails)
                    friendlyName = 'AM' + currTrainNo

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
                    maxSearchRadiusReached = 0
                    for searchRadius in range(0,(maxSearchRadius+1)):
                        currSearchRadius = searchRadius
                        if maxSearchRadiusReached < currSearchRadius:
                            maxSearchRadiusReached = currSearchRadius

                        for yNewPos in range((yCoord - searchRadius),(yCoord + searchRadius + 1)):
                            for xNewPos in range((xCoord - searchRadius),(xCoord + searchRadius + 1)):
                                #
                                # Ensure the X and Y positions are betweem zero and the   
                                # maximum image size
                                #   
                                rgbXPos = xNewPos
                                if rgbXPos < 0:
                                    rgbXPos = 0
                                if rgbXPos > mapContext.width:
                                    rgbXPos = mapContext.width
                                rgbYPos = yNewPos
                                if rgbYPos < 0:
                                    rgbYPos = 0 
                                if rgbYPos > mapContext.height:
                                    rgbYPos = mapContext.height

                                rgbValue = mapContext.getpixel((rgbXPos,rgbYPos)) 
                                hexValue = '#{:02x}{:02x}{:02x}'.format(*rgbValue).lower()     # Lowercase for searching
                                if hexValue != '#ffffff':
                                    break
                            if hexValue != '#ffffff':
                                break
                        if hexValue != '#ffffff':
                            break

                    if hexValue in list(trackDetails['hex_values']):

                        if currSearchRadius > stdSearchRadius:
                            eventMsg =  'stdSearchRadius = ' + str(stdSearchRadius) + '\n' + \
                                        'Train = ' + str(currTrainNo) + '\n' + \
                                        'currLatitude = ' + str(currLatitude) + '\n' + \
                                        'currLongitude = ' + str(currLongitude) + '\n' + \
                                        'currSearchRadius = ' + str(currSearchRadius) + '\n'
                            eventLogger('warn', eventMsg, 'Train was outside stanard search radius \'' + friendlyName + '\'', str(inspect.currentframe().f_lineno))

                        trainDetails['train'][currTrainNo].update({'section': trackDetails['hex_values'][hexValue]})
                        trainDetails['train'][currTrainNo].update({'search_radius':currSearchRadius})
                        currSectionBearing = trackDetails['hex_values'][hexValue]['bearing_to_britomart_int']

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
                            try:
                                currTrainBearing = int(float(currTrainBearingStr))
                                trainHasValidBearing = True
                            except (ValueError, TypeError):
                                eventMsg = 'Trains bearing is not a valid number: \'' + str(currTrainBearingStr) + '\', train = ' + str(currTrainNo)
                                eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                        else:
                            eventMsg = 'Train does not have a \'bearing\' value. Train = ' + str(currTrainNo)
                            eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

                        trainDetails['train'][currTrainNo].update({'heading_to_britomart':'na'})
                        headingToBritomart = 'na'
                        if (currSectionBearing != -1) and trainHasValidBearing:
                            bearingDelta = smallestAngleBetween(currTrainBearing,currSectionBearing)
                            headingToBritomart = 'N'
                            if bearingDelta < 90:
                                #
                                # If the difference between the trains bearing and the bearing of the track section to the city,
                                # is less than 90 degrees, in other words if the train is more or less pointing in the same direction
                                # as the direction to the city for this section of the track, 
                                #
                                headingToBritomart = 'Y'
                            trainDetails['train'][currTrainNo].update({'heading_to_britomart':headingToBritomart})
                            trainDetails['train'][currTrainNo].update({'bearing_delta_between_section_and_train':bearingDelta})

                        #
                        # If the train has changed direction, as in headingToBritomart has changed from 'Y' to 'N' or visaversa
                        # then we need to reset the front_train_history in fmt_train_sets
                        #
                        # As fmt_train_sets has not been loaded yet for this loop, we should be good to delete this column
                        #
                        if headingToBritomart in ['Y','N'] and currTrainNo in allDBTrainDetails and allDBTrainDetails[currTrainNo]['heading_to_britomart'] in ['Y','N']:    
                            if headingToBritomart != allDBTrainDetails[currTrainNo]['heading_to_britomart']:
                                eventMsg = 'Train ' + str(currTrainNo) + ' has changed direction. Updating \'fmt_trips\' details...'
                                eventLogger('info', eventMsg, 'Train Direction Change for train ' + str(currTrainNo), str(inspect.currentframe().f_lineno))
                                try:
                                    updateQuery = '''   UPDATE 
                                                            fmt_train_sets
                                                        SET
                                                            front_train_history = null
                                                        WHERE 
                                                            FIND_IN_SET( %s, train_set) > 0
                                                  '''
                                    updateValues = (str(currTrainNo),)
                                    cursorTrainList.execute(updateQuery, updateValues)
                                    DBConnection.commit()
                                except mysql.connector.Error as err:
                                    eventMsg = str(err)
                                    eventLogger('error', eventMsg, 'Error resetting front_train_history in database table \'fmt_train_sets\' after train changed direction.', str(inspect.currentframe().f_lineno))
                    
                        #
                        # Update the database train details
                        #                       
                        trainDetails['train'][currTrainNo].update({'friendly_name':friendlyName})
                        trainLabel = trainDetails['train'][currTrainNo]['vehicle']['vehicle']['label']
                        trainOdometer = -1
                        if 'odometer' in trainDetails['train'][currTrainNo]['vehicle']['position']:
                            trainOdometer = trainDetails['train'][currTrainNo]['vehicle']['position']['odometer']
                        customName = friendlyName
                        imageURL = specialTrainDetails['0']['train_featured_img_url']
                        smallImageURL = specialTrainDetails['0']['train_small_img_url']
                        trainDescription = specialTrainDetails['0']['train_description']
                        specialTrain = specialTrainDetails['0']['special_train']
                        geoLocation = str(trainDetails['train'][currTrainNo]['vehicle']['position']['latitude']) + ',' + \
                                      str(trainDetails['train'][currTrainNo]['vehicle']['position']['longitude'])                        
                        
                        # Work out the trip id
                        currentTripID = 'oos'
                        if 'trip' in trainDetails['train'][currTrainNo]['vehicle']:
                            currentTripID = trainDetails['train'][currTrainNo]['vehicle']['trip']['trip_id']

                        if currTrainNo in specialTrainDetails:
                            customName = specialTrainDetails[currTrainNo]['custom_name']
                            imageURL = specialTrainDetails[currTrainNo]['train_featured_img_url']
                            smallImageURL = specialTrainDetails[currTrainNo]['train_small_img_url']
                            trainDescription = specialTrainDetails[currTrainNo]['train_description']
                            specialTrain = specialTrainDetails[currTrainNo]['special_train']
                        if currTrainNo in knownTrains:
                            
                            try:
                                updateQuery = '''   UPDATE fmt_train_details 
                                                    SET vehicle_label = %s, 
                                                        friendly_name = %s,
                                                        odometer = %s, 
                                                        train_featured_img_url = %s, 
                                                        train_small_img_url = %s, 
                                                        train_description = %s, 
                                                        custom_name =%s, 
                                                        geo_location = %s, 
                                                        trip_id = %s,
                                                        special_train = %s,
                                                        heading_to_britomart = %s
                                                    WHERE train_number = %s'''
                                updateValues = (trainLabel,
                                                friendlyName,
                                                trainOdometer,
                                                imageURL,
                                                smallImageURL,
                                                trainDescription,
                                                customName,
                                                geoLocation,
                                                currentTripID,
                                                specialTrain,           
                                                headingToBritomart,
                                                currTrainNo                                               
                                                )
                                cursorTrainList.execute(updateQuery, updateValues)
                                DBConnection.commit()
                            except mysql.connector.Error as err:
                                eventMsg = str(err)
                                eventLogger('error', eventMsg, 'Error updating train details, in database table \'fmt_train_details\'.', str(inspect.currentframe().f_lineno))
                        else:
                            try:
                                insertQuery = ''' INSERT INTO fmt_train_details 
                                                (vehicle_label,
                                                friendly_name,
                                                odometer,
                                                train_featured_img_url,
                                                train_small_img_url,
                                                train_description,
                                                custom_name,
                                                train_number,
                                                last_updated,
                                                geo_location,
                                                trip_id,
                                                special_train,
                                                heading_to_britomart

                                                )
                                                VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                                insertValues = (trainLabel,
                                                friendlyName,
                                                trainOdometer,
                                                imageURL,
                                                smallImageURL,
                                                trainDescription,
                                                customName,
                                                currTrainNo,
                                                posixtoDateTime(trainDetails['train'][currTrainNo]['vehicle']['timestamp']),
                                                geoLocation,
                                                currentTripID,  
                                                specialTrain,
                                                headingToBritomart
                                                )
                                cursorTrainList.execute(insertQuery, insertValues)
                                DBConnection.commit()
                            except mysql.connector.Error as err:
                                eventMsg = 'Error inserting new train details, in table \'fmt_train_details\'.' + '\n\n' + \
                                           'trainLabel                      : ' + str(trainLabel) + '\n' + \
                                           'friendlyName                    : ' + str(friendlyName) + '\n' + \
                                           'trainOdometer                   : ' + str(trainOdometer) + '\n' + \
                                           'imageURL                        : ' + str(imageURL) + '\n' + \
                                           'smallImageURL                   : ' + str(smallImageURL) + '\n' + \
                                           'trainDescription                : ' + str(trainDescription) + '\n' + \
                                           'customName                      : ' + str(customName) + '\n' + \
                                           'currTrainNo                     : ' + str(currTrainNo) + '\n' + \
                                           'at britomart end                : ' + 'na' + '\n' + \
                                           'section id                      : ' + str(trainDetails['train'][currTrainNo]['section']['id']) + '\n' + \
                                           'timestamp                       : ' + str(posixtoDateTime(trainDetails['train'][currTrainNo]['vehicle']['timestamp'])) + '\n' + \
                                           str(err)
                                eventLogger('error', eventMsg, 'Error inserting new train details, in table \'fmt_train_details\'', str(inspect.currentframe().f_lineno))

                        currSectionID = trainDetails['train'][currTrainNo]['section']['id']
                        if currSectionID not in trainDetails['section']:
                            trainDetails['section'].update({
                                                            currSectionID:{
                                                                'trains':{},
                                                                'detail':trainDetails['train'][currTrainNo]['section'],
                                                            }})
                        trainDetails['section'][currSectionID]['trains'].update({currTrainNo:trainDetails['train'][currTrainNo]})

                    else:
                        trainDetails['train'][currTrainNo]['train_data_is_valid'] = False

                        if (currLatitude == 0) and (currLongitude == 0):
                            eventMsg = 'Exiting due to train being at 0,0 coordinates - debug'
                        else:
                            eventMsg =  'maxSearchRadiusReached = ' + str(maxSearchRadiusReached) + '\n' + \
                                    'Train = ' + str(currTrainNo) + '\n' + \
                                    'currLatitude = ' + str(currLatitude) + '\n' + \
                                    'currLongitude = ' + str(currLongitude) + '\n'
                        eventLogger('warn', eventMsg, 'Track details not found for train \'' + friendlyName + '\'', str(inspect.currentframe().f_lineno))

                    #############
                    #
                    # Add current position to historical locations
                    #
                    ############# 
                    if currTrainNo not in fullUp2DateTrainLocations:
                        fullUp2DateTrainLocations[currTrainNo] = {'history': {}, 'previous_train_sets': [], 'last_time_in_train_set': None, 'last_time_in_train_set_str': None}
                    
                    # Get the timestamp for this position update (ensure it's an integer)
                    positionTimestamp = int(currVehicle['vehicle']['timestamp'])
                    
                    # Build position record
                    positionRecord = {
                        'latitude': currVehicle['vehicle']['position']['latitude'],
                        'longitude': currVehicle['vehicle']['position']['longitude']
                    }
                    
                    # Add optional fields if they exist
                    if 'bearing' in currVehicle['vehicle']['position']:
                        positionRecord['bearing'] = str(currVehicle['vehicle']['position']['bearing'])
                    else:
                        positionRecord['bearing'] = None
                        
                    if 'speed' in currVehicle['vehicle']['position']:
                        positionRecord['speed'] = currVehicle['vehicle']['position']['speed']
                    else:
                        positionRecord['speed'] = None
                    
                    # Add human-readable timestamp
                    positionRecord['timestamp'] = str(posixtoDateTime(positionTimestamp))
                    
                    # Add heading_to_britomart (will be updated later after track position is determined)
                    positionRecord['heading_to_britomart'] = 'na'
                    
                    # Add section (will be updated later after track position is determined)
                    positionRecord['section'] = None
                    
                    # Add to historical locations with timestamp as key (under 'history' sub-dict)
                    fullUp2DateTrainLocations[currTrainNo]['history'][positionTimestamp] = positionRecord
                    
                    # Update heading_to_britomart and section if they were already calculated for this train
                    if 'heading_to_britomart' in trainDetails['train'][currTrainNo]:
                        positionRecord['heading_to_britomart'] = trainDetails['train'][currTrainNo]['heading_to_britomart']
                    if 'section' in trainDetails['train'][currTrainNo]:
                        positionRecord['section'] = trainDetails['train'][currTrainNo]['section']

                    #
                    # Clean up old position history - keep only recent positions within retention period
                    #
                    cutoffTimestamp = positionTimestamp - (locationHistoryRetentionPeriodMin * 60)
                    
                    # Filter out timestamps older than the retention period from history sub-dict
                    # Skip any non-integer keys that may exist due to legacy data
                    filteredHistory = {}
                    for ts, data in fullUp2DateTrainLocations[currTrainNo]['history'].items():
                        if isinstance(ts, int) and ts >= cutoffTimestamp:
                            filteredHistory[ts] = data
                    
                    # Update history with only the recent positions (preserves other top-level keys)
                    fullUp2DateTrainLocations[currTrainNo]['history'] = filteredHistory
                    
                    #
                    # Find the train's position at a common historical timestamp using interpolation
                    # Use current system time rather than position timestamp for more accurate calculation
                    #
                    commonTimestampLocation = None
                    
                    
                    # Find timestamps before and after the target
                    timestampBefore = None
                    timestampAfter = None
                    
                    # Loop through all timestamps in history to find the closest before and after target
                    # Dictionary keys are unique, so each timestamp appears only once
                    # The <= and >= comparisons work correctly since we won't see duplicates
                    for ts in fullUp2DateTrainLocations[currTrainNo]['history'].keys():
                        if ts <= commonLocationTargetTimestamp:
                            # This timestamp is before or at target
                            if timestampBefore is None or ts > timestampBefore:
                                timestampBefore = ts
                        
                        if ts >= commonLocationTargetTimestamp:
                            # This timestamp is after or at target
                            if timestampAfter is None or ts < timestampAfter:
                                timestampAfter = ts
                    
                    # Calculate interpolated position
                    if timestampBefore is not None and timestampAfter is not None:
                        if timestampBefore == timestampAfter:
                            # Exact match - use the position directly but update timestamp to target
                            commonTimestampLocation = fullUp2DateTrainLocations[currTrainNo]['history'][timestampBefore].copy()
                            commonTimestampLocation['timestamp'] = str(posixtoDateTime(commonLocationTargetTimestamp))
                            commonTimestampLocation['unix_timestamp'] = commonLocationTargetTimestamp
                            commonTimestampLocation['ratio_from_before_to_target'] = 0
                            commonTimestampLocation['timestampBefore'] = timestampBefore
                            commonTimestampLocation['timestampAfter'] = timestampAfter
                        else:
                            # Interpolate between the two positions
                            positionBefore = fullUp2DateTrainLocations[currTrainNo]['history'][timestampBefore]
                            positionAfter = fullUp2DateTrainLocations[currTrainNo]['history'][timestampAfter]
                            
                            # Calculate time ratio (how far between the two timestamps)
                            totalTimeDiff = timestampAfter - timestampBefore
                            targetTimeDiff = commonLocationTargetTimestamp - timestampBefore
                            ratio = targetTimeDiff / totalTimeDiff
                            
                            # Interpolate latitude and longitude
                            interpolatedLat = positionBefore['latitude'] + (positionAfter['latitude'] - positionBefore['latitude']) * ratio
                            interpolatedLon = positionBefore['longitude'] + (positionAfter['longitude'] - positionBefore['longitude']) * ratio
                            
                            commonTimestampLocation = {
                                'latitude': interpolatedLat,
                                'longitude': interpolatedLon,
                                'bearing': positionBefore.get('bearing'),  # Use before position's bearing
                                'speed': positionBefore.get('speed'),
                                'heading_to_britomart': positionBefore.get('heading_to_britomart'),
                                'section': positionBefore.get('section'),
                                'timestamp': str(posixtoDateTime(commonLocationTargetTimestamp)),
                                'unix_timestamp': commonLocationTargetTimestamp,
                                'ratio_from_before_to_target': ratio,
                                'timestampBefore': timestampBefore,
                                'timestampAfter': timestampAfter
                            }
                    elif timestampBefore is not None:
                        # Only have data before target - use the closest before but update timestamp to target
                        commonTimestampLocation = fullUp2DateTrainLocations[currTrainNo]['history'][timestampBefore].copy()
                        commonTimestampLocation['timestamp'] = str(posixtoDateTime(commonLocationTargetTimestamp))
                        commonTimestampLocation['unix_timestamp'] = commonLocationTargetTimestamp
                        commonTimestampLocation['ratio_from_before_to_target'] = None
                        commonTimestampLocation['timestampBefore'] = timestampBefore
                        commonTimestampLocation['timestampAfter'] = None
                    elif timestampAfter is not None:
                        # Only have data after target - use the closest after but update timestamp to target
                        commonTimestampLocation = fullUp2DateTrainLocations[currTrainNo]['history'][timestampAfter].copy()
                        commonTimestampLocation['timestamp'] = str(posixtoDateTime(commonLocationTargetTimestamp))
                        commonTimestampLocation['unix_timestamp'] = commonLocationTargetTimestamp
                        commonTimestampLocation['ratio_from_before_to_target'] = None
                        commonTimestampLocation['timestampBefore'] = None
                        commonTimestampLocation['timestampAfter'] = timestampAfter

                    # Store the calculated common timestamp location in fullUp2DateTrainLocations
                    if commonTimestampLocation is not None:
                        fullUp2DateTrainLocations[currTrainNo]['common_timestamp_location'] = commonTimestampLocation

                    #
                    # Save updated position history back to database
                    #
                    try:
                        positionHistoryJSON = json.dumps(fullUp2DateTrainLocations[currTrainNo])
                        updateQuery = 'UPDATE fmt_train_details SET position_history = %s WHERE train_number = %s'
                        updateValues = (positionHistoryJSON, currTrainNo)
                        cursorTrainList.execute(updateQuery, updateValues)
                        DBConnection.commit()
                    except mysql.connector.Error as err:
                        eventMsg = str(err)
                        eventLogger('error', eventMsg, f'Error updating position_history for train {currTrainNo} in table \'fmt_train_details\'.', str(inspect.currentframe().f_lineno))
                    except (TypeError, ValueError) as err:
                        eventMsg = str(err)
                        eventLogger('error', eventMsg, f'Error serializing position_history to JSON for train {currTrainNo}.', str(inspect.currentframe().f_lineno))        


        return trainDetails

    #
    # Find train sets (trains joined together)
    #
    # This function analyzes the common_timestamp_location data for all trains
    # to identify which trains are physically joined together as multi-car sets.
    # 
    # Trains are considered to be in the same set if:
    # - They have the same heading_to_britomart value (going the same direction)
    # - Their bearing and speed are not zero (indicating valid movement data)
    # - The distance between them is within maxMetersBetweenTrainsInASet meters
    # - They are not at an interchange station (type 'I') where multiple trains 
    #   can be close together but not actually joined
    #
    def findTrainSets():
        global fullUp2DateTrainLocations
        global trainSets
        
        eventMsg = 'Running findTrainSets()'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
        
        # Dictionary to store identified train sets
        # Structure: {set_id: {'trains': [train1, train2, ...], 'heading': 'Y'/'N', 'section': section_id}}
        trainSets = {}
        setIdCounter = 1
        
        # List to track which trains have already been assigned to a set
        assignedTrains = set()
        
        # Build list of candidate trains with valid common_timestamp_location data
        candidateTrains = []
        
        for trainNumber, trainData in fullUp2DateTrainLocations.items():
            # Check if this train has common_timestamp_location data
            if 'common_timestamp_location' not in trainData:
                continue
                
            commonLoc = trainData['common_timestamp_location']
            
            # Validate required fields exist
            if not all(key in commonLoc for key in ['heading_to_britomart', 'bearing', 'latitude', 'longitude', 'section']):
                continue
            
            # Skip trains with invalid heading_to_britomart
            if commonLoc['heading_to_britomart'] not in ['Y', 'N']:
                continue
            
            # Skip trains where bearing is zero and thus we can't determine if the train 'heading_to_britomart'
            try:
                bearing = float(commonLoc['bearing']) if commonLoc['bearing'] is not None else 0
            except (ValueError, TypeError):
                continue
                
            if bearing == 0:
                continue
            
            # Skip trains at interchange sections, stabling yards, or end of line sections  :
            # - 'I' (Interchange): Complex sections like Newmarket with many platforms where multiple
            #   trains can be close together but not actually joined - best to skip these
            # - 'Y' (Stabling Yard): Train yards where trains are parked/maintained
            # - 'E' (End of Line): Sections at the end of the line where trains may be stationary   
            if commonLoc['section'] is not None and isinstance(commonLoc['section'], dict):
                if commonLoc['section'].get('type') in sectionTypesToIgnoreForTrainSets:
                    continue
            
            # Add to candidate list
            candidateTrains.append({
                'train_number': trainNumber,
                'heading_to_britomart': commonLoc['heading_to_britomart'],
                'latitude': commonLoc['latitude'],
                'longitude': commonLoc['longitude'],
                'bearing': bearing,
                'section': commonLoc['section'],
                'timestamp-str-for-common-timestamp': commonLoc['timestamp'],
                'unix_timestamp-at-common-timestamp': commonLoc['unix_timestamp'],
                'latitude-at-common-timestamp': commonLoc['latitude'], 
                'longitude-at-common-timestamp': commonLoc['longitude'],
                'timestampBefore-before-common-timestamp': commonLoc['timestampBefore'],
                'latitude-before-common-timestamp': trainData['history'][commonLoc['timestampBefore']]['latitude'] if commonLoc['timestampBefore'] is not None else None,
                'longitude-before-common-timestamp': trainData['history'][commonLoc['timestampBefore']]['longitude'] if commonLoc['timestampBefore'] is not None else None,
            })
        
        
        # Group trains by heading_to_britomart for efficiency
        trainsByHeading = {'Y': [], 'N': []}
        listCandidateTrains = []    
        for train in candidateTrains:
            trainsByHeading[train['heading_to_britomart']].append(train)
            listCandidateTrains.append(train['train_number'])

        # #########
        #
        # Go through all trains and look for any "train sets" where 2 
        # or more trains are travelling together, so what we now call a 6 or
        # 9 car train
        #
        # for trains to be in a set they must both be travelling in the same direction. This
        # is the 'heading_to_britomart" value.
        #
        # Secondly to be travelling together they must be less than "maxMetersBetweenTrainsInASet" 
        # meters apart.
        #
        # To do this we break it into two loops
        # - The outer one loops through trains going in the same direction "for heading, trains in trainsByHeading.items():)"
        # - The inner one compares each train with every other train in the same heading group
        #
        ###########

        # Loop through all trains going in the same direction
        for heading, trains in trainsByHeading.items():

            # Compare each train with every other train in the same heading group, ie. going in the same direction
            # we need two nested loops to compare each train with every other train, but we 
            # can skip comparisons for trains that have already been assigned to a set
            for currTrainIdx, currTrainDict in enumerate(trains):
                # Outer loop is currTrainDict

                # Skip if currTrainDict is already assigned to a set
                if currTrainDict['train_number'] in assignedTrains:
                    continue
                    
                # Skip if the timestamp for this train is too old
                if currTrainDict['unix_timestamp-at-common-timestamp'] < (int(time.time()) - (locationHistoryRetentionPeriodMin * 60)):
                    continue

                # Start a potential new set with this train
                potentialSet = [currTrainDict]
                distance = -1
                
                # Check all other trains in this heading group
                for compareTrainIdx, compareTrainDict in enumerate(trains):
                    # Inner loop is compareTrainDict - compare with currTrainDict

                    if currTrainIdx == compareTrainIdx or compareTrainDict['train_number'] in assignedTrains:
                        # If it's the same train or compareTrainDict is already assigned to a set, skip
                        continue

                    if currTrainDict['unix_timestamp-at-common-timestamp'] != compareTrainDict['unix_timestamp-at-common-timestamp']:
                        # If the common timestamp locations are not from the same timestamp, skip
                        continue
                    
                    # Calculate distance between currTrainDict and compareTrainDict using haversine
                    compareDistance = haversine(
                        (currTrainDict['latitude'], currTrainDict['longitude']),
                        (compareTrainDict['latitude'], compareTrainDict['longitude']),
                        unit=Unit.METERS
                    )
                    
                    # If trains are close enough, add to potential set
                    if compareDistance <= maxMetersBetweenTrainsInASet:
                        potentialSet.append(compareTrainDict)
                        distance = compareDistance
                
                # If we found at least 2 trains close together, create a set
                if len(potentialSet) >= 2:
                    trainNumbers = [t['train_number'] for t in potentialSet]
                    potentialSetDict = {}
                    for foundTrain in potentialSet:
                        potentialSetDict.update({foundTrain['train_number']: foundTrain})

                    trainSets[setIdCounter] = {
                        'trains': potentialSetDict,
                        'heading_to_britomart': heading,
                        'section': currTrainDict['section'].get('id') if currTrainDict['section'] else None,
                        'train_count': len(trainNumbers),
                        'distance_between_trains_meters': distance
                    }
                    
                    # Mark these trains as assigned
                    for trainNum in trainNumbers:
                        assignedTrains.add(trainNum)
                    
                    eventMsg = f"Found train set {setIdCounter}: {trainNumbers} (heading_to_britomart={heading})"
                    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                    
                    setIdCounter += 1
        
        eventMsg = f'Found {len(trainSets)} train set(s) with {len(assignedTrains)} train(s) total'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

      
        #
        # Get a list of trains, 'unassignedTrains', that were candidates but were not assigned to any train set
        #
        unassignedTrains = []
        for trainNum in listCandidateTrains:
            if trainNum not in assignedTrains:
                unassignedTrains.append(trainNum)
        
        #
        # At this point we have identified which trains are likely joined together in sets based on their
        # proximity and movement data.
        #
        # Now update each train's previous_train_sets to track which trains it has been consistently with.
        # This helps identify trains that are reliably connected across multiple API cycles.
        #
        for setId, setData in trainSets.items():
            trainNumbers = list(setData['trains'].keys())
            
            # For each train in this set, update its previous_train_sets
            for trainNum in trainNumbers:
                if trainNum in fullUp2DateTrainLocations:
                    # Ensure previous_train_sets and last_time_in_train_set exist
                    if 'previous_train_sets' not in fullUp2DateTrainLocations[trainNum]:
                        fullUp2DateTrainLocations[trainNum]['previous_train_sets'] = []
                    if 'last_time_in_train_set' not in fullUp2DateTrainLocations[trainNum]:
                        fullUp2DateTrainLocations[trainNum]['last_time_in_train_set'] = None
                    if 'last_time_in_train_set_str' not in fullUp2DateTrainLocations[trainNum]:
                        fullUp2DateTrainLocations[trainNum]['last_time_in_train_set_str'] = None
                    
                    # Create list of OTHER trains in this set (excluding the current train)
                    otherTrainsInSet = [t for t in trainNumbers if t != trainNum]
                    
                    # Add this set to the front of the list (most recent first)
                    fullUp2DateTrainLocations[trainNum]['previous_train_sets'].insert(0, otherTrainsInSet)
                    
                    # Limit the history to maxTrainSetHistoryEntries
                    if len(fullUp2DateTrainLocations[trainNum]['previous_train_sets']) > maxTrainSetHistoryEntries:
                        fullUp2DateTrainLocations[trainNum]['previous_train_sets'] = \
                            fullUp2DateTrainLocations[trainNum]['previous_train_sets'][:maxTrainSetHistoryEntries]
                    
                    # Update last_time_in_train_set with current timestamp
                    currentTimestamp = int(time.time())
                    fullUp2DateTrainLocations[trainNum]['last_time_in_train_set'] = currentTimestamp
                    fullUp2DateTrainLocations[trainNum]['last_time_in_train_set_str'] = str(posixtoDateTime(currentTimestamp))
                    
                    eventMsg = f"Updated previous_train_sets for train {trainNum}: now has {len(fullUp2DateTrainLocations[trainNum]['previous_train_sets'])} entries"
                    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

        # 
        # Loop through all trains that were candidates to be part of a train set
        # but ultimately were not valid members of a train set
        #
        # In this case we want to assign an empty train set to these trains history
        # 
        for trainNum in unassignedTrains:
            if trainNum in fullUp2DateTrainLocations:
                # Ensure previous_train_sets and last_time_in_train_set exist
                if 'previous_train_sets' not in fullUp2DateTrainLocations[trainNum]:
                    fullUp2DateTrainLocations[trainNum]['previous_train_sets'] = []
                if 'last_time_in_train_set' not in fullUp2DateTrainLocations[trainNum]:
                    fullUp2DateTrainLocations[trainNum]['last_time_in_train_set'] = None
                if 'last_time_in_train_set_str' not in fullUp2DateTrainLocations[trainNum]:
                    fullUp2DateTrainLocations[trainNum]['last_time_in_train_set_str'] = None
                
                # Add an empty list to indicate train was alone in this cycle (most recent first)
                fullUp2DateTrainLocations[trainNum]['previous_train_sets'].insert(0, [])
                
                # Limit the history to maxTrainSetHistoryEntries
                if len(fullUp2DateTrainLocations[trainNum]['previous_train_sets']) > maxTrainSetHistoryEntries:
                    fullUp2DateTrainLocations[trainNum]['previous_train_sets'] = \
                        fullUp2DateTrainLocations[trainNum]['previous_train_sets'][:maxTrainSetHistoryEntries]
                
                eventMsg = f"Train {trainNum} was not in a set - added empty entry to previous_train_sets"
                eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
        
        #
        # We need to loop through all discovered train sets for this cycle in order to work out 
        # which train is in front.
        #
        # Strategy to find the train in front
        # -----------------------------------
        #
        # Key points
        #
        # - While working out if a train is in a train set we calculate all trains
        #   location at a common timestamp in the past, which is 'unix_timestamp-at-common-timestamp'
        #
        # - Each train in a train set has:
        #   'latitude-at-common-timestamp' and 'longitude-at-common-timestamp' which together
        #   represent that trains location at 'unix_timestamp-at-common-timestamp'
        #
        #   Each train also has 'latitude-before-common-timestamp' and 'longitude-before-common-timestamp'
        #   which represents the trains location immediately prior to the 'unix_timestamp-at-common-timestamp'
        #
        # The strategy
        #
        # for each train we will add up the distance between its common-timestamp location and each of the
        # before-common-timestamp locations for all trains, including itself.
        #
        # The train which has the largest total is the one in front.
        #
        # 
        eventMsg = f"Going through all discovered train sets to determine which train is in front"
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

        # Go through all train sets
        for currTrainSet in trainSets:
            print('Train set ', str(currTrainSet))  

            # For this to work all trains in the set must have a common-timestamp and a before-common-timestamp location
            firstTrainInSet = None
            largestDistanceBetweenTrains = -1
            secondlargestDistanceBetweenTrains = -1
            gapBetweenClosestTrains = 0
            trainSetCommonTimeStamp = None
            trainSetDataComplete = True
            trainsInSet = []

            print(' - set = ', str(trainSets[currTrainSet]['trains'].keys()))

            # For each train set iterate through all trains in the set
            for currTrainNoInSet in trainSets[currTrainSet]['trains']:

                trainsInSet.append(currTrainNoInSet)
                if firstTrainInSet is None:
                    firstTrainInSet = currTrainNoInSet
                    trainSetCommonTimeStamp = trainSets[currTrainSet]['trains'][currTrainNoInSet].get('unix_timestamp-at-common-timestamp')

                currTrainTotalDistance = 0
                
                print('  - Train number in set: ', str(currTrainNoInSet) + ', latitude-at-common-timestamp: ' + str(trainSets[currTrainSet]['trains'][currTrainNoInSet]['latitude-at-common-timestamp']))

                # Check current train has a common timestamp that equals 'trainSetCommonTimeStamp'
                if trainSets[currTrainSet]['trains'][currTrainNoInSet].get('unix_timestamp-at-common-timestamp') != trainSetCommonTimeStamp:
                    trainSetDataComplete = False
                    breakMsg = f"Train {currTrainNoInSet} in has a \'unix_timestamp-at-common-timestamp\' value of {trainSets[currTrainSet]['trains'][currTrainNoInSet].get('unix_timestamp-at-common-timestamp')}"
                    break

                # Check current train has a '...common-timestamp' location
                if 'latitude-at-common-timestamp' not in trainSets[currTrainSet]['trains'][currTrainNoInSet] or 'longitude-at-common-timestamp' not in trainSets[currTrainSet]['trains'][currTrainNoInSet]:
                    trainSetDataComplete = False
                    breakMsg = f"Train {currTrainNoInSet} in set {currTrainSet} does not have a common-timestamp location - cannot determine front train"
                    break 

                # Calculate distance between this trains common-timestamp location and the before-common-timestamp 
                # location of every train in the set (including itself)
                
                for subTrainNoInSet in trainSets[currTrainSet]['trains']:
                    print('    - Train number in set: ', str(subTrainNoInSet) + ', latitude-before-common-timestamp: ' + str(trainSets[currTrainSet]['trains'][subTrainNoInSet]['latitude-before-common-timestamp']))
                    
                    # Check current subtrain has a '...before-common-timestamp' location    
                    if 'latitude-before-common-timestamp' not in trainSets[currTrainSet]['trains'][subTrainNoInSet] or 'longitude-before-common-timestamp' not in trainSets[currTrainSet]['trains'][subTrainNoInSet]:
                        trainSetDataComplete = False
                        breakMsg = f"Train {subTrainNoInSet} in set {currTrainSet} does not have a before-common-timestamp location - cannot determine front train"
                        break  

                    # Calculate the distance between currTrainNoInSet's common-timestamp location and subTrainNoInSet's before-common-timestamp location using haversine    
                    currCommonLat = trainSets[currTrainSet]['trains'][currTrainNoInSet]['latitude-at-common-timestamp']
                    currCommonLon = trainSets[currTrainSet]['trains'][currTrainNoInSet]['longitude-at-common-timestamp']
                    subBeforeLat = trainSets[currTrainSet]['trains'][subTrainNoInSet]['latitude-before-common-timestamp']
                    subBeforeLon = trainSets[currTrainSet]['trains'][subTrainNoInSet]['longitude-before-common-timestamp']

                    if None in (currCommonLat, currCommonLon, subBeforeLat, subBeforeLon):
                        trainSetDataComplete = False
                        breakMsg = f"Train {subTrainNoInSet} in set {currTrainSet} has empty location details - cannot determine front train"
                        break

                    currDistanceMeters = haversine(
                        (currCommonLat, currCommonLon),
                        (subBeforeLat, subBeforeLon),
                        unit=Unit.METERS
                    )
                    currTrainTotalDistance += currDistanceMeters

                print('    - Total distance for train ', str(currTrainNoInSet), ' is ', str(currTrainTotalDistance))

                if (largestDistanceBetweenTrains == -1):
                    # if largestDistanceBetweenTrains is unset then set it
                    largestDistanceBetweenTrains = currTrainTotalDistance
                else:
                    if secondlargestDistanceBetweenTrains == -1:
                        # if secondlargestDistanceBetweenTrains is unset then set it
                        if currTrainTotalDistance > largestDistanceBetweenTrains:
                            secondlargestDistanceBetweenTrains = largestDistanceBetweenTrains
                            largestDistanceBetweenTrains = currTrainTotalDistance
                            firstTrainInSet = currTrainNoInSet
                        else:
                            secondlargestDistanceBetweenTrains = currTrainTotalDistance 
                    else:
                        if currTrainTotalDistance > largestDistanceBetweenTrains:
                            secondlargestDistanceBetweenTrains = largestDistanceBetweenTrains
                            largestDistanceBetweenTrains = currTrainTotalDistance
                            firstTrainInSet = currTrainNoInSet     
                        else:
                            if currTrainTotalDistance > secondlargestDistanceBetweenTrains:
                                secondlargestDistanceBetweenTrains = currTrainTotalDistance              

            gapBetweenClosestTrains = largestDistanceBetweenTrains - secondlargestDistanceBetweenTrains
            if trainSetDataComplete and (gapBetweenClosestTrains >= minSeparationForFrontTrainsMeters ):
                
                print('largestDistanceBetweenTrains : ', str(largestDistanceBetweenTrains) + ', secondlargestDistanceBetweenTrains: ' + str(secondlargestDistanceBetweenTrains) + ', gapBetweenClosestTrains: ' + str(gapBetweenClosestTrains))
                print(' Front train is ' + str(firstTrainInSet) + ' with a total distance of ' + str(int(largestDistanceBetweenTrains)) + ' and a gap of ' + str(int(gapBetweenClosestTrains)) + 'm.\n\n')

                #
                # We need to update fmt_train_sets
                # 

                # Create the train set string where the train numbers are sorted numerically and comma separated.
                trainsInSetSortedStr = ','.join(sorted(trainsInSet, key=int))
                newTrainSetDisplay = ''
                separatorStr = ''

                

                # Check if a record already exists
                cursorTrainSet = DBConnection.cursor(dictionary=True)
                print('= 2600 load fmt_train_sets')
                sqlQuery = 'SELECT * FROM fmt_train_sets WHERE train_set = \'' + trainsInSetSortedStr + '\';'
                try:
                    cursorTrainSet.execute(sqlQuery)
                except mysql.connector.Error as err:
                    eventMsg = str(err)
                    eventLogger('error', eventMsg, 'Error querying database table \'fmt_train_sets\' for current record.', str(inspect.currentframe().f_lineno))
                currDBTrainSet = cursorTrainSet.fetchone()
                if fullUp2DateTrainLocations[firstTrainInSet]['common_timestamp_location']['section']['id'] is not None:    
                    currTrainLocation = fullUp2DateTrainLocations[firstTrainInSet]['common_timestamp_location']['section']['id']
                else:
                    currTrainLocation = '-'
                trainSetExistsInDB = True
                if currDBTrainSet is None:  
                    trainSetExistsInDB = False
                    newTrainSetHistory = firstTrainInSet
                else:
                    existingFrontTrainHistory = currDBTrainSet.get('front_train_history') or ''
                    newTrainSetHistory = ','.join((firstTrainInSet + ',' + existingFrontTrainHistory).split(',')[:(maxPrevFrontTrainRecordsToKeep - 1)])

                if (currDBTrainSet is None) or (currDBTrainSet.get('train_set_debug') is None):
                    newTrainSetDebug = firstTrainInSet + '[' + currTrainLocation + ']'
                else:
                    newTrainSetDebug = ','.join((firstTrainInSet + '[' + currTrainLocation + ']' + ',' + currDBTrainSet['train_set_debug']).split(',')[:(maxPrevFrontTrainRecordsToKeep - 1)]) 

                print('- firstTrainInSet ' + firstTrainInSet + ' location ' + json.dumps(fullUp2DateTrainLocations[firstTrainInSet]['common_timestamp_location']['section']['id'], indent=4, sort_keys=True, default=str))

                #
                # Work out which train is in front based on what is in newTrainSetHistory.
                # The variable newTrainSetHistory holds a string list of the trains found to be
                # in front, so will look something like:
                #
                #      newTrainSetHistory = '117,578,117,117,117,644,578,644'
                #
                # This set of code will evaluate the rules in frontTrainRules and once a rule
                # is satisfied it will stop. So IT IS IMPORTANT TO ORDER THE RULES IN THE SEQUENCE YOU WANT THEM EVALUATED.
                #                                     =====================================================================
                #
                newTrainSetHistoryList = newTrainSetHistory.split(',')

                print('frontTrainRules: ', frontTrainRules)
                print('newTrainSetHistory: ', newTrainSetHistory)
                trainFoundInFront = None
                for currRuleStr in frontTrainRules:
                    ruleParts = currRuleStr.split('/')
                    minOccurrancesRequired, noToConsider = int(ruleParts[0]), int(ruleParts[1])
                    ruleSymbol = ruleParts[2] if len(ruleParts) > 2 else '*'
                    print(f'\n\ncurrRuleStr: {currRuleStr} , minOccurrancesRequired: {minOccurrancesRequired}, noToConsider: {noToConsider}, ruleSymbol: {ruleSymbol}')

                    newTrainSetHistoryListTruncated = newTrainSetHistoryList[:noToConsider]
                    maxOccurrancesFound = 0
                    trainWithMaxOccurrances = None
                    for trainInSet in newTrainSetHistoryListTruncated:
                        print(f' - trainInSet: {trainInSet}')
                        noOccurances = newTrainSetHistoryListTruncated.count(trainInSet)
                        print(f' - noOccurances: {noOccurances}')
                        if noOccurances > maxOccurrancesFound:
                            maxOccurrancesFound = noOccurances
                            trainWithMaxOccurrances = trainInSet
                    print(f' - trainWithMaxOccurrances: {trainWithMaxOccurrances}, maxOccurrancesFound: {maxOccurrancesFound}')

                    if maxOccurrancesFound >= minOccurrancesRequired:
                        print(f' - Train {trainWithMaxOccurrances} is in front according to rule {currRuleStr}')    
                        trainFoundInFront = trainWithMaxOccurrances
                        frontTrainSymbol = ruleSymbol
                    else:
                        print(f' - No train is in front according to rule {currRuleStr}')

                    if trainFoundInFront is not None:
                        break
                print(f'\n\nFinal train found in front: {trainFoundInFront}')


                # If there are enough to consider it the front train then update
                # newTrainSetDisplay so the front train has an asterix, '*', next to it.
                if trainFoundInFront is not None:
                    print('newTrainSetHistory = \'' + newTrainSetHistory + '\'')
                    for trainNo in trainsInSetSortedStr.split(','):
                        if trainNo == trainFoundInFront:
                            newTrainSetDisplay = newTrainSetDisplay + separatorStr + trainNo + frontTrainSymbol
                        else:
                            newTrainSetDisplay = newTrainSetDisplay + separatorStr + trainNo
                        separatorStr = ', '
                else:
                    newTrainSetDisplay = trainsInSetSortedStr


                print(' - trainsInSetSortedStr: ', trainsInSetSortedStr)
                print(' - newTrainSetDisplay: ', newTrainSetDisplay)
                print(' - newTrainSetHistory: ', newTrainSetHistory)

                newTrainSetDisplay += ' (' + ','.join(newTrainSetHistory.split(',')[:10]) + ')'

                # Update the DB
                print('= 2604 update fmt_train_sets')
                if trainSetExistsInDB:
                    # If the train set already exists, update the record
                    sqlUpdate = ''' UPDATE 
                                        fmt_train_sets 
                                    SET 
                                        train_set_display = %s, 
                                        front_train_history = %s, 
                                        train_set_debug = %s,
                                        updated = %s 
                                    WHERE 
                                        train_set = %s'''
                    updateValues = (newTrainSetDisplay, newTrainSetHistory, newTrainSetDebug, datetime.now(), trainsInSetSortedStr)   
                    try:
                        cursorTrainSet.execute(sqlUpdate, updateValues)
                        DBConnection.commit()
                    except mysql.connector.Error as err:
                        eventMsg = str(err)
                        eventLogger('error', eventMsg, 'Error updating database table \'fmt_train_sets\' for current record.', str(inspect.currentframe().f_lineno))        
                else:
                    # If the train set does not exist, insert a new record
                    print('= 2714 insert fmt_train_sets')
                    sqlInsert = 'INSERT INTO fmt_train_sets (train_set, train_set_display, front_train_history, train_set_debug, updated) VALUES (%s, %s, %s, %s, %s)'
                    insertValues = (trainsInSetSortedStr, newTrainSetDisplay, newTrainSetHistory, newTrainSetDebug, datetime.now())
                    try:
                        cursorTrainSet.execute(sqlInsert, insertValues)
                        DBConnection.commit()
                    except mysql.connector.Error as err:
                        eventMsg = str(err)
                        eventLogger('error', eventMsg, 'Error inserting into database table \'fmt_train_sets\' for new record.', str(inspect.currentframe().f_lineno))

        #
        # Cleanup the fmt_train_sets
        #
        eventMsg = f"Cleaning up \'fmt_train_sets\'.... "
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
        print('= 2729 delete fmt_train_sets')
        sqlCleanup = 'DELETE FROM fmt_train_sets  WHERE updated < now() - interval ' + str(maxRetensionTrainSetMinutes) + ' MINUTE;'
        cursorCleanup = DBConnection.cursor()
        try:
            cursorCleanup.execute(sqlCleanup)
            DBConnection.commit()
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error cleaning up database table \'fmt_train_sets\'.', str(inspect.currentframe().f_lineno))

        return

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

        eventMsg = 'Running drawMap()'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

        #
        # Set properties
        #

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

        #
        # Load track details csv into dict
        #
        with open(trackDetailsFilename, mode='r', encoding='windows-1252') as trackDetailsCSV:
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
                    eventMsg = 'There are at least two rows in this file with the ID \'' + currRowIDStr + '\'.'
                    eventLogger('error', eventMsg, 'Error with input details for \'' + trackDetailsFilename + '\'.', str(inspect.currentframe().f_lineno))

                # Check current hex hasn't been duplicated
                if currRow['color_hex'].lower() in trackDetails['hex_values']:
                    eventMsg = 'There are at least two rows in this file with the hex value \'' + currRow['color_hex'].lower() + '\'.'
                    eventLogger('error', eventMsg, 'Error with input details for \'' + trackDetailsFilename + '\'.', str(inspect.currentframe().f_lineno))

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
                    eventMsg = 'The row with ID \'' + currRowIDStr + '\' has a value of \'' + currRow['type'] + '\', for Section Type.' + '\n' + \
                               'This should be either \'N\' for Normal, \'S\' for Station, \'I\' for Interchange, or \'Y\' for Yard.'
                    eventLogger('error', eventMsg, 'Error with input details for \'' + trackDetailsFilename + '\'.', str(inspect.currentframe().f_lineno))

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
                    eventMsg = 'The problem was the line with \'ID\': ' + currRowIDStr + '\n' + \
                               'The value for bearing: \'' + currRow['bearing_to_britomart'] + '\'.' + '\n' + \
                               'This should either be a value from 0 to 360 or \'-1\''
                    eventLogger('error', eventMsg, 'Error with input details for \'' + trackDetailsFilename + '\'', str(inspect.currentframe().f_lineno))
                
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
                            eventMsg = 'The problem was the line with \'ID\': ' + currRowIDStr + '\n' + \
                                       'Points value causing an issue was: \'' + currPoint + '\'.' + '\n\n' + \
                                       'There should have been exactly two comma separated values but there weren\'t'
                            eventLogger('error', eventMsg, 'Error with input details for \'' + trackDetailsFilename + '\'.', str(inspect.currentframe().f_lineno))

                        #
                        # Check this point contains valid details
                        # As in they must be valid float values
                        #
                        try:
                            float(currPointSplit[0].strip())
                            float(currPointSplit[1].strip())
                        except ValueError as err:
                            eventMsg = 'The problem was the line with \'ID\': ' + currRowID + '\n' + \
                                       'Points value causing an issue was: \'' + currPoint + '\'.' + '\n\n' + \
                                       'The error returned was: ' + str(err)
                            eventLogger('error', eventMsg, 'Error with input details for \'' + trackDetailsFilename + '\'.', str(inspect.currentframe().f_lineno))

                        currLatitude = float(currPointSplit[0].strip())
                        currLongitude = float(currPointSplit[1].strip())
                        trackDetails['track_sections'][currRowID]['section_points'].update({pointCnt:{'latitude':currLatitude,'longitude':currLongitude}})

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
            
                # Calculate middle point for this section now that its points are loaded
                if currRowID in trackDetails['track_sections']:
                    section = trackDetails['track_sections'][currRowID]
                    points = section['section_points']
                    
                    if len(points) == 0:
                        # Empty points - skip middle point calculation
                        pass
                    elif len(points) == 1:
                        # Single point - use it as the middle point
                        first_point_key = list(points.keys())[0]
                        trackDetails['track_sections'][currRowID]['middle_point'] = {
                            'latitude': points[first_point_key]['latitude'],
                            'longitude': points[first_point_key]['longitude'],
                            'total_distance_km': 0.0,
                            'cumulative_distances': [0.0]
                        }
                    else:
                        # Multiple points - calculate distance-based middle point
                        cumulative_distances = [0.0]  # Start at 0
                        total_distance = 0.0
                        
                        # Build cumulative distance array
                        for point_num in range(1, len(points)):
                            prev_point = points[point_num]
                            curr_point = points[point_num + 1]
                            
                            distance = calculate_distance_km(
                                prev_point['latitude'], prev_point['longitude'],
                                curr_point['latitude'], curr_point['longitude']
                            )
                            
                            total_distance += distance
                            cumulative_distances.append(total_distance)
                        
                        # Find middle distance (50% of total)
                        middle_distance = total_distance / 2.0
                        
                        # Find which segment the middle distance falls in
                        middle_lat = None
                        middle_lon = None
                        
                        for i in range(len(cumulative_distances) - 1):
                            if cumulative_distances[i] <= middle_distance <= cumulative_distances[i + 1]:
                                # Middle point is between index i and index i+1
                                point1 = points[i + 1]
                                point2 = points[i + 2]
                                
                                # Calculate how far along this segment the middle point is
                                segment_start_distance = cumulative_distances[i]
                                segment_end_distance = cumulative_distances[i + 1]
                                segment_length = segment_end_distance - segment_start_distance
                                
                                if segment_length > 0:
                                    # How far along this segment (0.0 to 1.0)
                                    ratio = (middle_distance - segment_start_distance) / segment_length
                                    
                                    # Interpolate between the two points
                                    middle_lat = point1['latitude'] + (point2['latitude'] - point1['latitude']) * ratio
                                    middle_lon = point1['longitude'] + (point2['longitude'] - point1['longitude']) * ratio
                                else:
                                    # Zero-length segment, use first point
                                    middle_lat = point1['latitude']
                                    middle_lon = point1['longitude']
                                break
                        
                        # Store the middle point
                        if middle_lat is not None and middle_lon is not None:
                            trackDetails['track_sections'][currRowID]['middle_point'] = {
                                'latitude': middle_lat,
                                'longitude': middle_lon,
                                'total_distance_km': total_distance,
                                'cumulative_distances': cumulative_distances
                            }

                #
                # Update or add this to the fmt_track_sections table as required
                #
                if currRowIDStr in knownSections:
                    knownSectionDetails = knownSections[currRowIDStr]
                    
                    # Get middle point for this section if available
                    section_center = None
                    if currRowID in trackDetails['track_sections'] and 'middle_point' in trackDetails['track_sections'][currRowID]:
                        middle_point = trackDetails['track_sections'][currRowID]['middle_point']
                        section_center = f"{middle_point['latitude']},{middle_point['longitude']}"
                    
                    # Check if any field needs updating (including section_center)
                    current_section_center = knownSectionDetails.get('section_center', None)
                    if  (knownSectionDetails['bearing_to_britomart'] != bearingInt) or \
                        (knownSectionDetails['title'] != currRow['title']) or \
                        (knownSectionDetails['type'] != currRow['type']) or \
                        (current_section_center != section_center):
                        eventMsg =  '\nChange found for row ' + str(currRowIDStr) + ' = ' + str(knownSectionDetails) + '\n' + \
                                    'currRow[\'bearing_to_britomart\'] = ' + str(bearingInt) + '\n' + \
                                    'currRow[\'title\'] = ' + str(currRow['title']) + '\n' + \
                                    'currRow[\'type\'] = ' + str(currRow['type']) + '\n' + \
                                    'section_center = ' + str(section_center) + '\n' + \
                                    'current_section_center = ' + str(current_section_center) + '\n'
                        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                        try:
                            
                            updateQuery = ''' UPDATE fmt_track_sections SET title = %s, type = %s, bearing_to_britomart = %s, section_center = %s
                                            WHERE id = %s'''
                            updateValues = (currRow['title'],
                                            currRow['type'],
                                            bearingInt,
                                            section_center,
                                            int(currRowIDStr),
                                            )
                            cursorUpdateSectionDetails.execute(updateQuery, updateValues)
                            DBConnection.commit()
                        except mysql.connector.Error as err:
                            eventMsg = str(err)
                            eventLogger('error', eventMsg, 'Error updating section details, in \'fmt_track_sections\'', str(inspect.currentframe().f_lineno))

                else:
                    eventMsg =  'NOT FOUND section \'' + currRowIDStr + '\''
                    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                    try:
                        # Get middle point for this section if available
                        section_center = None
                        if currRowID in trackDetails['track_sections'] and 'middle_point' in trackDetails['track_sections'][currRowID]:
                            middle_point = trackDetails['track_sections'][currRowID]['middle_point']
                            section_center = f"{middle_point['latitude']},{middle_point['longitude']}"
                        
                        insertQuery = ''' INSERT INTO fmt_track_sections 
                                        (id,
                                        title,
                                        type,
                                        bearing_to_britomart,
                                        section_center
                                        ) 
                                        VALUES ( %s, %s, %s, %s, %s )'''
                        insertValues = (currRowID,
                                        currRow['title'],
                                        currRow['type'],
                                        bearingInt,
                                        section_center,
                                        )
                        cursorUpdateSectionDetails.execute(insertQuery, insertValues)
                        DBConnection.commit()
                    except mysql.connector.Error as err:
                        eventMsg = str(err)
                        eventLogger('error', eventMsg, 'Error updating section details, in table \'fmt_track_sections\'', str(inspect.currentframe().f_lineno))

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
            eventMsg = 'Something is wrong, the width or height of the map is zero.' + '\n\n' + \
                       'minLatitude: ' + str(minLatitude) + '\n\n' + \
                       'maxLatitude: ' + str(maxLatitude) + '\n\n' + \
                       'minLongitude: ' + str(minLongitude) + '\n\n' + \
                       'maxLongitude: ' + str(maxLongitude)
            eventLogger('error', eventMsg, 'Error with input details for \'' + trackDetailsFilename + '\'.', str(inspect.currentframe().f_lineno))

        #
        # Create the image
        #

        # First legend stuff
        legendRowHeight = legendTextMaxHeight + legendRowSpace
        legendColumnWidth = legendTextMaxWidth + (legendBoxWidth + legendBoxMargin + legendRightMargin)
        legendRowsPerColumn = math.floor(mapHeightPoints/(legendTextMaxHeight + legendRowSpace))
        legendColumnCnt = math.ceil(sectionCnt/legendRowsPerColumn)
        legendTotalWidth = (legendColumnCnt*legendColumnWidth) + primaryMarginSize
                
        imgTotalWidth = mapWidthPoints + (primaryMarginSize*2) + legendTotalWidth
        imgFullHeight = mapHeightPointsFull
        trackMap = Image.new('RGB', (imgTotalWidth,imgFullHeight), ImageColor.getrgb("#ffffff"))
        trackMapContext = ImageDraw.Draw(trackMap)

        eventMsg =  '\n' + \
                    'Map properties \n' + \
                    '============== \n' + \
                    'longitudeKmPerDegree     : ' + str(longitudeKmPerDegree) + '\n' + \
                    'latitudeKmPerDegree      : ' + str(latitudeKmPerDegree) + '\n' + \
                    'mapWidthKm               : ' + f'{mapWidthKm:f}' + '\n' + \
                    'mapPointSizeKm           : ' + f'{mapPointSizeKm:f}' + '\n' + \
                    'map width degrees        : ' + str((trackDetails['maxLongitude'] - trackDetails['minLongitude'])) + '\n' + \
                    'mapHeightKm              : ' + str(mapHeightKm) + '\n' + \
                    'heightDegreesPerMapPoint : ' + str(heightDegreesPerMapPoint) + '\n' + \
                    'mapHeightPoints          : ' + str(mapHeightPoints) + '\n' + \
                    'legendRowsPerColumn      : ' + str(legendRowsPerColumn) + '\n' + \
                    'legendColumnCnt          : ' + str(legendColumnCnt) + '\n' + \
                    'imgTotalWidth            : ' + str(imgTotalWidth) + '\n' + \
                    'imgFullHeight            : ' + str(imgFullHeight) + '\n' + \
                    '\n'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

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
            currSectionPoints = []
            for currPoints in range(1, len(trackDetails['track_sections'][currSection]['section_points']) + 1):
                currLatitude = trackDetails['track_sections'][currSection]['section_points'][currPoints]['latitude']
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

            trackMapContext.rectangle((((primaryMarginSize + xPosOffset), (primaryMarginSize + yPosOffset + legendBoxHeightOffset)),
                                        ((primaryMarginSize+ xPosOffset + legendBoxWidth),(primaryMarginSize + yPosOffset + legendBoxHeightOffset + legendBoxWidth))), 
                                        fill=ImageColor.getrgb(sectionColor), outline='black', width=1)
        trackMap.save(trackMapImgFilename)

        return trackMap
    
    def saveConfigsToDB():

        #
        # The "fmt_config" table has only one row, so the simplest approach
        # is to delete it each time and recreate.
        # 
        cursorConfigs = DBConnection.cursor(dictionary=True)
        try:                  
            # Wipe configs          
            updateQuery = ''' delete from fmt_config'''
            cursorConfigs.execute(updateQuery)

            # Add current configs
            updateQuery = ''' INSERT INTO fmt_config
                                (
                                    default_train,
                                    default_location
                                )
                                VALUES ( %s, %s )'''
            insertValues = (defaultTrainNumber, defaultLocation)
            cursorConfigs.execute(updateQuery, insertValues)


            DBConnection.commit()
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error updating config details, in \'fmt_config\'', str(inspect.currentframe().f_lineno))

        
    
    ###################################
    #
    # Starting core functions for this script
    #
    ###################################
    nextEventID = getLatestEventID() + 2
    scriptStartTime = datetime.now()
    scriptMaxFinishTime = scriptStartTime + timedelta(minutes=totalScriptTimeMin)
    eventMsg =  'Beginning core functions of script starting with routeDetails' + '\n' + \
                'Time started: ' + str(scriptStartTime)
    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

    # Save configs to the database
    eventMsg =  'Saving configurations to the Database'
    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
    saveConfigsToDB() 

    # Load route and special train details
    # Using API-based route loading instead of routes.csv
    routeDetails = loadTrainRoutesFromAPI()
    
    loadSpecialTrainDetails()

    # Ensure the 'Out of service' record exists in the fmt_trips table
    ensureOOSTripRecordExists()

    # Draw the map image
    eventMsg =  'Creating the map of the train tracks in Auckland'
    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
    mapContext = drawMap() 

    # Import stations from CSV file
    importStationsFromCSV()


    #########################################################
    #########################################################
    # 
    #                  Main loop cycle
    #                  ---------------
    #
    # Loop approximately every 30 seconds ('freqApiCallsSec')
    #
    #########################################################
    #########################################################
    eventMsg =  'Finished all initialization, including loading routes and trains plus drawing the map.' + '\n' + \
                'Starting a cycle of api calls at: ' + str(scriptStartTime)
    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
    

    lastApiCallStartTime = datetime.now()
    lastStopDetailsRefresh = datetime(1, 1, 1, 0, 0)
    while (datetime.now() + timedelta(seconds=(freqApiCallsSec + scriptBufferTimeSec))) < scriptMaxFinishTime:

        nextEventID = getLatestEventID() + 2

        # 
        # Collect details about stop
        # This only changes maybe every few days so only run this periodically based on 'refreshStopDetailsSec'
        #
        if (lastStopDetailsRefresh + timedelta(seconds=refreshStopDetailsSec)) < datetime.now():
            getStopDetails()
            lastStopDetailsRefresh = datetime.now()


        #
        # Perform api call
        #
        lastApiCallStartTime = datetime.now()
        eventMsg =  'Api cycle started at ' + str(lastApiCallStartTime) 
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

        # The actual api calls
        rawTrainDetails = {'train':{}}
        apiTimestampPosix = 0
        trainDetails = {
                            'train':{},
                            'section':{},
                        }
        getCurrVehicleDetails()
        findTrainSets()
        

        ###############
        #
        # Update various columns fmt_train_details with information we have collected in this cycle, such as position_history
        # We need to do this before updateTripStopDetails() as that function relies on the position_history being up to date    
        #
        ###############
        cursorUpdateTrainDetails = DBConnection.cursor(dictionary=True)        

        # Load all details from fmt_train_sets
        cursorAllTrainSets = DBConnection.cursor(dictionary=True)
        print('= 3304 load fmt_train_sets')
        sqlQuery = 'SELECT * FROM fmt_train_sets'
        try:
            cursorAllTrainSets.execute(sqlQuery)
            allDBTrainSets = cursorAllTrainSets.fetchall() 
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error all details from table \'fmt_train_sets\'.', str(inspect.currentframe().f_lineno))
        allTrainSetsFromDB = {} 
        for currTrainSet in allDBTrainSets:
            allTrainSetsFromDB.update({currTrainSet['train_set']:currTrainSet})      

        # Load all details from fmt_train_details
        cursorOrigTrainDetails = DBConnection.cursor(dictionary=True)
        sqlQuery = 'SELECT * FROM fmt_train_details'
        try:
            cursorOrigTrainDetails.execute(sqlQuery)
            originalTrainDetailsRows = cursorOrigTrainDetails.fetchall() # We need to cache the rows as we are using the same connection to do do an update
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error querying train_number from table \'fmt_train_details\'.', str(inspect.currentframe().f_lineno))
            originalTrainDetailsRows = []

        #
        # Loop through all trains that are listed in fmt_train_details using originalTrainDetailsRows, so effectively all known trains
        # Remembering we updated this table with the latest api details at the start of this cycle
        #
        for currOrigTrain in originalTrainDetailsRows:
            currTrainNo = currOrigTrain['train_number']
            dbUpdate_train_set = str(currOrigTrain['train_number'])
            if currTrainNo in fullUp2DateTrainLocations:
                dbUpdate_position_history = json.dumps(fullUp2DateTrainLocations[currTrainNo])

                # Work out which trains are connected to this based on the rules in trainSetCriteria{}
                # Look at the most recent train sets and count how many times each train appears
                if 'previous_train_sets' in fullUp2DateTrainLocations[currTrainNo]:
                    previousSets = fullUp2DateTrainLocations[currTrainNo]['previous_train_sets']
                    
                    if len(previousSets) > 0:
                        # Count how many times each train appears in the most recent sets
                        trainCounts = {}
                        setsToConsider = min(trainSetCriteria['no_prev_sets_to_consider'], len(previousSets))
                        
                        #
                        # get a list of train numbers and how many times they are in train sets
                        # This will produce something that looks like:
                        # trainCounts = {'917': 6, '1059': 6, '484': 1, '659': 1}
                        #
                        for i in range(setsToConsider):
                            for trainNum in previousSets[i]:
                                if trainNum != currTrainNo:  # Don't count the train itself
                                    trainCounts[trainNum] = trainCounts.get(trainNum, 0) + 1
                        
                        # Find trains that meet the minimum qualification threshold
                        connectedTrains = []
                        for train, count in trainCounts.items():
                            if count >= trainSetCriteria['min_no_sets_to_qualify']:
                                connectedTrains.append(train)
                        
                        if connectedTrains:
                            # Sort for consistent ordering and include current train
                            connectedTrains.sort()
                            allTrainsInSet = [currTrainNo] + connectedTrains
                            allTrainsInSet.sort()
                            # Format as a comma-separated string
                            dbUpdate_train_set = ','.join(sorted(allTrainsInSet, key=int))
                        else:
                            # No trains meet the criteria, just the current train
                            dbUpdate_train_set = str(currTrainNo)
                    else:
                        # No previous sets, just the current train
                        dbUpdate_train_set = str(currTrainNo)
                else:
                    # No previous_train_sets key, just the current train
                    dbUpdate_train_set = str(currTrainNo)
            else:
                # This should never happen.
                dbUpdate_position_history = currOrigTrain['position_history']

            if currTrainNo in trainDetails['train'] and 'section' in trainDetails['train'][currTrainNo] and 'id' in trainDetails['train'][currTrainNo]['section']:
                dbUpdate_section_id = trainDetails['train'][currTrainNo]['section']['id']
            else:
                # Train is not active, preserve existing section_id from database
                dbUpdate_section_id = currOrigTrain['section_id']

            #
            # Trains can be turned on in places like stabling yards even though they aren't really part of a train set
            # Yes they might be coupled to another train but that's not an active moving train set
            #
            # We will clean these up based on if they are in one of the sectionTypesToIgnoreForTrainSets section types
            # AND if they've been stationary there for more than parkedTrainInactivityMin minutes
            #
            # Only perform this check if the train has been in a train set before (both values must be set)
            lastTimeCurrTrainInTrainSet = fullUp2DateTrainLocations[currTrainNo].get('last_time_in_train_set')
            lastTimeCurrTrainInTrainSetStr = fullUp2DateTrainLocations[currTrainNo].get('last_time_in_train_set_str')
            current_time = int(time.time())
            if lastTimeCurrTrainInTrainSet is not None and lastTimeCurrTrainInTrainSetStr is not None:
                howLongsinceLastTimeInTrainSetMin = (current_time - lastTimeCurrTrainInTrainSet) / 60.0
                if currTrainNo in trainDetails['train'] and 'section' in trainDetails['train'][currTrainNo]:
                    currSection = trainDetails['train'][currTrainNo]['section']
                    if currSection is not None and isinstance(currSection, dict):
                        sectionType = currSection.get('type')
                        if sectionType in sectionTypesToIgnoreForTrainSets:
                            # Train is in an ignored section type, check if it's been stationary for a while
                            if howLongsinceLastTimeInTrainSetMin > parkedTrainInactivityMin:
                                
                                # Clear previous_train_sets history
                                if 'previous_train_sets' in fullUp2DateTrainLocations[currTrainNo]:
                                    fullUp2DateTrainLocations[currTrainNo]['previous_train_sets'] = []
                                # Set train_set to just this train
                                dbUpdate_train_set = str(currTrainNo)

            # Handle trip_id - check if current train has trip_id, if not check other trains in the set
            dbUpdate_trip_id = None
            trip_id_source_train = None
            
            # First check if current train has trip_id
            if currTrainNo in trainDetails['train'] and 'vehicle' in trainDetails['train'][currTrainNo] and 'trip' in trainDetails['train'][currTrainNo]['vehicle'] and 'trip_id' in trainDetails['train'][currTrainNo]['vehicle']['trip']:
                dbUpdate_trip_id = trainDetails['train'][currTrainNo]['vehicle']['trip']['trip_id']
                trip_id_source_train = currTrainNo
            else:
                # Current train doesn't have trip_id, check other trains in the set
                # Parse dbUpdate_train_set to get list of trains (it's a comma-separated string)
                trains_in_set = []
                for train in dbUpdate_train_set.split(','):
                    trains_in_set.append(train.strip())
                
                for other_train in trains_in_set:
                    if other_train != currTrainNo:
                        # Check if this other train has a trip_id
                        if other_train in trainDetails['train'] and 'vehicle' in trainDetails['train'][other_train] and 'trip' in trainDetails['train'][other_train]['vehicle'] and 'trip_id' in trainDetails['train'][other_train]['vehicle']['trip']:
                            dbUpdate_trip_id = trainDetails['train'][other_train]['vehicle']['trip']['trip_id']
                            trip_id_source_train = other_train
                            break  # Found one, use it
            
            # If we found a trip_id from another train, append the source train number
            if dbUpdate_trip_id is not None and trip_id_source_train != currTrainNo:
                dbUpdate_trip_id = dbUpdate_trip_id 
            
            # If still no trip_id found then leave it as is
            # This is not a perfect solution as we could have a train that is decoupled from a 6 or was
            # incorrectly assigned to a 6 and got the wrong train number.
            # On the other hand we need to deal with the situation where a train is at an interchange for 10 min
            # or more, such as what happens now in Newmarket. In this situation we need the train to keep the trip id
            if dbUpdate_trip_id is None:
                dbUpdate_trip_id = currOrigTrain['trip_id']

            # Handle last_updated - update timestamp for active trains, preserve for inactive
            if currTrainNo in trainDetails['train'] and 'vehicle' in trainDetails['train'][currTrainNo] and 'timestamp' in trainDetails['train'][currTrainNo]['vehicle']:
                dbUpdate_last_updated = posixtoDateTime(trainDetails['train'][currTrainNo]['vehicle']['timestamp'])
            else:
                # Train is not active, preserve existing last_updated from database
                dbUpdate_last_updated = currOrigTrain['last_updated']

            #
            # We need a way to reset trains that are perhaps sitting at a station a long time
            # and so the speed and bearing are zero and thus we don't recalculate if the train is
            # in a train set.
            # 
            # We need a fairly simple way to clean up.
            #
            # If a train's most recent 'history' record is more than 'parkedTrainInactivityMin' minutes ago then
            # we need to review the column 'train_set' and go through each train in the 'train_set' and if
            # any of the trains in the list are more than 'maxMetersBetweenTrainsInASet' meters away
            # then remove that train from the train set.
            #
            mostRecentHistoryRecord = 0
            for currPositionRec in fullUp2DateTrainLocations[currTrainNo]['history'].keys():  
                if int(currPositionRec) > mostRecentHistoryRecord:
                    mostRecentHistoryRecord = int(currPositionRec)
            
            # 
            # Check if train has been inactive (parked) for more than parkedTrainInactivityMin
            #            
            minutes_since_last_update = (current_time - mostRecentHistoryRecord) / 60.0            
            if minutes_since_last_update > parkedTrainInactivityMin:
                # Clear this train's previous_train_sets history since it's been parked too long
                if 'previous_train_sets' in fullUp2DateTrainLocations[currTrainNo]:
                    fullUp2DateTrainLocations[currTrainNo]['previous_train_sets'] = []
                # Since we cleared the history, update dbUpdate_train_set to just this train
                dbUpdate_train_set = str(currTrainNo)

            # Update the train set using the train_set_display value from fmt_train_sets if one exists, 
            # otherwise use the calculated train set
            if dbUpdate_train_set in allTrainSetsFromDB:
                dbUpdate_train_set_display = allTrainSetsFromDB[dbUpdate_train_set]['train_set_display']
            else:   
                dbUpdate_train_set_display = dbUpdate_train_set

            # Update DB
            try:
                updateQuery = '''UPDATE
                                    fmt_train_details 
                                SET 
                                    position_history = %s,
                                    section_id = %s,
                                    train_set = %s,
                                    trip_id = %s,
                                    last_updated = %s,
                                    train_set_display = %s  
                                WHERE 
                                    train_number = %s'''
                updateValues = (
                                dbUpdate_position_history, 
                                dbUpdate_section_id,   
                                dbUpdate_train_set,
                                dbUpdate_trip_id,
                                dbUpdate_last_updated,
                                dbUpdate_train_set_display,
                                currOrigTrain['train_number']
                                )
                cursorUpdateTrainDetails.execute(updateQuery, updateValues)
                DBConnection.commit()
            except mysql.connector.Error as err:
                eventMsg = str(err)
                eventLogger('error', eventMsg, f'Error updating fmt_train_details for train ' + str(currOrigTrain['train_number']) , str(inspect.currentframe().f_lineno))
            except (TypeError, ValueError) as err:
                eventMsg = str(err)
                eventLogger('error', eventMsg, f'Error serializing fmt_train_details  for train ' + str(currOrigTrain['train_number']) , str(inspect.currentframe().f_lineno))

        #
        # Need to clean up "trip_id" column in fmt_train_details
        #
        # It seems sometimes trains will not get switched to out of service correctly with "trip_id" 
        # column still retaining old trip_id GUIDs
        #
        # The approach now taken is to work out when a trip is scheduled to get to the last station, 'trip_end_sec_past_midnight', then add the current delay 'trip_delay'
        # 


        # 
        eventMsg = 'Cleaning up Out Of Service trains in \'fmt_train_details\''
        eventLogger('info', eventMsg, 'Updating \'fmt_train_details\' for Out Of Service trains', str(inspect.currentframe().f_lineno))
        tripsUpdateCursor = DBConnection.cursor(dictionary=True)

        sqlQuery = '''  UPDATE  
                            fmt_train_details ftd
                        SET
                            ftd.trip_id = "oos"
                        WHERE ftd.trip_id IN 
                            (
                                SELECT 
                                    trip_id
                                FROM 
                                    fmt_trips ft
                                WHERE 
                                    DATE_ADD(TIMESTAMP(CURDATE()) , INTERVAL (ft.trip_end_sec_past_midnight + ft.trip_delay) SECOND ) < now() - INTERVAL ''' + str(endOfTripTimeoutMin) + ''' MINUTE
                            )
                        ;'''

        try:
            tripsUpdateCursor.execute(sqlQuery)
            rowsUpdated = tripsUpdateCursor.rowcount
            DBConnection.commit()
            eventMsg = 'Out Of Service cleanup of table \'fmt_train_details\' updated ' + str(rowsUpdated) + ' row(s) in \"fmt_train_details\".'
            eventLogger('info', eventMsg, 'Out Of Service cleanup result', str(inspect.currentframe().f_lineno))
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error cleaning up Out Of Service trains in table \'fmt_train_details\'.', str(inspect.currentframe().f_lineno))
            exit(1)


        updateTripStopDetails()
        postUpdateTasks()
        
        currApiCallEndTime = datetime.now()
        eventMsg =  '\n\n\nApi cycle finished at ' + str(currApiCallEndTime) + ', and it took ' + str((currApiCallEndTime - lastApiCallStartTime).total_seconds()) + ' seconds.'
        eventMsg += '\n\n\n##### >>>---------------------------------<<<< #####\n\n\n'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
        eventLogger('info_close', '', '', str(inspect.currentframe().f_lineno))
   
        #
        # Check if we have time to do another api cycle
        #
        if (datetime.now() + timedelta(seconds=(freqApiCallsSec + scriptBufferTimeSec))) <= scriptMaxFinishTime:
            #
            # Doing another cycle but first need to sleep so the total time = freqApiCallsSec
            #
            nextApiCall = lastApiCallStartTime + timedelta(seconds=(freqApiCallsSec))
            sleepSec = (nextApiCall - datetime.now()).total_seconds()

            if sleepSec < 0:
                sleepSec = 0
                
            eventMsg =  'sleepSec = ' + str(sleepSec)
            eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

            time.sleep(sleepSec)


    #
    # Close things off
    #
    DBConnection.close()

#
# This block captures the full script for errors,
# so it should trap any unexpected errors
#
except Exception as e:
    eventMsg = traceback.format_exc()
    eventTitle = str(e)
    eventLogger('error', eventMsg, eventTitle, str(inspect.currentframe().f_lineno))

