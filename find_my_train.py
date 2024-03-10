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
# Crontab
# If we set this as "*/10 * * * *", then it will fire every tenth minute, so not 
# from now but at say 6:00, 6:10, 6:20, and so on
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
from requests.exceptions import ConnectionError
from PIL import Image, ImageDraw, ImageColor, ImageFont
from haversine import haversine, Unit                       # Used to work out meters to latitude/longitude
import mysql.connector 

#
# User properties
#
secretsConfFilename = os.path.dirname(os.getcwd()) + '/find_my_train.ini'
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
maxSearchRadius = 30   # was 5
stdSearchRadius = 5
maxTimestampDiffBetweenMultiTrainsSec = 90
timeZoneStr = 'Pacific/Auckland'
timeRetainMostRecentDataMinutes = 60  
refreshStopDetailsSec = 100

# Info retention period for a train that is/was part of 6 carridge train. 
# Period measured in number of track sections  
multiTrainDetailsMaxRetentionCount = 5  

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
nextEventID = -1
apiTimestampPosix = 0
rawTrainDetails = {'train':{}}
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
logInfoMsg = ''
lastApiCallStartTime = None

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

        #print(errorMessage)


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
    atVehiclePosURL = 'https://api.at.govt.nz/realtime/legacy/vehiclelocations'
    atAllStopsURL = 'https://api.at.govt.nz/gtfs/v3/stops'
    tripUpdatesURL = 'https://api.at.govt.nz/realtime/legacy/'

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


    # Check the required files exist
    for filePath in [trackDetailsFilename, specialTrainsFilename, legendFontFilename, trainRoutesFilename]:
        if not os.path.isfile(filePath):
            eventMsg = 'The file \'' + filePath + '\' was expected but not found.'
            eventLogger('error', eventMsg, 'A required file is missing', str(inspect.currentframe().f_lineno))

    #
    # Derived properties
    #
    primaryMarginSize = int((mapWidthPoints*imgMarginPercent)/100)
    lineWidthPt = int((mapWidthPoints*lineWidthPercent)/100)

    #
    # Convert time string to seconds past midnight
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

        outOfServiceRouteID = int(routeDetails['at_route_id']['oos']['route_id'])
        #print('outOfServiceRouteID: ' + str(outOfServiceRouteID))
        #print('- Type: ' + str(type(outOfServiceRouteID)))

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

        # Update trip details
        for currTrain in currentDBTrainDetails:
            #print('Set: ' + currentDBTrainDetails[currTrain]['most_recent_list_connected_trains'])

            # Check all trains connected to this train to see if they have a trip_id
            currWholeTrainTripID = ''
            for trainInSetRaw in currentDBTrainDetails[currTrain]['most_recent_list_connected_trains'].lower().split(' and '):
                currMultiTrainNo = trainInSetRaw.strip()[3:]
                #print('- \'' + currMultiTrainNo + '\'')
                if currentDBTrainDetails[currMultiTrainNo]['trip_id'] != '':
                    currWholeTrainTripID = currentDBTrainDetails[currMultiTrainNo]['trip_id']
                    #print('   - \'' + currWholeTrainTripID + '\'')

            #
            # If the current multitrain doesn't have trip_id set for any of the sub-trains then
            # it must be an out of service
            #
            # Obviously only update if it's value isn't already out of service
            #
            if currWholeTrainTripID == '' :
                if currentDBTrainDetails[currTrain]['most_recent_route_id'] != outOfServiceRouteID:
                    #
                    # If we get here it means none of the trains connected to this train have a trip_id. This means
                    # this train is out of service.
                    #
                    # Additionally this train is not currently flagged as out of service
                    #
                    # Thus update the DB to show it is out of service
                    #
                    eventMsg = 'Updating \'fmt_train_details\' for Out Of Service train ' + str(currTrain)
                    eventLogger('info', eventMsg, 'Updating \'fmt_train_details\' for Out Of Service train ' + str(currTrain), str(inspect.currentframe().f_lineno))
                    try:
                        updateQuery = '''UPDATE fmt_train_details 
                                         SET 
                                            most_recent_route_id = %s
                                         WHERE train_number = %s'''
                        updateValues = (outOfServiceRouteID,
                                        currTrain
                                        )
                        cursorTrainDetails.execute(updateQuery, updateValues)
                        DBConnection.commit()
                    except mysql.connector.Error as err:
                        eventMsg = 'Error updating route_id for Out Of Service trains in table \'fmt_train_details\'.'  + '\n' + \
                                    str(err)
                        eventLogger('error', eventMsg, 'Error updating route_id for Out Of Service trains in table \'fmt_train_details\'', str(inspect.currentframe().f_lineno))
            #
            # We need to ensure that the 'whole_train_trip_id' is correct for this train
            #
            if currWholeTrainTripID != currentDBTrainDetails[currTrain]['whole_train_trip_id']:
                # If it's not correct then update
                eventMsg = 'Updating \'fmt_train_details\' column \'whole_train_trip_id\', has changed for train ' + str(currTrain)
                eventLogger('info', eventMsg, 'Updating \'fmt_train_details\' column \'whole_train_trip_id\' ' + str(currTrain), str(inspect.currentframe().f_lineno))
                try:
                    updateQuery = '''UPDATE fmt_train_details 
                                        SET 
                                        whole_train_trip_id = %s
                                        WHERE train_number = %s'''
                    updateValues = (currWholeTrainTripID,
                                    currTrain
                                    )
                    cursorTrainDetails.execute(updateQuery, updateValues)
                    DBConnection.commit()
                except mysql.connector.Error as err:
                    eventMsg = 'Error Updating \'fmt_train_details\' column \'whole_train_trip_id\'.'  + '\n' + \
                                str(err)
                    eventLogger('error', eventMsg, 'Error updating whole_train_trip_id in table \'fmt_train_details\'', str(inspect.currentframe().f_lineno))

        ################
        #
        # We need to work out the current delay for each trip
        #
                    
        # First step get a list of all active trips
        sqlQuery = 'SELECT DISTINCT whole_train_trip_id FROM fmt_train_details ftd WHERE whole_train_trip_id != \"\" '
        try:
            cursorTrainDetails.execute(sqlQuery)
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error querying database table \'fmt_train_details\' to get list of all active train trips.', str(inspect.currentframe().f_lineno))

        activeTripIDs = []
        for currTrip in cursorTrainDetails:            
            activeTripIDs.append(currTrip['whole_train_trip_id'])

        # Get all trip updates
        tripUpdatesResponse = apiRequest(tripUpdatesURL, True, 2, 'Trip updates')
        eventMsg = 'Updating \'fmt_trips\' details... ' + str(currTrain)
        eventLogger('info', eventMsg, 'Updating \'fmt_trips\' details... ' + str(currTrain), str(inspect.currentframe().f_lineno))
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
                            eventLogger('error', eventMsg, 'Error updating train \'trip_delay\' in database table \'fmt_train_details\'.', str(inspect.currentframe().f_lineno))


        



    #
    # Call an AT api
    #
    def apiRequest(requestURL, failOnError, requestId, requestDesc):
                
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

                    # Get the stop times for the trip by calling an API
                    stopTimesURL = 'https://api.at.govt.nz/gtfs/v3/trips/' + str(currTripId) + '/stoptimes'
                    stopTimesDetail = apiRequest(stopTimesURL, True, 1, 'Stop times')

                    #
                    # Create a string with stop details
                    #
                    # This is a semicolon delimited lists of stops with the stop details being comma separated
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
                        departTimePosixInt = timestrToSeconds(departTimeStr)

                        currTripStopDetailsJson.update({ stopNumber:{
                                                            'stop_name':stopName.replace(',','').replace(';',''),       # Note we want to remove commas and semi colons from names
                                                            'arrival_time_str':departTimeStr,'depart_time_posix_int':departTimePosixInt
                                                        }})
                        
                    #
                    # Create the output strings
                    #
                    currStopDetailsStr = ''
                    for currStop in sorted(list(currTripStopDetailsJson)):
                        if currStopDetailsStr != '':
                            currStopDetailsStr += ';'
                        currStopDetailsStr += str(currStop) + ',' + currTripStopDetailsJson[currStop]['stop_name'] + ',' + str(currTripStopDetailsJson[currStop]['depart_time_posix_int']) + \
                                            ',' + currTripStopDetailsJson[currStop]['arrival_time_str']                  

                    #
                    # Insert the record into the DB
                    #
                    try:
                        insertQuery = ''' INSERT INTO fmt_trips
                                        (trip_id,
                                        stop_details_str
                                        )
                                        VALUES ( %s, %s)'''
                        insertValues = (currTripId,
                                        currStopDetailsStr,
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
        apiTimestampPosix = apiRequest(atAllStopsURL, True, 3, 'Stop details')['data'] 
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
    # Perform additional train calculations
    #
    # This performs additional algorithms to work out extra details
    # which were not immediately obvious
    #
    def additionalCalculations(routeDetails):

        eventMsg = 'Running additionalCalculations()'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

        global trainDetails

        #
        # Get current train details from the DB
        #
        cursorTrainDetails = DBConnection.cursor(dictionary=True)
        sqlQuery = 'select * from fmt_train_details'
        try:
            cursorTrainDetails.execute(sqlQuery)
        except mysql.connector.Error as err:
            eventMsg = str(err)
            eventLogger('error', eventMsg, 'Error querying database table \'fmt_train_details\'.', str(inspect.currentframe().f_lineno))

        currentDBTrainDetails = {}
        for currDBTrain in cursorTrainDetails:
            
            currentDBTrainDetails.update({currDBTrain['train_number']:currDBTrain})

        #
        # Work out if trains are part of a multi-train
        #
        # Imagine a train is at a normal station, which is in the table 'fmt_track_sections' and has
        # a value of 'S' for 'type'.
        #
        # IF we have two trains like this at a normal station and they are both going in the same
        # direction, as in to or from Britomart, then we will assume they both for a 6 car train.
        #
        # This doesn't work at an 'Interchange', type = 'I' (eg. Britomart), or stations at the end of 
        # the line, type = 'E', or yards, type = 'Y'. In these places there could be multiple
        # trains going the same direction but not in the same service and not part of a 6 car. The same
        # is true for yards.
        #
        for currSection in trainDetails['section']:
            sectionType = trainDetails['section'][currSection]['detail']['type']

            eventMsg = 'Section \'' + str(currSection) + '\'' + '\n' + \
                        '- sectionType \'' + str(sectionType) + '\'' + '\n' + \
                        '- Number of trains \'' + str(len(trainDetails['section'][currSection]['trains'])) 
            eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

            #
            # Because trains are not grouped by the direction they are travelling we have to
            # step through all the train
            #  
            # Obviously it's not worth doing this if there is only 1 train at this station
            #
            if (sectionType == 'S') and (len(trainDetails['section'][currSection]['trains']) > 1):

                #
                # Look at the trains going in each direction - to or from Britomart
                #
                # If there are two, or more, trains going in the same direction then this must
                # be a multi-train, ie. a 6 carridge train.
                #
                for goingToBritomart in ('Y', 'N'):

                    eventMsg = 'goingToBritomart = \'' + str(goingToBritomart) + '\''
                    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

                    #
                    # If neither train in a 6 carridge train has 'trip' details, then sectionTrainRouteID will remain
                    # at the default value set below
                    #
                    # If neither train in a set of 6 has 'trip' details then it is reasonable to assume it is an
                    # 'out of service' train.
                    #
                    sectionTrainRouteID = routeDetails['at_route_id']['oos']['route_id']
                    multitrainListConnectedTrains = []
                    multitrainListConnectedTrainsStr = ''
                    earliestTimestamp = latestTimestamp = 0

                    # Go through all trains at this section
                    for sectionTrain in trainDetails['section'][currSection]['trains']:

                        eventMsg = 'sectionTrain = \'' + str(sectionTrain) + '\'' 
                        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                        
                        if trainDetails['section'][currSection]['trains'][sectionTrain]['heading_to_britomart'] == goingToBritomart:

                            trainDetails['train'][sectionTrain].update({'currently_part_of_multi-train':False})

                            #
                            # For the trains in this section, going the same way, we need to find the
                            # time difference between the earliest and latest timestamps for this group of trains
                            #
                            currTimestamp = trainDetails['section'][currSection]['trains'][sectionTrain]['vehicle']['timestamp']

                            eventMsg = '- currTimestamp = \'' + str(currTimestamp) + '\'' 
                            eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

                            if earliestTimestamp == 0:
                                earliestTimestamp = latestTimestamp = currTimestamp
                            else:
                                if currTimestamp < earliestTimestamp:
                                    earliestTimestamp = currTimestamp
                                if currTimestamp > latestTimestamp:
                                    latestTimestamp = currTimestamp
                            eventMsg = '- earliestTimestamp = \'' + str(earliestTimestamp) + '\'' + '\n' + \
                                        '- latestTimestamp = \'' + str(latestTimestamp) + '\''
                            eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                            
                            #
                            # Get a list of the currently connected trains both as a 'list' and a string
                            #
                            trainFriendlyName = trainDetails['section'][currSection]['trains'][sectionTrain]['friendly_name']
                            multitrainListConnectedTrains.append(sectionTrain)
                            if multitrainListConnectedTrainsStr == '':
                                multitrainListConnectedTrainsStr += trainFriendlyName
                            else:
                                multitrainListConnectedTrainsStr += ' and ' + trainFriendlyName

                            eventMsg = 'multitrainListConnectedTrains = ' + str(multitrainListConnectedTrains)
                            eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

                            # 
                            # For a multi-train set there will only be one train with 'trip' details.
                            #
                            # If this train is the one with 'trip' details then record the routeID value - 'sectionTrainRouteID'
                            #
                            if 'trip' in trainDetails['section'][currSection]['trains'][sectionTrain]['vehicle']:
                                ATRouteID = trainDetails['section'][currSection]['trains'][sectionTrain]['vehicle']['trip']['route_id']
                                if ATRouteID in routeDetails['at_route_id']:
                                    sectionTrainRouteID = routeDetails['at_route_id'][ATRouteID]['route_id']

                                    eventMsg = 'Train route found, sectionTrainRouteID = ' + str(sectionTrainRouteID)
                                    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                                else:
                                    #
                                    # This is a new train route we don't know about
                                    #
                                    eventMsg = 'Halting script as route \'' + ATRouteID + '\' is not defined in the route csv file.'
                                    eventLogger('error', eventMsg, 'The AT train route of \'' + ATRouteID + '\' is unknown', str(inspect.currentframe().f_lineno))

                    
                    if (latestTimestamp - earliestTimestamp) >= maxTimestampDiffBetweenMultiTrainsSec:
                        eventMsg = 'Maximum time between timestamps is too large at ' + str(latestTimestamp - earliestTimestamp) + ' seconds.'
                        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                    #
                    # If 
                    # - 'len(multitrainListConnectedTrains) > 1' then this set of trains is:
                    #       - More than 2 trains going in the same direction where that direction is 'Y' or 'N' to Britomart, aka the value 'goingToBritomart'
                    # - '(latestTimestamp - earliestTimestamp) < maxTimestampDiffBetweenMultiTrainsSec' then
                    #       - The maximum time between timestamps for this set of trains, is below our defined maximum
                    #
                    # We can assume that this set of 2 or more trains are a set, aka a 6 carridge train
                    #
                    # We thus need to mark each train as being part of a set
                    #
                    eventMsg = 'Checking if set is valid 6 train' + '\n' + \
                                '- len(multitrainListConnectedTrains) = ' + str(len(multitrainListConnectedTrains)) + '\n' + \
                                '- (latestTimestamp - earliestTimestamp) = ' + str((latestTimestamp - earliestTimestamp)) + '\n' + \
                                '- maxTimestampDiffBetweenMultiTrainsSec = ' + str(maxTimestampDiffBetweenMultiTrainsSec) 
                    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                    if (len(multitrainListConnectedTrains) > 1) and ((latestTimestamp - earliestTimestamp) < maxTimestampDiffBetweenMultiTrainsSec):

                        # Loop though all trains in this set
                        eventMsg = 'multitrainListConnectedTrains = ' + str(multitrainListConnectedTrains)
                        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                        for currConnectedTrain in multitrainListConnectedTrains:
                            eventMsg = 'Updating details for train = ' + str(currConnectedTrain)
                            eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

                            trainDetails['train'][currConnectedTrain].update({'most_recent_route_id':sectionTrainRouteID})
                            trainDetails['train'][currConnectedTrain].update({'most_recent_list_connected_trains':multitrainListConnectedTrainsStr})
                            trainDetails['train'][currConnectedTrain].update({'most_recent_no_connected_trains':len(multitrainListConnectedTrains)})
                            trainDetails['train'][currConnectedTrain].update({'multi_train_most_recent_section':currSection})
                            trainDetails['train'][currConnectedTrain].update({'multi_train_most_recent_section_count':0})
                            trainDetails['train'][currConnectedTrain].update({'currently_part_of_multi-train':True})
                            trainDetails['train'][currConnectedTrain].update({'train_at_britomart_end':'na'})

                            eventMsg = 'Updated value for trainDetails[\'train\'][currConnectedTrain]\n' + json.dumps(trainDetails['train'][currConnectedTrain], indent=4, sort_keys=True, default=str) + '\n' + \
                                        ' - currConnectedTrain = ' + str(currConnectedTrain)
                            eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
        
        #
        # Having identified all the 6 carridge trains, we now need to go through all the trains a second time looking at 
        # trains that aren't part of a 6. We need to do our best to collect the details for these trains
        # 
        for currTrain in  trainDetails['train']:
            # 
            # Check if this train's data is still valid
            # - It could have been marked invalid, for example, if it's location was not found
            #
            if trainDetails['train'][currTrain]['train_data_is_valid']:
                trainFriendlyName = trainDetails['train'][currTrain]['friendly_name']

                #
                # If this train IS NOT part of a 6 carridge
                # 
                eventMsg = 'trainDetails[\'train\'][currTrain][\'currently_part_of_multi-train\'] = ' + str(trainDetails['train'][currTrain]['currently_part_of_multi-train']) + '\n' 
                eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                if not trainDetails['train'][currTrain]['currently_part_of_multi-train']:      

                    #
                    # Does this train have 'trip' details
                    #
                    if 'trip' in trainDetails['train'][currTrain]['vehicle']:
                        # Work out the current route id
                        ATRouteID = trainDetails['train'][currTrain]['vehicle']['trip']['route_id']
                        if ATRouteID in routeDetails['at_route_id']:
                            currTrainRouteID = routeDetails['at_route_id'][ATRouteID]['route_id']
                        else:
                            #
                            # This is a new train route we don't know about
                            #
                            eventMsg = 'Halting script as route \'' + ATRouteID + '\' is not defined in the route csv file.'
                            eventLogger('error', eventMsg, 'The AT train route of \'' + ATRouteID + '\' is unknown', str(inspect.currentframe().f_lineno))

                        #
                        # At this point this train could still be a part of a 6, even though it was not found
                        # as part of a 6 above.
                        #
                        # This could happen for example where one part of a 6 reported as being in one section
                        # and the other half of the 6 reported as part of an adjacent section
                        #
                        eventMsg = 'Train ' + str(currTrain) + ' is not part of a 6' + '\n' + \
                                    'currentDBTrainDetails[currTrain][\'multi_train_most_recent_section_count\' = ]' + str(currentDBTrainDetails[currTrain]['multi_train_most_recent_section_count'])
                        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                        if (currentDBTrainDetails[currTrain]['multi_train_most_recent_section_count'] < multiTrainDetailsMaxRetentionCount):            

                            #
                            # - This train was NOT identified as being in a current 6 for the current api call
                            # - However it was recently part of a 6, ie. "multi_train_most_recent_section_count < multiTrainDetailsMaxRetentionCount"
                            # 
                            # So we should assume it is still part of a 6 although it might now be on a different route
                            # We should change the route id, and increment
                            eventMsg = 'Train ' + str(currTrain) + ' is not part of a 6' + '\n' + \
                                     '==================== FIX CURRENT DEBUG ================ @739'
                            eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                            if currentDBTrainDetails[currTrain]['multi_train_most_recent_section'] == trainDetails['train'][currTrain]['section']['id']:
                            
                                # We are still in the same section so don't change anything from what is currently in the DB
                                multitrainSectionCount = currentDBTrainDetails[currTrain]['multi_train_most_recent_section_count']
                            else:
                                # We have changed section so increment count 
                                eventMsg = 'Incrementing section count for train' + str(currTrain) + '\n' + \
                                            'currentDBTrainDetails[currTrain][\'multi_train_most_recent_section_count\'] = ' + str(currentDBTrainDetails[currTrain]['multi_train_most_recent_section_count'])
                                eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                                multitrainSectionCount = currentDBTrainDetails[currTrain]['multi_train_most_recent_section_count'] + 1
                            noConnectedTrains = currentDBTrainDetails[currTrain]['most_recent_no_connected_trains']
                            listConnectedTrains = currentDBTrainDetails[currTrain]['most_recent_list_connected_trains']
                            currTrainAtBritomartEnd = currentDBTrainDetails[currTrain]['train_at_britomart_end']

                        else:
                            #
                            # - Train has trip details
                            # - It wasn't part of a 6 or has expired from being in a 6
                            #
                            # So it's a 3 train with trip details so update
                            #
                            # Remember "currTrainRouteID" has already been updated above
                            #                    
                            multitrainSectionCount = 1
                            noConnectedTrains = 1
                            listConnectedTrains = trainFriendlyName                    
                            currTrainAtBritomartEnd = 'na'

                    else:
                        # 
                        # If we get here it means for this api call, this train was NOT identified as
                        # being part of a 6 carridge train. Though it could have been
                        #
                        # Also this train doesn't have any trip information
                        #
                        # This could mean:
                        # - It is part of a 6 carridge train, but just isn't at a station or for some reason the trains with the
                        #   trip details didn't fit into this api window
                        # - It doesn't have trip details for some reason
                        # - It could have been in a 6, split in half and this one is on it's way back to the yard
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
                                # BUT remember we can only do this if is in a station section, ie section type equals 'S'.
                                if trainDetails['train'][currTrain]['section']['type'] == 'S':
                                    multitrainSectionCount = currentDBTrainDetails[currTrain]['multi_train_most_recent_section_count'] + 1
                                else:
                                    multitrainSectionCount = currentDBTrainDetails[currTrain]['multi_train_most_recent_section_count']


                            # Rest of the details are unchanged - Note 'multi_train_most_recent_section' will fix itself as that is always
                            # set to the current section
                            noConnectedTrains = currentDBTrainDetails[currTrain]['most_recent_no_connected_trains']
                            currTrainRouteID = currentDBTrainDetails[currTrain]['most_recent_route_id']
                            currTrainAtBritomartEnd = currentDBTrainDetails[currTrain]['train_at_britomart_end']
                            listConnectedTrains = currentDBTrainDetails[currTrain]['most_recent_list_connected_trains']
                            
                        else:
                            #
                            # If this train has been through 'multiTrainDetailsMaxRetentionCount' number of sections then we 
                            # can no-longer assume it is still part of the same 6 carriage train
                            #
                            noConnectedTrains = 1
                            multitrainSectionCount = 99

                            #
                            # If this is a single 3 carridge train that is no-longer a part of a 6 and doesn't have trip
                            # details then we assume it's 'out of service'
                            #
                            currTrainRouteID = routeDetails['at_route_id']['oos']['route_id']  
                            listConnectedTrains = trainFriendlyName
                            currTrainAtBritomartEnd = 'na'

                    #
                    # Update train details
                    #
                    trainDetails['train'][currTrain].update({'most_recent_list_connected_trains':listConnectedTrains})
                    trainDetails['train'][currTrain].update({'most_recent_no_connected_trains':noConnectedTrains})
                    trainDetails['train'][currTrain].update({'most_recent_route_id':currTrainRouteID})
                    trainDetails['train'][currTrain].update({'train_at_britomart_end':currTrainAtBritomartEnd})
                    trainDetails['train'][currTrain].update({'multi_train_most_recent_section': trainDetails['train'][currTrain]['section']['id']})
                    trainDetails['train'][currTrain].update({'multi_train_most_recent_section_count':multitrainSectionCount})

                    

        #
        # Step through the current trains and update the DB where necessary
        #
        cursorUpdateTrains = DBConnection.cursor(dictionary=True)
        for currTrain in trainDetails['train']:

            eventMsg = 'Updating table \'fmt_train_details\' for train ' + str(currTrain)
            eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

            # Check if the trains data is still valid
            if trainDetails['train'][currTrain]['train_data_is_valid']:

                odometer = currentDBTrainDetails[currTrain]['odometer']  
                if 'odometer' in trainDetails['train'][currTrain]['vehicle']['position']:
                    odometer = trainDetails['train'][currTrain]['vehicle']['position']['odometer']
                    
                try:
                    updateQuery = ''' UPDATE 
                                        fmt_train_details 
                                        SET 
                                        odometer = %s,
                                        most_recent_route_id = %s,
                                        train_at_britomart_end = %s,
                                        most_recent_list_connected_trains = %s,
                                        most_recent_no_connected_trains = %s,
                                        multi_train_most_recent_section = %s,
                                        multi_train_most_recent_section_count = %s,
                                        section_id = %s,
                                        section_id_updated = %s,
                                        heading_to_britomart = %s,
                                        latest_event_id = %s
                                        WHERE 
                                        train_number = %s'''
                    updateValues = (odometer,
                                    trainDetails['train'][currTrain]['most_recent_route_id'],
                                    trainDetails['train'][currTrain]['train_at_britomart_end'],
                                    trainDetails['train'][currTrain]['most_recent_list_connected_trains'],
                                    trainDetails['train'][currTrain]['most_recent_no_connected_trains'],         
                                    trainDetails['train'][currTrain]['multi_train_most_recent_section'],
                                    trainDetails['train'][currTrain]['multi_train_most_recent_section_count'],
                                    trainDetails['train'][currTrain]['section']['id'],
                                    posixtoDateTime(trainDetails['train'][currTrain]['vehicle']['timestamp']),
                                    trainDetails['train'][currTrain]['heading_to_britomart'],
                                    nextEventID,
                                    currTrain,
                                    )
                    cursorUpdateTrains.execute(updateQuery, updateValues)
                    DBConnection.commit()
                except mysql.connector.Error as err:
                    eventMsg = str(err)
                    eventLogger('error', eventMsg, 'Error updating train details in database table \'fmt_train_details\'.', str(inspect.currentframe().f_lineno))

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

            specialTrainDetails = {}

            # Load rows
            for currRow in specialTrainsDetailsReader:
                specialTrainDetails.update({currRow['train_number']:currRow})
                if currRow['train_number'] == '0':
                    defaultTrainFound = True

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
                                        special_train = true
                                    WHERE 
                                        train_number = %s
                                '''
                    updateValues = (specialTrainDetails[currTrain]['train_featured_img_url'],
                                    specialTrainDetails[currTrain]['train_small_img_url'],
                                    specialTrainDetails[currTrain]['train_description'],
                                    specialTrainDetails[currTrain]['custom_name'],
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
    # Ensure the fmt_routes table is correct
    #
    def loadTrainRoutes():

        eventMsg = 'Running loadTrainRoutes()'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

        #
        # There needs to be a row for default train details - as in trains that aren't special
        # The default train has a number '0'
        #
        unknownRouteFound = False
        outOfServiceRouteFound = False
        
        #
        # Load csv into dict
        #
        with open(trainRoutesFilename, mode='r', encoding='windows-1252') as routeDetailsCSV:
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
                if (currRow['route_id'] == "") or (currRow['at_route_id'] == "") or (currRow['route_name_to_britomart'] == "") or (currRow['route_name_from_britomart'] == "")\
                    or (currRow['route_id'] in routeDetails['route_id']) or (currRow['at_route_id'] in routeDetails['at_route_id']):

                    eventMsg = 'There is a problem for \'ID\' in \'' + trainRoutesFilename + '\'.' + '\n\n' + \
                                json.dumps(currRow, indent=4, sort_keys=True, default=str) + '\n\n' + \
                                'Check all columns have values.' + '\n' + \
                                'Check all \'ID\' values are unique.' + '\n' + \
                                'Check all \'AT route id\' values are unique.' + '\n\n' + \
                                'routeDetails: ' + json.dumps(routeDetails, indent=4, sort_keys=True, default=str) 
                    eventLogger('error', eventMsg, 'Problem for \'ID\' in \'' + trainRoutesFilename + '\'', str(inspect.currentframe().f_lineno))

                routeDetails['route_id'].update({currRow['route_id']:currRow})
                routeDetails['at_route_id'].update({currRow['at_route_id']:currRow})
                if currRow['at_route_id'] == 'na':
                    unknownRouteFound = True
                if currRow['at_route_id'] == 'oos':
                    outOfServiceRouteFound = True
            
            if not unknownRouteFound:
                    eventMsg = 'No \'na\' route found in \'' + specialTrainsFilename + '\'.' + '\n' + \
                               'This is the route description that will be used where no route id has been given.' + '\n' + \
                               'Ensure this file has a row with a \'AT route id\' of \'na\'.'
                    eventLogger('error', eventMsg, 'No \'na\' route found in \'' + specialTrainsFilename + '\'.', str(inspect.currentframe().f_lineno))
            if not outOfServiceRouteFound:
                    eventMsg = 'No \'oss\' route found in \'' + specialTrainsFilename + '\'.' + '\n' + \
                               'This is the route description that will be used to describe out of service trains.' + '\n' + \
                               'Ensure this file has a row with a \'AT route id\' of \'oos\'.'
                    eventLogger('error', eventMsg, 'No \'oos\' route found in \'' + specialTrainsFilename + '\'.', str(inspect.currentframe().f_lineno))

        #
        # Get a list of all routes in the "fmt_routes" table
        #
        cursorRoutesList = DBConnection.cursor(dictionary=True)
        sqlQuery = 'select * from fmt_routes'
        try:
            cursorRoutesList.execute(sqlQuery)
        except mysql.connector.Error as err:
                        eventMsg = str(err)
                        eventLogger('error', eventMsg, 'Error querying \'fmt_routes\', in database', str(inspect.currentframe().f_lineno))
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
        for routeID in routeDetails['route_id']:
            if int(routeID) in knownRoutes:
                #
                # Update the row only if it is not correct
                #
                if (routeDetails['route_id'][routeID]['at_route_id'] != knownRoutes[int(routeID)]['at_route_id']) or \
                (routeDetails['route_id'][routeID]['route_name_to_britomart'] != knownRoutes[int(routeID)]['route_name_to_britomart']) or \
                (routeDetails['route_id'][routeID]['route_name_from_britomart'] != knownRoutes[int(routeID)]['route_name_from_britomart']):
                    try:                    
                        updateQuery = ''' UPDATE fmt_routes SET at_route_id = %s, route_name_to_britomart = %s, route_name_from_britomart = %s WHERE id = %s'''
                        updateValues = (routeDetails['route_id'][routeID]['at_route_id'],
                                        routeDetails['route_id'][routeID]['route_name_to_britomart'],
                                        routeDetails['route_id'][routeID]['route_name_from_britomart'],
                                        routeID,
                                        )
                        cursorRoutesList.execute(updateQuery, updateValues)
                        DBConnection.commit()
                    except mysql.connector.Error as err:
                        eventMsg = str(err)
                        eventLogger('error', eventMsg, 'Error updating route details, in \'fmt_routes\', in database', str(inspect.currentframe().f_lineno))
            else:
                #
                # Current details aren't in the table so add them
                #
                try:
                    insertQuery = ''' INSERT INTO fmt_routes 
                                    (id,
                                    at_route_id,
                                    route_name_to_britomart,
                                    route_name_from_britomart
                                    )
                                    VALUES ( %s, %s, %s, %s)'''
                    insertValues = (routeID,
                                    routeDetails['route_id'][routeID]['at_route_id'],
                                    routeDetails['route_id'][routeID]['route_name_to_britomart'],
                                    routeDetails['route_id'][routeID]['route_name_from_britomart'],
                                    )
                    cursorRoutesList.execute(insertQuery, insertValues)
                    DBConnection.commit()
                except mysql.connector.Error as err:
                    eventMsg = str(err)
                    eventLogger('error', eventMsg, 'Error inserting new route details, into table \'fmt_routes\'.', str(inspect.currentframe().f_lineno))

        return routeDetails


    

    #
    # Make the api call to get current details about vehicles
    #
    def getCurrVehicleDetails(specialTrainDetail):
        global apiTimestampPosix
        global trainDetails

        eventMsg = 'Running Running getCurrVehicleDetails()'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))


        #
        # Get vehicle positions via api call
        # 
        vehiclePositionsResponse = apiRequest(atVehiclePosURL, True, 4, 'Vehicle positions')
        apiTimestampPosix = vehiclePositionsResponse['response']['header']['timestamp']
        
        #
        # Get a list of all trains in the "fmt_train_details" table
        #
        cursorTrainList = DBConnection.cursor(dictionary=True)
        sqlQuery = 'select train_number from fmt_train_details'
        cursorTrainList.execute(sqlQuery)
        knownTrains = []
        for currTrain in cursorTrainList:
            knownTrains.append(currTrain['train_number'])

        #
        # Load train data
        #
        # With little alternative we will determine that the vehicle is a Train it has an 'id' tha begins 59
        # Also it has a 'label' that begins with 'AMP ' - note the space and uppercase.
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
                    #
                    if 'bearing' in currVehicle['vehicle']['position']:
                        trainDetails['train'][currTrainNo]['vehicle']['position']['bearing'] = str(currVehicle['vehicle']['position']['bearing'])

                    trainLabel = trainDetails['train'][currTrainNo]['vehicle']['vehicle']['label']
                    currLatitude = trainDetails['train'][currTrainNo]['vehicle']['position']['latitude'] 
                    currLongitude = trainDetails['train'][currTrainNo]['vehicle']['position']['longitude']
                    imgCoords = geographicLocToImgLoc(currLatitude, currLongitude, trackDetails)
                    friendlyName = trainDetails['train'][currTrainNo]['vehicle']['vehicle']['label'].replace(' ', '')

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
                                rgbValue = mapContext.getpixel((xNewPos,yNewPos)) 
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
                            if currTrainBearingStr.isdigit():
                                currTrainBearing = int(currTrainBearingStr)
                                trainHasValidBearing = True
                            else:
                                eventMsg = 'Trains bearing is not a digit: \'' + str(currTrainBearingStr) + '\', train = ' + str(currTrainNo)
                                eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
                        else:
                            eventMsg = 'Train does not have a \'bearing\' value. Train = ' + str(currTrainNo)
                            eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

                        trainDetails['train'][currTrainNo].update({'heading_to_britomart':'na'})
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
                        # Update the database train details
                        #
                        hasTripDetails = False
                        if 'trip' in trainDetails['train'][currTrainNo]['vehicle']:
                            hasTripDetails = True
                        
                        trainDetails['train'][currTrainNo].update({'friendly_name':friendlyName})
                        trainID = trainDetails['train'][currTrainNo]['vehicle']['vehicle']['id']
                        trainLabel = trainDetails['train'][currTrainNo]['vehicle']['vehicle']['label']
                        trainOdometer = -1
                        if 'odometer' in trainDetails['train'][currTrainNo]['vehicle']['position']:
                            trainDetails['train'][currTrainNo]['vehicle']['position']['odometer']
                        customName = friendlyName
                        imageURL = specialTrainDetail['0']['train_featured_img_url']
                        smallImageURL = specialTrainDetail['0']['train_small_img_url']
                        trainDescription = specialTrainDetail['0']['train_description']
                        geoLocation = str(trainDetails['train'][currTrainNo]['vehicle']['position']['latitude']) + ',' + \
                                      str(trainDetails['train'][currTrainNo]['vehicle']['position']['longitude'])
                        
                        # Work out the trip id
                        currentTripID = ''
                        if 'trip' in trainDetails['train'][currTrainNo]['vehicle']:
                            currentTripID = trainDetails['train'][currTrainNo]['vehicle']['trip']['trip_id']

                        if currTrainNo in specialTrainDetail:
                            customName = specialTrainDetail[currTrainNo]['custom_name']
                            imageURL = specialTrainDetail[currTrainNo]['train_featured_img_url']
                            smallImageURL = specialTrainDetail[currTrainNo]['train_small_img_url']
                            trainDescription = specialTrainDetail[currTrainNo]['train_description']
                        if currTrainNo in knownTrains:
                            
                            try:
                                updateQuery = '''   UPDATE fmt_train_details 
                                                    SET vehicle_id = %s, 
                                                        vehicle_label = %s, 
                                                        friendly_name = %s,
                                                        odometer = %s, 
                                                        train_featured_img_url = %s, 
                                                        train_small_img_url = %s, 
                                                        train_description = %s, 
                                                        custom_name =%s, 
                                                        has_trip_details = %s, 
                                                        geo_location = %s, 
                                                        latest_event_id = %s,
                                                        trip_id = %s 
                                                    WHERE train_number = %s'''
                                updateValues = (trainID,
                                                trainLabel,
                                                friendlyName,
                                                trainOdometer,
                                                imageURL,
                                                smallImageURL,
                                                trainDescription,
                                                customName,
                                                hasTripDetails,
                                                geoLocation,
                                                nextEventID,
                                                currentTripID,
                                                currTrainNo,                                                
                                                )
                                cursorTrainList.execute(updateQuery, updateValues)
                                DBConnection.commit()
                            except mysql.connector.Error as err:
                                eventMsg = str(err)
                                eventLogger('error', eventMsg, 'Error updating train details, in database table \'fmt_train_details\'.', str(inspect.currentframe().f_lineno))
                        else:
                            # We don't know the route id so set to the unknown route
                            mostRecentRouteID = routeDetails['at_route_id']['na']['route_id']   
                            mostRecentListConnectedTrains = friendlyName
                            mostRecentNoConnectedTrains = 1
                            multiTrainMostRecentSection = trainDetails['train'][currTrainNo]['section']['id']
                            multiTrainMostRecentSectionCount = 0
                            
                            try:
                                insertQuery = ''' INSERT INTO fmt_train_details 
                                                (vehicle_id,
                                                vehicle_label,
                                                friendly_name,
                                                odometer,
                                                train_featured_img_url,
                                                train_small_img_url,
                                                train_description,
                                                custom_name,
                                                train_number,
                                                most_recent_route_id,
                                                train_at_britomart_end,
                                                most_recent_list_connected_trains,
                                                most_recent_no_connected_trains,
                                                multi_train_most_recent_section,
                                                multi_train_most_recent_section_count,
                                                section_id,
                                                section_id_updated,
                                                heading_to_britomart,
                                                has_trip_details,
                                                geo_location,
                                                latest_event_id,
                                                trip_id
                                                )
                                                VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                                insertValues = (trainID,
                                                trainLabel,
                                                friendlyName,
                                                trainOdometer,
                                                imageURL,
                                                smallImageURL,
                                                trainDescription,
                                                customName,
                                                currTrainNo,
                                                mostRecentRouteID,
                                                'na',
                                                mostRecentListConnectedTrains,
                                                mostRecentNoConnectedTrains,
                                                multiTrainMostRecentSection,
                                                multiTrainMostRecentSectionCount,
                                                trainDetails['train'][currTrainNo]['section']['id'],
                                                posixtoDateTime(trainDetails['train'][currTrainNo]['vehicle']['timestamp']),
                                                trainDetails['train'][currTrainNo]['heading_to_britomart'],
                                                hasTripDetails,
                                                geoLocation,
                                                nextEventID,
                                                currentTripID,
                                                )
                                cursorTrainList.execute(insertQuery, insertValues)
                                DBConnection.commit()
                            except mysql.connector.Error as err:
                                eventMsg = 'Error inserting new train details, in table \'fmt_train_details\'.' + '\n\n' + \
                                           'trainID                         : ' + str(trainID) + '\n' + \
                                           'trainLabel                      : ' + str(trainLabel) + '\n' + \
                                           'friendlyName                    : ' + str(friendlyName) + '\n' + \
                                           'trainOdometer                   : ' + str(trainOdometer) + '\n' + \
                                           'imageURL                        : ' + str(imageURL) + '\n' + \
                                           'smallImageURL                   : ' + str(smallImageURL) + '\n' + \
                                           'trainDescription                : ' + str(trainDescription) + '\n' + \
                                           'customName                      : ' + str(customName) + '\n' + \
                                           'currTrainNo                     : ' + str(currTrainNo) + '\n' + \
                                           'mostRecentRouteID               : ' + str(mostRecentRouteID) + '\n' + \
                                           'at britomart end                : ' + 'na' + '\n' + \
                                           'mostRecentListConnectedTrains   : ' + str(mostRecentListConnectedTrains) + '\n' + \
                                           'mostRecentNoConnectedTrains     : ' + str(mostRecentNoConnectedTrains) + '\n' + \
                                           'multiTrainMostRecentSection     : ' + str(multiTrainMostRecentSection) + '\n' + \
                                           'multiTrainMostRecentSectionCount: ' + str(multiTrainMostRecentSectionCount) + '\n' + \
                                           'section id                      : ' + str(trainDetails['train'][currTrainNo]['section']['id']) + '\n' + \
                                           'timestamp                       : ' + str(posixtoDateTime(trainDetails['train'][currTrainNo]['vehicle']['timestamp'])) + '\n' + \
                                           'heading to britomart            : ' + str(trainDetails['train'][currTrainNo]['heading_to_britomart']) + '\n\n' + \
                                           'latest_event_id                 : ' + str(nextEventID) + '\n\n' + \
                                           str(err)
                                eventLogger('error', eventMsg, 'Error inserting new train details, in table \'fmt_train_details\'', str(inspect.currentframe().f_lineno))

                        #
                        # Determine initial details to insert or update
                        #
                        dbRouteID= routeDetails['at_route_id']['na']['route_id']

                        if 'trip' in trainDetails['train'][currTrainNo]['vehicle']:
                            ATRouteID = trainDetails['train'][currTrainNo]['vehicle']['trip']['route_id']
                            currTrainRouteID = routeDetails['at_route_id'][ATRouteID]['route_id']
                            if currTrainRouteID in routeDetails['route_id']:
                                dbRouteID= routeDetails['route_id'][currTrainRouteID]['route_id']
                            else:
                                eventMsg = 'Route NOT KNOWN'
                                eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))

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
                            eventMsg = str(err)
                            eventLogger('error', eventMsg, 'Error querying database table \'fmt_locations\'', str(inspect.currentframe().f_lineno))

                        currLocationRow = cursorLocations.fetchone()
                        dbRowID = -1
                        insertNewRow = False
                        if cursorLocations.rowcount < 1:
                            insertNewRow = True
                        else:
                            dbRowID = currLocationRow['id']
                            if (currLocationRow['section_id'] == int(dbSectionID)):
                               #
                               # Check if the timestamp is different
                               #
                               # If its the same of course we do nothing
                               #
                               if (currLocationRow['last_updated_posix'] != currTimestampPosix):
                                    #
                                    # If we get here then there is at least one row of data and
                                    # that row is for the same track section as where the train
                                    # is currently and the timestamp is different.
                                    #
                                    # So we need to UPDATE this row - rather than insert
                                    #

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
                                        eventMsg = str(err)
                                        eventLogger('error', eventMsg, 'Error updating route details database table \'fmt_routes\'.', str(inspect.currentframe().f_lineno))
                            else:
                                # The section is different so we need to insert a new row
                                insertNewRow = True               

                        if insertNewRow:
                            #
                            # Insert new row
                            #
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
                                eventMsg = str(err)
                                eventLogger('error', eventMsg, 'Error inserting new location details, in database table \'fmt_locations\'', str(inspect.currentframe().f_lineno))


                            #
                            # Truncate similar records
                            #
                            try:
                                trucateQuery = '''
                                                DELETE FROM fmt_locations  
                                                WHERE row_inserted < now() - interval %s DAY'''
                                truncateValues = (  
                                                    retainLocationRowsDays,
                                                )
                                cursorTrainList.execute(trucateQuery, truncateValues)
                                DBConnection.commit()
                            except mysql.connector.Error as err:
                                eventMsg = str(err)
                                eventLogger('error', eventMsg, 'Error truncating rows in database table \'fmt_locations\'.', str(inspect.currentframe().f_lineno))

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
                        eventMsg =  'maxSearchRadiusReached = ' + str(maxSearchRadiusReached) + '\n' + \
                                    'Train = ' + str(currTrainNo) + '\n' + \
                                    'currLatitude = ' + str(currLatitude) + '\n' + \
                                    'currLongitude = ' + str(currLongitude) + '\n'
                        eventLogger('warn', eventMsg, 'Track details not found for train \'' + friendlyName + '\'', str(inspect.currentframe().f_lineno))

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
                            a = float(currPointSplit[0].strip())
                            a = float(currPointSplit[1].strip())
                        except ValueError as err:
                            eventMsg = 'The problem was the line with \'ID\': ' + currRowID + '\n' + \
                                       'Points value causing an issue was: \'' + currPoint + '\'.' + '\n\n' + \
                                       'The error returned was: ' + str(err)
                            eventLogger('error', eventMsg, 'Error with input details for \'' + trackDetailsFilename + '\'.', str(inspect.currentframe().f_lineno))

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
                        eventMsg =  'Discrepency found for ' + str(currRowIDStr) + ' = ' + str(knownSectionDetails) + '\n' + \
                                    'currRow[\'bearing_to_britomart\'] = ' + str(bearingInt) + '\n' + \
                                    'currRow[\'title\'] = ' + str(currRow['title']) + '\n' + \
                                    'currRow[\'type\'] = ' + str(currRow['type'])
                        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
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
                            eventMsg = str(err)
                            eventLogger('error', eventMsg, 'Error updating section details, in \'fmt_track_sections\'', str(inspect.currentframe().f_lineno))

                else:
                    eventMsg =  'NOT FOUND section \'' + currRowIDStr + '\''
                    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
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

        return trackMap
    
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

    # Load route and special train details
    routeDetails = loadTrainRoutes()
    specialTrainDetail = loadSpecialTrainDetails()

    # Draw the map image
    eventMsg =  'Creating the map of the train tracks in Auckland'
    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
    mapContext = drawMap() 

    

    #
    # Start cycle of api calls
    #
    eventMsg =  'Finished all initialization, including loading routes and trains plus drawing the map.' + '\n' + \
                'Starting a cycle of api calls at: ' + str(scriptStartTime)
    eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
    

    lastApiCallStartTime = datetime.now()
    outOfTime = False
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
        getCurrVehicleDetails(specialTrainDetail)
        additionalCalculations(routeDetails)
        updateTripStopDetails()
        postUpdateTasks()
        
        currApiCallEndTime = datetime.now()
        eventMsg =  'Api cycle finished at ' + str(currApiCallEndTime) + ', and it took ' + str((currApiCallEndTime - lastApiCallStartTime).total_seconds()) + ' seconds.'
        eventMsg += '\n\n\n\n\n>>>---------------------------------<<<<\n'
        eventLogger('info', eventMsg, '', str(inspect.currentframe().f_lineno))
        eventLogger('info_close', '', '', str(inspect.currentframe().f_lineno))
   
        #
        # Check if we have time to do another api cycle
        #
        if (datetime.now() + timedelta(seconds=(freqApiCallsSec + scriptBufferTimeSec))) > scriptMaxFinishTime:
            #
            # Not enough time to do another cycle
            #
            outOfTime = True

        else:
            #
            # Doing another cycle but first need to sleep so the total time = freqApiCallsSec
            #
            nextApiCall = lastApiCallStartTime + timedelta(seconds=(freqApiCallsSec))
            sleepSec = (nextApiCall - datetime.now()).total_seconds()

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

