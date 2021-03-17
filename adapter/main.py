import time
from time import ctime
import datetime
from datetime import datetime, timedelta
import json
import logging
import pymongo
from pymongo import MongoClient
import requests
import os
import pytz

# Config file 
import config

############ LOGGING CONFIGURATION    ########################################

#Create and configure logger 
logging.basicConfig(filename="adapter.log", 
                    format='%(asctime)s %(message)s',
                    datefmt="%m/%d/%Y %I:%M:%S %p %Z",
                    filemode='w') 
#Creating an object and setting of logger level 
logger=logging.getLogger() 
logger.setLevel(logging.DEBUG)

#############    DATABASE CONFIGURATION   ####################################

#client = MongoClient("localhost", port=27017)
URI = 'mongodb://' + os.environ['MONGO_USERNAME'] + ':' + os.environ['MONGO_PASSWORD'] + '@db'
client = MongoClient(URI)
db = client.excavatorDB

################  USER EXCEPTIONS   ###########################################

class InvalidTokenError(Exception):
    """Raised when response code is 401 considering it as token expired"""
    pass

class TokenExpiredError(Exception):
    """Raised when the input value is too large"""
    pass

class SessionExpiredError(Exception):
    """Raised when session is expired"""
    pass


###############  MAIN FUNCTIONS   ##############################################

def scrape_asset_manager(currentSession):
    """
        Scrape data from the BigCo asset manager site
        Input: current session
    """
    
    try:
        # scraping BigCo Login site

        # using the input session to access the excavator state
        response = currentSession.get(config.URL_CONFIG['excav_status'])
        
        if response.status_code == 200:

            # string manipulation of data retrieved to filter only the status
            # Sample data retrieved : "Excavator Status: OPERATIONAL"
            # Splits string and status is in last position
            excav_status = response.text.split()[-1]
            
            scrape_data = {
                    'status': excav_status,
                    'timestamp': datetime.now()
            }
            logger.info("Got status: " + excav_status)
                                
            # inserting status data in excavator_status in DB
            ok = db.excavator_status.insert_one(scrape_data)
            logger.debug(f"Inserted excavator status : {ok.inserted_id}")
            
        elif response.status_code == 440 or response.status_code == 401:
            raise SessionExpiredError
    except SessionExpiredError:
        raise SessionExpiredError


    
def pull_excavator_stats(time1, time2, token):
    
    """Pulls data from the /excavator_stats endpoint
       and persists them to excavator_details collection in DB
       (Before adding the above pulled data, checks if data for the most recent maintenance is
       present, if not pulls that and stores it first)
       Inputs: time1 - start time
               time2 - end time
               token - access token
    """
    try:
        
        # Pulls the data from endpoint for the time between time1 and time2
        statsList = _pull_excav_info(time1, time2, token)

        logger.info(f"Pulled {len(statsList)} records from endpoint")

        # Persisting data only if data is received
        if len(statsList) > 0:
           
           """ Before persisting data in excavator_details in DB
             1. Finding whats the recent maintenance time from the last record pulled
             2. Checking if we have record when timestamp = recent maintenance time
             3. If we already have -> then nothing to do
             4. If we dont have -> then pull that data and put it in DB before persisting the bulk data
           # The above is done so that we have operating hours data for answering REST API call
           for /excavator_operating_hours_since_last_maintenance """
           
           # 1. Retrieving stats of data of last maintenance
           lastMaintenanceDate = datetime.strptime(statsList[len(statsList)-1]['most_recent_maintenance'], '%Y-%m-%d %H:%M:%S')
           #lastMaintenanceDate = datetime(2021, 3, 1, 0, 0)
           lastMaintDataBool = False

           # Converting timestamp string to datetime before storing in DB
           # 2. And also checking if we have data for the timestamp = most_recent_maintenance
           for item in statsList:
               item["timestamp"] = datetime.strptime(item["timestamp"],'%Y-%m-%d %H:%M:%S')
               if item["timestamp"] == lastMaintenanceDate:
                   lastMaintDataBool = True 
                   logger.info("Last maintenance data already exists in list retrieved")

           # 3 & 4 When data of timestamp = recent maintenance is not there
           # in the data pulled from endpoint, we check if its already present in DB
           # start_time as lastMaintenanceDate and
           # end_time as lastMaintenanceDate + 1 minute
           if lastMaintDataBool == False: 
               _verify_lastMaintenanceData_exists(lastMaintenanceDate, token)

           # Inserting the pulled data for the time between time1 and time2 to DB
           ok = db.excavator_details.insert_many(statsList)
           logger.debug(f"Inserted excavator details : {ok.inserted_ids} records")
        else:
           logger.info("No data")
           logger.info(response)
    except InvalidTokenError:
        logger.info("Token expired")
        raise TokenExpiredError


def pull_can_data(token):
    """
        Pull from the GET can_stream endpoint
        Input: token
    """
    auth_header = {
        'Authorization': 'JWT ' + token
    }
    try:
        # pulls data from endpoint
        response = requests.get(config.URL_CONFIG['can_stream'], headers=auth_header)

        # checks for invalid token
        if response.status_code == 401:
            raise InvalidTokenError
        
        # converts response to list
        canstreamList = json.loads(response.text)
        logger.debug(f"CAN stream pulled : {canstreamList} records")
        if len(canstreamList) > 0:
            # converting timestamp from string to datetime before storing to DB
            for item in canstreamList:
                item["timestamp"] = datetime.strptime(item["timestamp"],'%Y-%m-%d %H:%M:%S')

            # since only last 10 messages required, previous message if any are deleted
            remove_oldies = db.excavator_canstatus.delete_many({})

            # inserting to excavator_canstatus collection in DB
            ok = db.excavator_canstatus.insert_many(canstreamList)
            logger.debug(f"Inserted excavator_canstatus: {len(ok.inserted_ids)} records ")
    except InvalidTokenError:
       logger.info("Invalid Token")
       raise TokenExpiredError



#####################---------HELPER FUNCTIONS-----------###############################################

def get_token():
    """
       Accesses /auth endpoint to get access token
       Output: token
    """
    try:
        response = requests.post(config.URL_CONFIG['auth'], headers=config.SCRAPE_HEADER, data=config.TOKEN_DATA)
        
        if response.status_code == 200:
            token = json.loads(response.text)['access_token']
            return token
        else:
            logger.error(f"Request to get token failed {response.status_code}" )
    except Exception as e:
        logger.error("Error in accesing token" + str(e))


def _verify_lastMaintenanceData_exists(lastMaintenanceDate, token):
    """ helper function
        - to check if excavator_details data exist for timestamp = lastMaintenanceDate provided
        - if data exists - nothing happens
        - if data does not exist in DB - it pulls the stats from the endpoint for that timeperiod
          and stores in DB
        Input: lastMaintenanceDate - a date from most_recent_maintenances
               token - access token for pulling stats from end point
    """
    
    logger.debug("Verifying if recent maintenance data exists.....")
    logger.debug(f"Most recent maintenance was at {lastMaintenanceDate}")
    exists = db.excavator_details.count_documents({'timestamp': lastMaintenanceDate})
    found = db.excavator_details.find({'timestamp': lastMaintenanceDate})
    if exists == 0:
              
       logger.info(f"No record exists {exists} - hence going to pull data for recent maintenance")
       endTime = lastMaintenanceDate + timedelta(minutes = 1)
       initialStats = _pull_excav_info(lastMaintenanceDate, endTime, token)
       
       logger.debug(f"Pulled data for recent maintenance {len(initialStats)}")
       for item in initialStats:
           item["timestamp"] = datetime.strptime(item["timestamp"],'%Y-%m-%d %H:%M:%S')
           
       logger.debug(initialStats)
       ok = db.excavator_details.insert_many(initialStats)
       logger.info(f"Inserted last maintenance({lastMaintenanceDate}) data in excavator_details DB : {ok.inserted_ids} records")
       logger.debug(f"Inserted data for last Maintenance date  : {len(ok.inserted_ids)}")
    else:
        logger.info("Last maintenance data already exists as below - so nothing to pull from endpoint")
        logger.info(found.next())

        

def _pull_excav_info(time1, time2, token):
    """Helper function -
       Frames the header and pulls from the GET excavator_stats endpoint
       Inputs: time1 - start time
               time2 - end time
               token - access token"""
    auth_header = {
          'Authorization': 'JWT ' + token
       }
    try:
       url = f"{config.URL_CONFIG['excav_info']}?start_time={time1:%Y-%m-%d+%H:%M}&end_time={time2:%Y-%m-%d+%H:%M}"
       statsResponse = requests.get(url, headers=auth_header)
       # Check for token expiry
       if response.status_code == 401:
           raise InvalidTokenError
       statsList = json.loads(statsResponse.text)
       return statsList
    except InvalidTokenError:
        raise TokenExpiredError


        

##########---------END OF FUNCTIONS-----------#######################################################

# setting timezone
tz_DE = pytz.timezone("Europe/Berlin")
# startTime and endTime is initially set to last 24 hours to pull excavator stats data
startTime = datetime(datetime.now(tz_DE).year, datetime.now(tz_DE).month, datetime.now(tz_DE).day - 1, 0, tzinfo=tz_DE) 
endTime = datetime.now(tz_DE)
logger.info(f"Start time is {startTime} end time is ${endTime}")

# gets token and uses the same until expired
token = get_token()

# creating session to scrape BigCo asset manager site 
s = requests.Session()
# logs in to BigCo Login Asset manager site
response= s.post(config.URL_CONFIG['login'], data=config.LOGIN_PAYLOAD)

# Pulls data from endpoint every minute
# If session expires or token expires, then pulls data immediately with new token/session
while True:
    try:
        scrape_asset_manager(s)
        pull_can_data(token)
        pull_excavator_stats(startTime, endTime, token)

        # processing next startTime and endTime for next minute
        startTime = endTime
        endTime = endTime + timedelta(minutes = 1)
        logger.debug(f"Next start time {startTime} and next end time {endTime} to pull excav stats")
        time.sleep(60)
       
    except SessionExpiredError:
        logger.info("Current session expired")
        logger.info("Trying to login again")
        response= s.post(config.URL_CONFIG['login'], data=config.LOGIN_PAYLOAD)
    except TokenExpiredError:
        logger.info("Access token Expired: getting another token")
        token = get_token()
    except Exception as e:
        print('Exception '+ str(e))
        





