from flask import Flask, jsonify
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import logging
import os
import pytz

app = Flask(__name__)
# setting timezone
tz_DE = pytz.timezone("Europe/Berlin")
###########------------ DATABASE CONFIGURATION ---------------------############################

URI = 'mongodb://' + os.environ['MONGO_USERNAME'] + ':' + os.environ['MONGO_PASSWORD'] + '@db'
client = MongoClient(host=URI, port=27017, connect=False)

#client = MongoClient("localhost", port=27017, connect=False)  #for local
db = client.excavatorDB

##########------------- LOGGING CONFIGURATION ---------------------#############################

#Create and configure logger 
logging.basicConfig(filename="app.log", 
                    format='%(asctime)s %(message)s',
                    datefmt="%m/%d/%Y %I:%M:%S %p %Z",
                    filemode='w') 
#Creating an object and setting logger level
logger=logging.getLogger() 
logger.setLevel(logging.DEBUG) 

###############---------------  REST API FUNCTIONS -------------------################################
@app.route('/')
def hello():
    try:
        return "Hello!!"
    except Exception as e:
        logger.error("Error " + str(e))


@app.route('/excavator_operating_hours_since_last_maintenance', methods=['GET'])
def get_hours_since_last_maintenance():
    """
        Gets the lastrecord data, gets record for last maintenance timestamp
        Difference in corresponding cumulative_hours_operated will give the hours since last maintenance
    """
    try:
        # Finding the latest of current date's record 
        currentDate = datetime(datetime.now(tz_DE).year, datetime.now(tz_DE).month, datetime.now(tz_DE).day)
        logger.debug(f"Current date is {currentDate}")
        datetimeFormat = '%Y-%m-%d %H:%M:%S'
        result = db.excavator_details.find({'timestamp': {'$gte' : currentDate}}).sort('_id', -1).limit(1)
        lastRecord = result.next()
        logger.info(f"Got last operational record {lastRecord}")

        # Extraction of the latest cumulative_hours_operated
        latestOpHr = lastRecord['cumulative_hours_operated']

        # Finding the record for timestamp same as in the last record's 'most_recent_maintenance'
        lastMaintenanceRecord = db.excavator_details.find({'timestamp': datetime.strptime(lastRecord['most_recent_maintenance'], datetimeFormat)})

        # Extraction of cumulative_hours_operated for recently maintained timestamp record
        lastMaintenanceOpHr = lastMaintenanceRecord.next()['cumulative_hours_operated']

        # Calculation of the hours since last Maintenance
        diff = latestOpHr - lastMaintenanceOpHr

        # Rounding off to 2 digits after decimal       
        return jsonify(round(diff, 2))  
    except Exception as e:
        logger.error(f"Exception: {e}")
        return "Error"


@app.route('/excavator_average_fuel_rate_past_24h', methods=['GET'])
def get_average_fuel_rate_past_24h():
    """
        Extracts excavator details records for the last 24 hours from current time
        Sorts it with respect to ascending order of timestamp
        Gets the first and latest record
        Calculates the average fuel rate
    """
    averageFuelRate = 0
    try:
        # extracting excavator_details for the past 24 hours
        currentTime = datetime(datetime.now(tz_DE).year, datetime.now(tz_DE).month, datetime.now(tz_DE).day, datetime.now(tz_DE).hour,  datetime.now(tz_DE).minute)
        logger.debug(f"Current date/time is {currentTime}")
        records = db.excavator_details.find({'timestamp': {'$gte': (currentTime-timedelta(days=1)), '$lte':currentTime}})
        # converting to list for getting the latest data and first data in the past 24 hours
        result = list(records.sort('timestamp', 1))
        
        if len(result) > 0:
            first = result[0]
            logger.info(f"Record 24 hours ago {first}")
            latest = result[len(result)-1]
            logger.info(f"Latest record {latest}")
                       
            # calculating average fuel rate = total fuel used / total operational hours
            averageFuelRate = (latest['cumulative_fuel_used'] - first['cumulative_fuel_used'])/(latest['cumulative_hours_operated'] - first['cumulative_hours_operated'])

        return jsonify(round(averageFuelRate, 2))
    except Exception as e:
        logger.error("Error " + str(e))
        return f"Error {str(e)}"


@app.route('/excavator_last_10_CAN_messages', methods=['GET'])
def get_last_10_CAN_messages():
    """
    Returns the last 10 CAN messages from excavator_canstatus collection
    """
    try:
        # finding last 10 can messages from excavator_canstatus collection based on sorting mongo Object Id _id in descending order
        records = db.excavator_canstatus.find({},{'id':1, 'message':1, 'timestamp':1, '_id':0}).sort('timestamp',-1).limit(10)

        # converting cursor to list 
        canList = list(records)

        # formatting timestamp from DB to local time
        for item in canList:
            item['timestamp'] = item['timestamp'].strftime("%c") 

        return jsonify(canList)
    except Exception as e:
        logger.error("Error " + str(e))
        return f"Error {str(e)}"


@app.route('/excavator_operational', methods=['GET'])
def get_operational_status():
    """
    Retrieves the latest record from excavator_status collection in DB
    and returns the corresponding status
    Output: operational or down
    """
    try:
        # fetching the latest excavator_status data where timestamp greater than current date and hour
        currentDate = datetime(datetime.now().year, datetime.now().month, datetime.now().day, datetime.now().hour)
        logger.info(f"Current date is {currentDate}")
        exists = db.excavator_status.count_documents({"timestamp": {'$gte' : currentDate}})
        if exists > 0:
            found = db.excavator_status.find({"timestamp": {'$gte' : currentDate}}).sort('_id', -1).limit(1).next()
            logger.info(f"Fetched latest record - {found}")
            status = found['status'].lower()
            return status
        else:
            return "No record found"
    except Exception as e:
        logger.error("Error " + str(e))
        return f"Error {str(e)}"
       


if __name__ == "__main__":
    app.run(debug=True)
