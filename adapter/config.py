from datetime import datetime

EXCAVSTATUS_CONFIG = {
    #'initial_start_time': datetime(2021, 3, 1, 0, 0),
    #'initial_end_time': datetime(2021, 3, 12, 14),
    #'sleep_interval': 60 #in seconds
}

URL_CONFIG = {
    'auth' : 'https://corrux-challenge.azurewebsites.net/auth',
    'login': 'https://corrux-challenge.azurewebsites.net/login',
    'excav_status': 'https://corrux-challenge.azurewebsites.net/status',
    'excav_info': 'https://corrux-challenge.azurewebsites.net/excavator_stats',
    'can_stream' : 'https://corrux-challenge.azurewebsites.net/can_stream'
    }

TOKEN_DATA = '{"username":"employee@bigco.com","password":"Bagger123!"}'

LOGIN_PAYLOAD = {
    'email': 'employee@bigco.com',
    'password': 'Bagger123!',
    'submit': 'Submit'
    }


#Scrape website header config  
SCRAPE_HEADER = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}

##MONGO = {
##    'username': 'root',
##    'password': ''
##    }

