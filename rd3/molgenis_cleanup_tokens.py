#////////////////////////////////////////////////////////////////////////////
# FILE: molgenis_cleanup_tokens.py
# AUTHOR: David Ruvolo
# CREATED: 2022-05-12
# MODIFIED: 2022-05-12
# PURPOSE: clean up expired tokens in molgenis
# STATUS: stable
# PACKAGES: molgenis, datetime, pytz
# COMMENTS: This script is designed to run locally and deployed as a job on
# Molgenis server. Run the code based on your environment. If you are running
# the job locally, you can either copy your username and password in the
# script or in an `.env` file. If you are using a .env file, create two
# entries ('MOLGENIS_USERNAME' and 'MOLGENIS_PASSWORD') and run the script.
#////////////////////////////////////////////////////////////////////////////

import molgenis.client as molgenis
from datetime import datetime
import pytz

# ~ Local Dev ~
from dotenv import load_dotenv
from os import environ
load_dotenv()

host=environ['MOLGENIS_HOST_ACC']
db=molgenis.Session(url=host)
db.login(
    username=environ['MOLGENIS_USERNAME'],
    password=environ['MOLGENIS_PASSWORD']
)

# ~ Deployed ~
# when deployed
# host='http://localhost/api'
# token='${molgenisToken}'
# db = molgenis.Session(url=host, token=token)

def molgenisApiTimestamp(tz='Europe/Amsterdam'):
    """Molgenis API Timestamp
    Create ISO 8601 timestamp minus UTC offset for use in API requests
    
    @param tz an IANA time zone
    
    @return datetime string for molgenis api requests
    """
    return datetime.now(tz=pytz.timezone(tz)).isoformat().split('+')[0]
    
    
def setApiTimeFilter(columnName, endingOn,startingOn=None):
    """Set API Time Filter
    Format datetime range as a string
    
    @param columnName name of the column in the database that is a date
    @param endingOn ISO-8601 datetime string indicating the most recent datetime
    @param startingOn ISO 8601 datetime string indicating the earliest datetime
    
    @return string containg a date range filter for use in API requests
    """
    timeFilter=f'{columnName}=le={endingOn}'
    if startingOn:
        timeFilter=f'{columnName}=ge={startingOn};{timeFilter}'
    return timeFilter
    

# from the tokens table, retrieve the tokens that have expired at the time
# the script is executed, and then delete them.

timeFilter=setApiTimeFilter(columnName='expirationDate', endingOn=molgenisApiTimestamp())
expiredTokens=db.get(entity='sys_sec_Token', q=timeFilter, batch_size=10000)

tokensToRemove=[row['id'] for row in expiredTokens]

db.delete_list(entity='sys_sec_Token', entities=tokensToRemove)
