#'////////////////////////////////////////////////////////////////////////////
#' FILE: cluster_listfiles.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-10-11
#' MODIFIED: 2021-10-12
#' PURPOSE: list and push phenopacket/ped files to RD3
#' STATUS: working
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

# set job info
wd = ''
host = ''
usr = ''
pwd = ''

# imports
from urllib.parse import quote_plus
import molgenis.client as molgenis
from datetime import datetime
import pandas as pd
import hashlib
import re
import os
import json

def status_msg(*args):
    """Status Message
    
    Prints a message with a timestamp
    
    @param *args : message to write 
    
    """
    msg = ' '.join(map(str, args))
    timestamp = datetime.utcnow().strftime('%H:%M:%S.%f')[:-3]
    print('\033[94m[' + timestamp + '] \033[0m' + msg)


class molgenis(molgenis.Session):
    """molgenis
    
    An extension of the molgenis.client class
    
    """
    
    # update_table
    def update_table(self, data, entity):
        """Update Table
        
        When importing data into a new table using the client, there is a 1000
        row limit. This method allows you push data without having to worry
        about the limits.
        
        @param self required class param
        @param data object containing data to import
        @param entity ID of the target entity written as 'package_entity'
        
        @return a response code
        """
        if len(data) < 1000:
            response = self._session.post(
                url = self._api_url + 'v2/' + quote_plus(entity),
                headers = self._get_token_header_with_content_type(),
                data = json.dumps({'entities' : data})
            )
            if response.status_code == 201:
                status_msg(
                    'Successfully imported data (response: {})'
                    .format(response.status_code)
                )
            else:
                status_msg(
                    'Failed to import data (response: {}): \nReason:{}'
                    .format(response.status_code, response.content)
                )
        else:    
            for d in range(0, len(data), 1000):
                response = self._session.post(
                    url=self._api_url + 'v2/' + entity,
                    headers=self._get_token_header_with_content_type(),
                    data=json.dumps({'entities': data[d:d+1000]})
                )
                if response.status_code == 201:
                    status_msg(
                        'Successfuly imported batch {} (response: {})'
                        .format(d, response.status_code)
                    )
                else:
                    status_msg(
                        'Failed to import data (response: {}): \nReason:{}'
                        .format(response.status_code, response.content)
                    )

#//////////////////////////////////////////////////////////////////////////////


# list available directories 
dirs = [wd + '/' + x for x in os.listdir(wd) if x != 'master']

# compile available files and prep for import
files = []
for dir in dirs:
    status_msg('Processing files in', dir)
    
    # gather list of pedigree (.ped) and phenopacket files (.json)
    raw_ped = os.listdir(dir + '/ped/')
    raw_json = os.listdir(dir + '/phenopacket/')
    
    # remove uncessary files
    ped_files = [f for f in raw_ped if re.search(r'((.ped)|(.ped.cip))$', f)]
    json_files = [f for f in raw_json if re.search(r'(.json)$', f)]
    cluster_files = ped_files + json_files
    
    # prep files
    for file in cluster_files:
        if re.search(r'((.ped)|(.ped.cip))$', file): folder = '/ped/'
        if re.search(r'(.json)$', file): folder = '/phenopacket/'
        
        # build entry for file
        entry = {
            'release': os.path.basename(dir),
            'path': dir + folder + file,
            'name': file,
            'type': folder.replace('/',''),
            'created': str(datetime.now()).replace(' ', 'T') + 'Z'
        }
        
        # get checksum
        entry['md5sum'] = hashlib.md5(entry['name'].encode('utf-8')).hexdigest()
        files.append(entry)

# filter files - remove duplicates
data = pd.DataFrame(files).drop_duplicates(subset = 'name', keep = 'first').to_dict('records')

# push data to molgenis
status_msg('Connecting to Molgenis database...')
rd3 = molgenis(host)
rd3.login(usr, pwd)

status_msg('Removing existing table')
rd3.delete('rd3_portal_cluster')

status_msg('Importing data into `rd3_portal_cluster`')
rd3.update_table(files, 'rd3_portal_cluster')