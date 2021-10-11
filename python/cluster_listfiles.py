#'////////////////////////////////////////////////////////////////////////////
#' FILE: cluster_listfiles.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-10-11
#' MODIFIED: 2021-10-11
#' PURPOSE: list and push phenopacket/ped files to RD3
#' STATUS: in.progress
#' PACKAGES: NA
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from urllib.parse import quote_plus
import molgenis.client as molgenis
from datetime import datetime
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
                url = self._url + 'v2/' + quote_plus(entity),
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
                    url=self._url + 'v2/' + entity,
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
dirs = list(
    filter(
        lambda x: x != 'master',
        os.listdir('/groups/solve-rd/tmp10/releases')
    )
)



# compile available files and prep for import
files = []
for dir in dirs:
    
    # gather list of pedigree (.ped) and phenopacket files (.json)
    raw_ped = os.listdir(dir + '/ped/')
    raw_json = os.listdir(dir + '/phenopacket/')
    
    # remove uncessary files
    ped_files = list(filter(lambda p: re.search(r'((.ped)|(.ped.cip))$', p), raw_ped))
    json_files = list(filter(lambda p: re.search(r'(.json)$', p), raw_json))
    cluster_files = ped_files + json_files
    
    # prep files
    for file in cluster_files:
        if re.search(r'((.ped)|(.ped.cip))$', file): folder = '/ped/'
        if re.search(r'(.json)$', file): folder = '/phenopacket/'
        
        # build entry for file
        entry = {
            'path': dir + folder + file,
            'name': file,
            'type': 'ped',
            'created': str(datetime.now())
        }
        
        # get checksum
        entry['md5sum'] = hashlib.md5(entry['filename']).hexdigest()
        files.append(entry)


# push data to molgenis
rd3 = molgenis('https://solve-rd.gcc.rug.nl/api/')
rd3.login('','') # set credentials only when run
rd3.update_table(files, 'rd3_portal_cluster_files')