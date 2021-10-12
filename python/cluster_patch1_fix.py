#'////////////////////////////////////////////////////////////////////////////
#' FILE: cluster_patch1_fix.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-10-12
#' MODIFIED: 2021-10-12
#' PURPOSE: pull data from the cluster filemetadata table in RD3
#' STATUS: in.progress
#' PACKAGES: NA
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

# imports
import hashlib
from urllib.parse import quote_plus
import molgenis.client as molgenis
from datetime import datetime
import requests
import json
import re
import os

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
    def update_table(self, entity: str, data: list, hasApiUrl: bool = False):
        """Update Table
        
        When importing data into a new table using the client, there is a 1000
        row limit. This method allows you push data without having to worry
        about the limits.
        
        @param entity (str) : name of the entity to import data into
        @param data (list) : data to import
        @param hasApiUrl (bool) : if True, url will be built with _api_url
            (This supports instances that haven't been upgraded yet.)
        
        @return a status message
        """
        props = list(self.__dict__.keys())
        if '_url' in props: url = self._url
        if '_api_url' in props: url = self._api_url
        url = f'{url}v2/{quote_plus(entity)}'
        
        # single push
        if len(data) < 1000:
            try:
                response = self._session.post(
                    url = url,
                    headers = self._get_token_header_with_content_type(),
                    data = json.dumps({'entities' : data})
                )
                if not response.status_code // 100 == 2:
                    return f'Error: unable to import data\n{response.status_code}'
                
                return f'Imported {len(data)} entities into {str(entity)}'
            except requests.exceptions.HTTPError as err:
                raise SystemError(err)
        
        # batch push
        if len(data) >= 1000:    
            for d in range(0, len(data), 1000):
                try:
                    response = self._session.post(
                        url = url,
                        headers = self._get_token_header_with_content_type(),
                        data = json.dumps({'entities': data[d:d+1000] })
                    )
                    if not response.status_code // 100 == 2:
                        raise SystemError(f'Batch {d} Error: unable to import data ({response.status_code}):\n{response.content}')

                    return f'Batch {d}: Imported {len(data)} entities into {str(entity)}'
                except requests.exceptions.HTTPError as err:
                    raise SystemError(err)

    # batch_update_one_attr
    def batch_update_one_attr(self, entity: str, attr: str, data: list):
        """Batch Update One Attribute
        
        Import data for an attribute in groups of 1000
        
        
        @param data (list) : data to import
        @param entity (str) : name of the entity to import data into
        
        @return a response code
        """
        props = self.__dict__.keys()
        if '_url' in props: url = self._url
        if '_api_url' in props: url = self._api_url
        url = f'{url}v2/{quote_plus(entity)}/{attr}'
        
        for d in range(0, len(data), 1000):
            try:
                response = self._session.put(
                    url = url,
                    headers = self._get_token_header_with_content_type(),
                    data = json.dumps({'entities': data[d:d+1000] })
                )
                
                if not response.status_code // 100 == 2:
                    raise SystemError(f'Batch {d} Error: unable to import data ({response.status_code}):\n{response.content}')
                
                return f'Batch {d}: Imported {len(data)} entities into {str(entity)}' 
            except requests.exceptions.HTTPError as err:
                raise SystemError(err)

#//////////////////////////////////////////////////////////////////////////////

# init molgenis session
rd3 = molgenis('https://solve-rd.gcc.rug.nl/api/')
rd3.login()

# get data
files = rd3.get(
    entity = 'rd3_portal_cluster',
    q = 'type=="phenopacket"',
    attributes = 'release,name,type',
    batch_size = 10000
)

subjects = rd3.get(
    entity = 'rd3_freeze1_subject',
    attributes = 'id,subjectID,patch',
    batch_size = 10000
)

status_msg('File metadata entries pulled: {}'.format(len(files)))
status_msg('Subject metadata entries pulled: {}'.format(len(subjects)))

# extract subject ID
for f in files:
    f['subject'] = re.sub(
        pattern = r'((.[0-9]{4}-[0-9]{2}-[0-9]{2})?(.json))$',
        repl = '',
        string = f['name'])

ids = [x['subject'] for x in files]

# update ptch
for s in subjects: 
    if s['subjectID'] in ids:
        patches = ['freeze1_patch1']
        for patch in s.get('patch'):
            patches.append(patch.get('id', None))
        s['patch'] = ','.join(list(set(patches)))
        

data = list(map(lambda x: {k: v for k, v in x.items() if k in ['id','patch']}, subjects))

rd3.batch_update_one_attr(
    entity = 'rd3_freeze1_subject',
    attr = 'patch',
    values = data
)

rd3.batch_update_one_attr(
    entity = 'rd3_freeze1_subjectinfo',
    attr = 'patch',
    values = data
)