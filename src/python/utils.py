#'////////////////////////////////////////////////////////////////////////////
#' FILE: utils.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-02-03
#' MODIFIED: 2022-02-03
#' PURPOSE: Functions for mapping scripts
#' STATUS: stable
#' PACKAGES: see below
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

# define imports
import molgenis.client as molgenis
from urllib.parse import quote_plus
from datetime import datetime
import json
import requests

# generic timestamped messages
def status_msg(*args):
    """Status Message
    Prints a message with a timestamp
    @param *args : message to write 
    """
    msg = ' '.join(map(str, args))
    timestamp = datetime.utcnow().strftime('%H:%M:%S.%f')[:-3]
    print('[{}] {}'.format(timestamp, msg))


# extend molgenis class
class molgenis(molgenis.Session):
    """molgenis
    An extension of molgenis.client
    """
 
    def __baseUrl__(self):
        props = list(self.__dict__.keys())
        if '_url' in props: return self._url
        if '_api_url' in props: return self._api_url
    

    def update_table(self, data, entity):
        """Update Table
        When importing data into a new table using the client, there is a 1000
        row limit. This method allows you push data without having to worry
        about the limits.
        
        @param entity (str) : name of the entity to import data into
        @param data (list) : data to import
        
        @return a status message
        """
        url = f'{self.__baseUrl__()}v2/{quote_plus(entity)}'
        # single push
        if len(data) < 1000:
            try:
                response = self._session.post(
                    url = url,
                    headers = self._get_token_header_with_content_type(),
                    data = json.dumps({'entities' : data})
                )
                if not response.status_code // 100 == 2:
                    return f'Error: unable to import data({response.status_code}): {response.content}'
                print(f'Imported {len(data)} entities into {str(entity)}')
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
                        raise response.raise_for_status()
                    print(f'Batch {d}: Imported {len(data)} entities into {str(entity)}')
                except requests.exceptions.HTTPError as err:
                    raise SystemError(f'Batch {d} Error: unable to import data:\n{str(err)}')


    def batch_update_one_attr(self, entity: str, attr: str, data: list):
        """Batch Update One Attribute
        Import data for an attribute in batches (i.e., into groups of 1000 entities).
        Data should be a list of dictionaries with two keys: `id` and <attr> where
        attr is the name of the attribute that you would like to update
        
        @param data (list) : data to import
        @param attr (str) : name of the attribute to update
        @param entity (str) : name of the entity to import data into
        
        @return a response code
        """
        url = f'{self.__baseUrl__()}v2/{quote_plus(entity)}/{attr}' 
        for d in range(0, len(data), 1000):
            try:
                response = self._session.put(
                    url = url,
                    headers = self._get_token_header_with_content_type(),
                    data = json.dumps({'entities': data[d:d+1000] })
                )
                if not response.status_code // 100 == 2:
                    raise response.raise_for_status()
                print(f'Batch {d}: Imported {len(data)} entities into {str(entity)}')
            except requests.exceptions.HTTPError as err:
                raise SystemError(f'Batch {d} Error: unable to import data:\n{str(err)}')
