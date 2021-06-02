#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_ped_import.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-02
#' MODIFIED: 2021-06-02
#' PURPOSE: extract metadata from PED files and import into Molgenis
#' STATUS: in.progress
#' PACKAGES: molgenis.client, ...
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import os
import molgenis.client as molgenis
import json

# @title Molgenis Extra
# @describe Add script-specific methods to molgenis.Session class
# @param molgenis.Session required param
# @return ...
class molgenis_extra(molgenis.Session):
    def update_table(self, data, entity):
        for d in range(0, len(data), 1000):
            response = self._session.post(
                url=self._api_url + 'v2/' + entity,
                headers=self._get_token_header_with_content_type(),
                data=json.dumps({'entities': data[d:d+1000]})
            )
            if response.status_code == 201:
                print("Imported batch " + str(d) +
                      " successfully (" + str(response.status_code) + ")")
            else:
                print("Failed to import batch " + str(d) +
                      " (" + str(response.status_code) + ")")


#//////////////////////////////////////

# configure api
# os.environ['molgenisToken'] = ''
env = 'prod'
api = {
    'host': {
        'prod': 'https://solve-rd.gcc.rug.nl/api/',
        'acc': 'https://solve-rd-acc.gcc.rug.nl/api/'
    },
    'token': os.getenv('molgenisToken') if os.getenv('molgenisToken') is not None else None
}

# connect
rd3 = molgenis_extra(url=api['host'][env], token=api['token'])

