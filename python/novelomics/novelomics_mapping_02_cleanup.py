#'////////////////////////////////////////////////////////////////////////////
#' FILE: novelomics_mapping_02_cleanup.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-05-06
#' MODIFIED: 2021-05-06
#' PURPOSE: clean up processed records
#' STATUS: in.progress
#' PACKAGES: molgenis.client
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import os
import molgenis.client as molgenis

# @title pull IDs
# @description create a list of IDs to remove based on processed status
def pull_ids(data):
    out = []
    for d in data:
        if d['processed']:
            out.append(d['molgenis_id'])
    return out


# init API config
# os.environ['molgenisToken'] = ''
env = 'dev'
api = {
    'host': {
        'prod': 'https://solve-rd.gcc.rug.nl/api/',
        'acc' : 'https://solve-rd-acc.gcc.rug.nl/api/',
        'dev' : 'https://solve-rd-acc.gcc.rug.nl/api/',
    },
    'token': {
        'prod': '${molgenisToken}',
        'acc': '${molgenisToken}',
        'dev': os.getenv('molgenisToken') if os.getenv('molgenisToken') is not None else None
    }
}

# init session
rd3 = molgenis.Session(url=api['host'][env], token=api['token'][env])


# fetch processed cases from staging tables
experiment = rd3.get('rd3_portal_novelomics_experiment',q='processed==True',batch_size=10000)
metadata = rd3.get('rd3_portal_novelomics_shipment', q='processed==True',batch_size=10000)

# flatten IDs
experiment_ids_to_remove = pull_ids(data=experiment)
metadata_ids_to_remove = pull_ids(data=metadata)


# delete IDs from staging area
rd3.delete_list(entity='rd3_portal_novelomics_experiment', entities=experiment_ids_to_remove)
rd3.delete_list(entity='rd3_portal_novelomics_shipment', entities=metadata_ids_to_remove)