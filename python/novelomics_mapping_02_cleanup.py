#'////////////////////////////////////////////////////////////////////////////
#' FILE: novelomics_mapping_02_cleanup.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-05-06
#' MODIFIED: 2021-06-23
#' PURPOSE: clean up processed records
#' STATUS: working
#' PACKAGES: rd3tools
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import python.rd3tools as rd3tools
config = rd3tools.load_yaml_config('python/_config.yml')

# @title pull IDs
# @description create a list of IDs to remove based on processed status
def pull_ids(data):
    out = []
    for d in data:
        if d['processed']:
            out.append(d['molgenis_id'])
    return out

#//////////////////////////////////////////////////////////////////////////////

# init session
rd3tools.status_msg('Starting session...')
rd3 = rd3tools.molgenis(
    url = config['hosts'][config['run']['env']],
    token = config['tokens'][config['run']['env']]
)

# fetch processed cases from staging tables
rd3tools.status_msg('Fetching processed portal data...')

experiment = rd3.get(
    entity = 'rd3_portal_novelomics_experiment',
    q='processed==True',
    batch_size=10000
)

metadata = rd3.get(
    entity = 'rd3_portal_novelomics_shipment',
    q = 'processed==True',
    batch_size = 10000
)

# check results
if metadata:
    rd3tools.status_msg('Removing processed data from `rd3_portal_novelomics_shipment`...')
    metadata_ids_to_remove = pull_ids(data=metadata)
    rd3.delete_list(
        entity = 'rd3_portal_novelomics_shipment',
        entities = metadata_ids_to_remove
    )

if experiment:
    rd3tools.status_msg('Removing processed data from `rd3_portal_novelomics_experiment`...')
    experiment_ids_to_remove = pull_ids(data=experiment)
    rd3.delete_list(
        entity = 'rd3_portal_novelomics_experiment',
        entities = experiment_ids_to_remove
    )