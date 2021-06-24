#'////////////////////////////////////////////////////////////////////////////
#' FILE: novelomics_cleanup.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-24
#' MODIFIED: 2021-06-24
#' PURPOSE: Clear novelomics data from freeze tables
#' STATUS: in.progress
#' PACKAGES: rd3tools
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////


import python.rd3tools as rd3tools
config = rd3tools.load_yaml_config('python/_config.yml')
rd3 = rd3tools.molgenis(url = config['hosts']['acc'], token = config['tokens']['acc'])

# @title batch_remove
# @description batch remove IDs from entitiy
# @param entity ID of the entity
# @param data list of IDs used to remove rows
# @retrun status message
def batch_remove(entity, data):
    if len(data) < 1000:
        rd3.delete_list(entity = entity, entities = data)
    else:
        for d in range(0, len(data), 1000):
            rd3.delete_list(
                entity = entity,
                entities = data[d+d:1000]
            )

#//////////////////////////////////////

# ~ 1 ~
# Remove Files Metadata

# pull data
freeze1_file_ids_raw = rd3.get(
    entity = 'rd3_freeze1_file',
    q = 'patch==novelomics_original',
    attributes='EGA,patch',
    batch_size = 10000
)

freeze2_file_ids_raw = rd3.get(
    entity = 'rd3_freeze2_file',
    q = 'patch==novelomics_original',
    attributes='EGA,patch',
    batch_size = 10000
)

# check
len(freeze1_file_ids_raw)
len(freeze2_file_ids_raw)

# flatten
freeze1_file_ids = rd3tools.flatten_attr(data = freeze1_file_ids_raw, attr = 'EGA')
freeze2_file_ids = rd3tools.flatten_attr(data = freeze2_file_ids_raw, attr = 'EGA')

# remove
batch_remove(entity = 'rd3_freeze1_file', data = freeze1_file_ids)
batch_remove(entity = 'rd3_freeze2_file', data = freeze2_file_ids)

#//////////////////////////////////////

# ~ 2 ~
# Clear Labinfo Novelomics Tables, and then delete manually in Molgenis

rd3.delete(entity = 'rd3_freeze1_labinfo_novelomics')
rd3.delete(entity = 'rd3_freeze2_labinfo_novelomics')

#/////////////////////////////////////////

# ~ 3 ~
# Clear Samples


# pull
freeze1_sample_ids_raw = rd3.get(
    entity = 'rd3_freeze1_sample',
    q = 'patch==novelomics_original',
    attributes = 'id,sampleID,patch',
    batch_size = 1000
)
freeze2_sample_ids_raw = rd3.get(
    entity = 'rd3_freeze2_sample',
    q = 'patch==novelomics_original',
    attributes = 'id,sampleID,patch',
    batch_size = 1000
)

# check
len(freeze1_sample_ids_raw)
len(freeze2_sample_ids_raw)

# flatten
freeze1_sample_ids = rd3tools.flatten_attr(data = freeze1_sample_ids_raw, attr = 'id')
freeze2_sample_ids = rd3tools.flatten_attr(data = freeze2_sample_ids_raw, attr = 'id')

# delete
batch_remove(entity = 'rd3_freeze1_sample', data = freeze1_sample_ids)
batch_remove(entity = 'rd3_freeze2_sample', data = freeze2_sample_ids)

