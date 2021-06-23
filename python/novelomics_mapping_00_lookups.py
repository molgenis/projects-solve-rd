#'////////////////////////////////////////////////////////////////////////////
#' FILE: novelomics_mapping_00_lookups.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-05-05
#' MODIFIED: 2021-06-23
#' PURPOSE: process reference entities for Novel Omics data
#' STATUS: in.progress
#' PACKAGES: os, molgenis.client,
#' COMMENTS: This file should be run before the main novelomics processing
#'      script.
#'////////////////////////////////////////////////////////////////////////////


import python.rd3tools as rd3tools
config = rd3tools.load_yaml_config('python/_config.yml')

# init session
rd3 = rd3tools.molgenis(
    url = config['hosts'][config['run']['env']],
    token = config['tokens'][config['run']['env']]
)

# fetch data from the portal
experiment = rd3.get(
    entity = 'rd3_portal_novelomics_experiment',
    attributes='file_type,library_strategy',
    batch_size=1000
)

shipment = rd3.get(
    entity = 'rd3_portal_novelomics_shipment',
    attributes = 'organisation,ERN',
    batch_size=10000
)

# fetch RD3 reference entities
rd3_filetypes = rd3.get('rd3_typeFile')
rd3_seqtypes = rd3.get('rd3_seqType')
rd3_organisations = rd3.get('rd3_organisation')
rd3_ERN = rd3.get('rd3_ERN')

#//////////////////////////////////////

# process file types, if there are new records add to lookup table
rd3tools.status_msg('References: Looking for new filetypes...')
novelomics_filetypes = rd3tools.flatten_attr(
    data = experiment,
    attr = 'file_type',
    distinct = True
)

new_rd3_filetypes = rd3tools.lookups_find_new(
    lookup = rd3_filetypes,
    lookup_attr = 'identifier',
    new = novelomics_filetypes
)

if len(new_rd3_filetypes):
    rd3tools.status_msg(msg = 'Identified new file type references: ' + str(len(new_rd3_filetypes)))
    filetypes_to_upload = rd3tools.lookups_prep_new(data = new_rd3_filetypes)
    rd3tools.status_msg('Importing new file type lookups...')
    rd3tools.status_msg(filetypes_to_upload)
    rd3.update_table(data = filetypes_to_upload, entity = 'rd3_typeFile')
else:
    print('No new filetypes')

#//////////////////////////////////////

# process: sequenceTypes
# using the reference entity `seqType`, identify potential new cases in the
# experiment staging entity. 
rd3tools.status_msg('References: Looking for new sequencing types...')
novelomics_seqtypes = rd3tools.flatten_attr(
    data = experiment,
    attr = 'library_strategy',
    distinct = True
)

new_rd3_seqtypes = rd3tools.lookups_find_new(
    lookup = rd3_seqtypes,
    lookup_attr = 'identifier',
    new = novelomics_seqtypes
)

if len(new_rd3_seqtypes):
    rd3tools.status_msg(msg = 'Identified new seqType references:' + str(len(new_rd3_seqtypes)))
    seqtypes_to_upload = rd3tools.lookups_prep_new(data = new_rd3_seqtypes)
    rd3tools.status_msg('Importing new seqType lookups...')
    rd3tools.status_msg(seqtypes_to_upload)
    rd3.update_table(data = seqtypes_to_upload, entity = 'rd3_seqType')
else:
    rd3tools.status_msg('No new sequencing types identified')


# process Organisation
rd3tools.status_msg("References: Looking for new organisations...")
organisation_types = rd3tools.flatten_attr(
    data = shipment,
    attr = 'organisation',
    distinct = True
)

new_organisation_types = rd3tools.lookups_find_new(
    lookup = rd3_organisations,
    lookup_attr = 'name',
    new = organisation_types
)

if len(new_organisation_types):
    rd3tools.status_msg(msg = 'Identified new organisations: ' + str(len(new_organisation_types)))
    orgs_to_upload = rd3tools.lookups_prep_new(
        data = new_organisation_types,
        id_name = 'name',
        label_name = 'identifier',
        clean = True
    )
    rd3tools.status_msg('Importing new `organisations`...')
    rd3tools.status_msg(orgs_to_upload)
    rd3.update_table(data = orgs_to_upload, entity = 'rd3_organisation')
else:
    rd3tools.status_msg('No new organistations identified')


# process: European Reference Networks
rd3tools.status_msg("References: Looking for new ERNs")
ern_types = rd3tools.flatten_attr(data = shipment, attr = 'ERN', distinct = True)

new_ern_types = rd3tools.lookups_find_new(
    lookup = rd3_ERN,
    lookup_attr = 'identifier',
    new = ern_types
)

if len(new_ern_types):
    rd3tools.status_msg(msg = 'Identified new ERNs:' + str(len(new_ern_types)))
    rd3tools.status_msg(new_ern_types)
    rd3tools.status_msg('Please fix these before importing...')
else:
    rd3tools.status_msg('No new ERN types to add')

# END
rd3tools.status_msg("Done! :-)")