#'////////////////////////////////////////////////////////////////////////////
#' FILE: novelomics_mapping_00_lookups.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-05-05
#' MODIFIED: 2021-05-06
#' PURPOSE: process reference entities for Novel Omics data
#' STATUS: in.progress
#' PACKAGES: os, molgenis.client,
#' COMMENTS: This file should be run before the main novelomics processing
#'      script.
#'////////////////////////////////////////////////////////////////////////////

import os  # for local testing only
import json
import molgenis.client as molgenis

# set token
# os.environ['molgenisToken'] = ''

# @title rd3_extra
# @param rd3 molgenis session
class molgenis_extra(molgenis.Session):
    def update_table(self, data, entity):
        for d in range(0, len(data), 1000):
            response = self._session.post(
                url=self._url + 'v2/' + entity,
                headers=self._get_token_header_with_content_type(),
                data=json.dumps({'entities': data[d:d+1000]})
            )
            if response.status_code == 201:
                print("Imported batch " + str(d) +
                    " successfully (" + str(response.status_code) + ")")
            else:
                print("Failed to import batch " + str(d) +
                    " (" + str(response.status_code) + ")")

# @title flatten attribute
# @description pull values from a specific attribute
# @param data list of dict
# @param name of attribute to flatten
# @param distinct if TRUE, return unique cases only
# @return a list of values
def flatten_attr(data, attr, distinct=False):
    out = []
    for d in data:
        tmp_attr = d.get(attr)
        out.append(tmp_attr)
    if distinct:
        return list(set(out))
    else:
        return out

# @title Identitfy new lookup values
# @description using a list of new values, determine if there are new values to update
# @param lookup RD3 lookup table
# @param lookup_attr the attribute to look into
# @param new a list of unique values
# @return a list of dictionaries of 
def identify_new_lookups(lookup, lookup_attr, new):
    refs = flatten_attr(lookup, lookup_attr)
    out = []
    for n in new:
        if (n in refs) == False:
            out.append({'id': n, 'label': n})
    return out

# @title Prepare Reference Types for Import
# @descrition prepare new reference data for import
# @param data list containing one or more dictionaries of new references
# @param id_name label to apply to the ID variable (id => identifier)
# @param id_var specific key to extract
# @param label_name label to map to 'label' key (i.e., name => label)
# @param label_var specific key to extract
def prepare_new_lookups(data, id_name='identifier', label_name='label'):
    out = []
    for d in data:
        new = {}
        new[id_name] = d
        new[label_name] = d
        out.append(new)
    return out

# set tokens and host
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

rd3 = molgenis_extra(url=api['host'][env], token=api['token'][env])

# fetch: novel omics data to process
experiment = rd3.get('rd3_portal_novelomics_experiment', batch_size=1000)
shipment = rd3.get('rd3_portal_novelomics_shipment', batch_size=10000)

# fetch: reference entities
rd3_filetypes = rd3.get('rd3_typeFile')
rd3_seqtypes = rd3.get('rd3_seqType')
rd3_organisations = rd3.get('rd3_organisation')
rd3_ERN = rd3.get('rd3_ERN')


# process file types, if there are new records add to lookup table
print('References: Looking for new filetypes...')
novelomics_filetypes = flatten_attr(experiment, 'file_type', distinct=True)
new_rd3_filetypes = identify_new_lookups(rd3_filetypes, 'identifier', novelomics_filetypes)

if len(new_rd3_filetypes):
    print('Identified new file type references:', len(new_rd3_filetypes))
    filetypes_to_upload = prepare_new_lookups(data = new_rd3_filetypes)
    print('Importing new file type lookups. (update labels manually)')
    print(filetypes_to_upload)
    rd3.update_table(data=filetypes_to_upload, entity='rd3_typeFile')
else:
    print('No new filetypes')


# process: sequenceTypes
# using the reference entity `seqType`, identify potential new cases in the
# experiment staging entity. 
print('References: Looking for new sequencing types...')
novelomics_seqtypes = flatten_attr(experiment, 'library_strategy', distinct=True)
new_rd3_seqtypes = identify_new_lookups(rd3_seqtypes, 'identifier', novelomics_seqtypes)

if len(new_rd3_seqtypes):
    print('Identified new seqType references:', len(new_rd3_seqtypes))
    seqtypes_to_upload = prepare_new_lookups(data=new_rd3_seqtypes)
    print('Importing new seqType lookups. (Update labels manually)')
    print(seqtypes_to_upload)
    rd3.update_table(data=seqtypes_to_upload, entity='rd3_seqType')
else:
    print('No new sequencing types identified')


# process Organisation
print("References: Looking for new submitting organisations...")
organisation_types = flatten_attr(shipment, 'organisation', distinct=True)
new_organisation_types = identify_new_lookups(rd3_organisations, 'name', organisation_types)

if len(new_organisation_types):
    print('Identified new organisations', len(new_organisation_types))
    orgs_to_upload = prepare_new_lookups(data=new_organisation_types, id_name='name', label_name='identifier')
    print('Importing new `organisations` (change labels manually)')
    print(orgs_to_upload)
    rd3.update_table(data = orgs_to_upload, entity='rd3_organisation')
else:
    print('No new organistations identified')


# process: European Reference Networks
print("References: Looking for new ERNs")
ern_types = flatten_attr(shipment, 'ERN', distinct=True)
new_ern_types = identify_new_lookups(rd3_ERN, 'identifier', ern_types)

if len(new_ern_types):
    print('Identified new ERNs', len(new_ern_types))
    print(new_ern_types)
    print('Please fix these before importing...')
else:
    print('No new ERN types to add')