# //////////////////////////////////////////////////////////////////////////////
# FILE: novelomics_mapping_01_main.py
# AUTHOR: David Ruvolo
# CREATED: 2021-04-15
# MODIFIED: 2021-06-09
# PURPOSE: process novel omics into Solve RD
# STATUS: working
# DEPENDENCIES: molgenis.client, os, json, datetime, time
# COMMENTS: The purpose of this script is to map novel omics metadata into
# main RD3 tables. Data is uploaded by external partners and this workflow
# maps values into RD3 terminology and imports them into the correct freeze.
# At the moment, this script covers Freeze 1 & 2. If new data does not exist
# in either freeze, then records are pushed into a 'holding' table. This script
# will likely need to be updated to pull and process data from the 'holding'
# table.
# //////////////////////////////////////////////////////////////////////////////

import os  # for local testing only
import json
import molgenis.client as molgenis
from datetime import datetime
from time import sleep
import requests
from urllib.parse import quote_plus, urlparse, parse_qs

# set mode
env = 'dev'

# set token
os.environ['molgenisToken'] = ''

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
                    " (" + str(response.status_code) + str(response.reason) + ")")
    def batch_update_one_attr(self, entity, attr, values):
        add = 'No new data'
        for i in range(0, len(values), 1000):
            add = 'Update did tot go OK'
            """Updates one attribute of a given entity with the given values of the given ids"""
            response = self._session.put(
                self._url + "v2/" + quote_plus(entity) + "/" + attr,
                headers=self._get_token_header_with_content_type(),
                data=json.dumps({'entities': values[i:i+1000]})
            )
            if response.status_code == 200:
                add = 'Update went OK'
            else:
                try:
                    response.raise_for_status()
                except requests.RequestException as ex:
                    self._raise_exception(ex)
                return response
        return add

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

# @title filter list of dictionaries
# @param data object to search
# @param attr variable find match
# @param value value to filter for
# @return a list of a dictionary
def find_dict(data, attr, value):
    return list(filter(lambda d: d[attr] in value, data))

# @title select_dict
# @description select keys from a dict
# @param data input dataset
# @param keys keys to select as array
# @param list of dictionaries reduced to the named keys
# @return list of dictionaries reduced to named keys
def select_dict(data, keys):
    return list(map(lambda x: {k:v for k, v in x.items() if k in keys}, data))

# @title distinct_dict
# @description get distinct dictionnaires only
# @param data a list containing one or more dictionaries 
# @param key one or more keys to filter by
# @return a list containing distinct dictionaries
def distinct_dict(data, key):
    if key is None:
        key = lambda x: x
    seen = set()
    for d in data:
        k = key(d)
        if k in seen:
            continue
        yield d
        seen.add(k)
    return seen

# @title evaluate patch values
# @description for subjects with existing patch information, extract and
#   collapse new patch information with existing
# @param data a single dictionary containing subject metadata
# @param patch new patch information
# @return a dictionary with the updated patch information
def proc_subject_patch(data, patch):
    out = []
    if len(data['patch']) == 1:
        out['patch'] = data['patch'][0]['id'] + ',' + patch
    else:
        out['patch'] = patch
    return out


# @title map new freeze files
# @description map file metadata to target EMX format
# @param data input data
# @param sample_id_suffix string to append to sample ID so that mrefs are properly linked
# @param patch SolveRD3 data release
# @return list of dictionaries
def map_rd3_files(data, sample_id_suffix, patch):
    out = []
    for d in data:
        tmp = {}
        tmp['EGA'] = d.get('file_ega_id')
        tmp['name'] = d.get('file_path') + '/' + d.get('file_name')
        tmp['md5'] = d.get('unencrypted_md5_checksum')
        tmp['typeFile'] = d.get('file_type')
        # tmp['filegroupID'] = d.get('file_group_id')
        tmp['samples'] = d.get('sample_id') + sample_id_suffix
        tmp['experimentID'] = d.get('project_experiment_dataset_id')
        # tmp['run_ega_id'] = d.get('run_ega_id')
        # tmp['experiment_ega_id'] = d.get('experiment_ega_id')
        tmp['patch'] = patch
        tmp['dateCreated'] = datetime.today().strftime('%Y-%m-%d')
        out.append(tmp)
    return out


# @title map new labinfo
# @param data input data
# @param id_suffix content to append to ID, e.g., "_original"
# @param sample_id_suffix string to append to sample ID so that mrefs are properly linked
# @param patch SolveRD3 data release
# @param distinct if True, distinct dictionaries will be returned
# @return list of dictionaries
def map_rd3_labinfo(data, id_suffix, sample_id_suffix, patch, distinct=False):
    out = []
    for d in data:
        tmp = {}
        tmp['id'] = d.get('project_experiment_dataset_id') + id_suffix
        tmp['experimentID'] = d.get('project_experiment_dataset_id')
        tmp['sample'] = d.get('sample_id') + sample_id_suffix
        tmp['capture'] = d.get('library_selection')
        tmp['libraryType'] = d.get('library_source').title()
        tmp['library'] = None
        if d.get('library_layout') == 'PAIRED':
            tmp['library'] = '1'
        tmp['sequencingCentre'] = d.get('sequencing_center')
        tmp['sequencer'] = d.get('platform_model')
        tmp['seqType'] = d.get('library_strategy')
        tmp['patch'] = patch
        out.append(tmp)
    if distinct:
        return list(distinct_dict(out, lambda x: ( x['id'], x['experimentID'] )))
    else:
        return out

# @title map new rd3 samples
# @param data input data
# @param id_suffix content to append to ID, e.g., "_original"
# @param subject_suffix A string indicating which patch in rd3_freeze*_subject to link to
# @param patch SolveRD3 data release
# @param distinct if True, distinct dictionaries will be returned
# @return list of dictionaries
def map_rd3_samples(data, id_suffix, subject_suffix, patch, distinct=False):
    out = []
    for d in data:
        tmp = {}
        tmp['id'] = d.get('sample_id') + id_suffix
        tmp['subject'] = d.get('subject_id') + subject_suffix
        tmp['sampleID'] = d.get('sample_id')
        tmp['tissueType'] = None
        if d.get('tissue_type') == 'blood':
            tmp['tissueType'] = 'Whole Blood'
        tmp['materialType'] = d.get('sample_type')
        tmp['patch'] = patch
        tmp['organisation'] = d.get('organisation', {}).get('identifier')
        tmp['ERN'] = d.get('ERN', {}).get('identifier')
        out.append(tmp)
    if distinct:
        return list(distinct_dict(out, lambda x: ( x['id'], x['sampleID'] ) ))
    else:
        return out


# @title update RD3 Subjects
# @description Using a reference ID list, update patch data for matching subjects
# @param data a list containing 1 or more dictionaries
# @param ids a list of unique reference IDs
# @param patch string containing a new patch ID
# @return a list containing one or more dictionaries
def update_rd3_subject(data, ids, patch):
    out = []
    for d in data:
        tmp = {}
        if d['subjectID'] in ids:
            tmp['id'] = d.get('id')
            tmp['subjectID'] = d.get('subjectID')
            tmp['patch'] = d.get('patch')
            tmp['organisation'] = d.get('organisation', {}).get('identifier')
            tmp['ERN'] = d.get('ERN', {}).get('identifier')
            if len(tmp['patch']) >= 1:
                tmp_patches = flatten_attr(tmp['patch'], 'id')
                tmp['patch'] = ','.join(map(str, tmp_patches))
                if not (patch in tmp_patches):
                    tmp['patch'] = tmp['patch'] + "," + patch
            else:
                tmp['patch'] = patch
            out.append(tmp)
    return out


# @title Update Processed Experiment Data
# @description Set the value of processed Freeze1 and Freeze2 data to True
# @param data input dataset containing a list of dictionaries
# @return string containing data request response
def update_processed_experiment_data(data):
    update = []
    for d in data:
        update.append({'molgenis_id': d.get('molgenis_id'), 'processed': True})
    rd3.batch_update_one_attr(
        entity='rd3_portal_novelomics_experiment',
        attr = 'processed',
        values = update
    )

# @title Update Processed Shipment Staging Table
# @description For Freeze1- & 2 cases, update the processed attribute to True
# @param metadata list containing shipmeny metadata
# @param data list of freeze IDs to update
def update_processed_shipment_data(metadata, data):
    records = select_dict(
        data=find_dict(data=metadata,attr='participant_subject',value=data),
        keys=['molgenis_id']
    )
    update = []
    for r in records:
        update.append({'molgenis_id': r.get('molgenis_id'), 'processed': True})
    rd3.batch_update_one_attr(
        entity='rd3_portal_novelomics_shipment',
        attr='processed',
        values=update
    )


# @title Merge Afilliation Data
# @description merge ERN and organisation metadata with main dataset
# @param data main dataset
# @param metadata reference dataset containing affiliation metadata
# @return list of dictionaries
def merge_affiliation_data(data, metadata):
    for dat in data:
        result = find_dict(metadata, 'subjectID', dat['subject_id'])
        if len(result):
            dat['organisation'] = result[0]['organisation']
            dat['ERN'] = result[0]['ERN']
    return data

# set client config
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
    },
    'attribs': {
        'subject': 'id,subjectID,patch,organisation,ERN'
    }
}

# init session
rd3 = molgenis_extra(url=api['host'][env], token=api['token'][env])


# fetch data
experiment = rd3.get('rd3_portal_novelomics_experiment', q='processed==false',batch_size=10000)
metadata = rd3.get('rd3_portal_novelomics_shipment', q='processed==false', batch_size=10000)
freeze1_subjects = rd3.get('rd3_freeze1_subject',attributes= api['attribs']['subject'],batch_size=10000)
freeze2_subjects = rd3.get('rd3_freeze2_subject',attributes= api['attribs']['subject'],batch_size=10000)

# flatten subject IDs
freeze1_ids = flatten_attr(freeze1_subjects, 'subjectID')
freeze2_ids = flatten_attr(freeze2_subjects, 'subjectID')

# Update IDs: using the shipment metadata, merge patientID using sample and experiment ID.
# print('Processings IDs where outdated...')
# for index,expr in enumerate(experiment):
#     result = list(filter(lambda x: expr['sample_id'] in x['sample_id'], metadata))
#     if len(result):
#         if (expr['project_experiment_dataset_id'] == result[0]['solve_rd_experiment_id']) and not (expr['subject_id'] == result[0]['participant_subject']):
#             print('Updated participant ID from "',expr['subject_id'],'" to "', result[0]['participant_subject'], '"')
#             experiment[index]['subject_id'] = result[0]['participant_subject']


# Triage all records from the staging area. Use subject ID to determine if the
# record belongs in Freeze1 or Freeze2. We don't need to do anything with new cases
# as they don't exist in Freeze1 or 2. It's best to leave them for now until they
# are released in a freeze.
print('Triaging records...')
rd3_freeze1 = []
rd3_freeze2 = []
rd3_new = []
for d in experiment:
    if not d.get('processed'):
        if d.get('subject_id') in freeze1_ids:
            rd3_freeze1.append(d)
        elif d.get('subject_id') in freeze2_ids:
            rd3_freeze2.append(d)
        else:
            rd3_new.append(d)

print('Freeze 1 records to process:', len(rd3_freeze1))
print('Freeze 2 records to process:', len(rd3_freeze2))
print('New records:', len(rd3_new))

# validate triage
if sum(map(len, [rd3_freeze1, rd3_freeze2, rd3_new])) == len(experiment):
    print('Triaged all records')
else:
    print('Unable to triage all data')

# get unique IDs for freeze1 and freeze2 patients
rd3_freeze1_ids = flatten_attr(rd3_freeze1, 'subject_id', distinct=True)
rd3_freeze2_ids = flatten_attr(rd3_freeze2, 'subject_id', distinct=True)

#//////////////////////////////////////////////////////////////////////////////

# set import flags
should_import_freeze_1 = False
should_import_freeze_2 = False

# process freeze1 data
if len(rd3_freeze1):
    print('Mapping new freeze1 data...')
    rd3_freeze1 = merge_affiliation_data(
        data=rd3_freeze1,
        metadata=freeze1_subjects
    )
    rd3_freeze1_file = map_rd3_files(
        data=rd3_freeze1,
        sample_id_suffix="_novelomics_original",
        patch='novelomics_original'
    )
    rd3_freeze1_labinfo = map_rd3_labinfo(
        data=rd3_freeze1,
        id_suffix = '_novelomics_original',
        sample_id_suffix = '_novelomics_original',
        patch = 'novelomics_original',
        distinct = True
    )
    rd3_freeze1_sample = map_rd3_samples(
        data=rd3_freeze1,
        id_suffix='_novelomics_original',
        subject_suffix="_original",
        patch='novelomics_original',
        distinct = True
    )
    rd3_freeze1_subject = update_rd3_subject(
        data = freeze1_subjects,
        ids = rd3_freeze1_ids,
        patch = 'novelomics_original'
    )
    should_import_freeze_1 = True
    print('Mapped Freeze1 Files:', len(rd3_freeze1_file))
    print('Mapped Freeze1 Labinfo:', len(rd3_freeze1_labinfo))
    print('Mapped Freeze1 Samples:', len(rd3_freeze1_sample))
    print('Updated Freeze1 Subjects:', len(rd3_freeze1_subject))
else:
    print('No new freeze1 records to map')

# process freeze2 data
if len(rd3_freeze2):
    print('Mapping new freeze2 data...')
    rd3_freeze2 = merge_affiliation_data(
        data=rd3_freeze2,
        metadata=freeze2_subjects
    )
    rd3_freeze2_file = map_rd3_files(
        data=rd3_freeze2,
        sample_id_suffix='_novelomics_original',
        patch='novelomics_original'
    )
    rd3_freeze2_labinfo = map_rd3_labinfo(
        data=rd3_freeze2,
        id_suffix = '_novelomics_original',
        sample_id_suffix = '_novelomics_original',
        patch = 'novelomics_original',
        distinct = True
    )
    rd3_freeze2_sample = map_rd3_samples(
        data=rd3_freeze2,
        id_suffix='_novelomics_original',
        subject_suffix="_original",
        patch='novelomics_original',
        distinct = True
    )
    rd3_freeze2_subject = update_rd3_subject(
        data = freeze2_subjects,
        ids = rd3_freeze2_ids,
        patch = 'novelomics_original'
    )
    should_import_freeze_2 = True
    print('Mapped Freeze2 Files:', len(rd3_freeze2_file))
    print('Mapped Freeze2 Labinfo:', len(rd3_freeze2_labinfo))
    print('Mapped Freeze2 Samples:', len(rd3_freeze2_sample))
    print('Updated Freeze2 Subjects:', len(rd3_freeze2_subject))
else:
    print('No new freeze2 records to map')


#//////////////////////////////////////////////////////////////////////////////

# import freeze1 data if applicable
if should_import_freeze_1:
    print('Importing Freeze1 Subjects and Subject Info data...')
    rd3.batch_update_one_attr(
        entity= 'rd3_freeze1_subject',
        attr='patch',
        values=select_dict(rd3_freeze1_subject, ['id','patch'])
    )
    rd3.batch_update_one_attr(
        entity='rd3_freeze1_subjectinfo',
        attr='patch',
        values = select_dict(rd3_freeze1_subject, ['id', 'patch'])
    )
    print('Importing Freeze 1 Samples...')
    rd3.update_table(data=rd3_freeze1_sample,entity='rd3_freeze1_sample')
    print('Importing Freeze 1 Labinfo...')
    rd3.update_table(data=rd3_freeze1_labinfo, entity='rd3_freeze1_labinfo_novelomics')
    print('Importing Freeze1 Files...')
    rd3.update_table(data=rd3_freeze1_file,entity='rd3_freeze1_file')
    print('Updating Portal Tables: setting `processed to `True`')
    update_processed_experiment_data(data=rd3_freeze1)
    update_processed_shipment_data(metadata=metadata,data=rd3_freeze1_ids)


# import freeze2 data if applicable
if should_import_freeze_2:
    print('Importing Freeze2 Subjects and Subject Info data...')
    rd3.batch_update_one_attr(
        entity='rd3_freeze2_subject',
        attr='patch',
        values=select_dict(rd3_freeze2_subject, ['id','patch'])
    )
    rd3.batch_update_one_attr(
        entity='rd3_freeze2_subjectinfo',
        attr='patch',
        values=select_dict(rd3_freeze2_subject, ['id','patch'])
    )
    print('Importing Freeze 2 Samples...')
    rd3.update_table(data=rd3_freeze2_sample,entity='rd3_freeze2_sample')
    print('Importing Freeze 2 Labinfo...')
    rd3.update_table(data=rd3_freeze2_labinfo,entity='rd3_freeze2_labinfo_novelomics')
    print('Importing Freeze 2 Files...')
    rd3.update_table(data=rd3_freeze2_file,entity='rd3_freeze2_file')
    print('Updating Portal Tables: setting `processed to `True`')
    update_processed_experiment_data(data=rd3_freeze2)
    update_processed_shipment_data(metadata=metadata,data=rd3_freeze2_ids)

# FIN!
print('Done!! :-)')

