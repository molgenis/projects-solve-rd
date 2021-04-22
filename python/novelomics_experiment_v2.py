# //////////////////////////////////////////////////////////////////////////////
# FILE: novelomics_shipment.py
# AUTHOR: David Ruvolo
# CREATED: 2021-04-15
# MODIFIED: 2021-04-22
# PURPOSE: process novel omics into Solve RD
# STATUS: working
# DEPENDENCIES: NA
# COMMENTS: NA
# //////////////////////////////////////////////////////////////////////////////

import json
import molgenis.client as molgenis

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

# @title map new freeze files
# @description map file metadata to target EMX format
# @param data input data
# @param patch SolveRD3 data release
# @return list of dictionaries
def map_rd3_files(data, patch):
    out = []
    for d in data:
        tmp = {}
        tmp['EGA'] = d.get('file_ega_id')
        tmp['EGApath'] = d.get('file_path')
        tmp['name'] = d.get('file_name')
        tmp['md5'] = d.get('unencrypted_md5_checksum')
        tmp['typeFile'] = d.get('file_type')
        tmp['filegroupID'] = d.get('file_group_id')
        tmp['samplesID'] = d.get('sample_id')
        tmp['experimentID'] = d.get('project_experiment_dataset_id')
        tmp['run_ega_id'] = d.get('run_ega_id')
        tmp['experiment_ega_id'] = d.get('experiment_ega_id')
        tmp['patch'] = patch
        out.append(tmp)
    return out


# @title map new labinfo
# @param data input data
# @param id_suffix content to append to ID, e.g., "_original"
# @param patch SolveRD3 data release
# @param distinct if True, distinct dictionaries will be returned
# @return list of dictionaries
def map_rd3_labinfo(data, id_suffix, patch, distinct=False):
    out = []
    for d in data:
        tmp = {}
        tmp['id'] = d.get('project_experiment_dataset_id') + id_suffix
        tmp['experimentID'] = d.get('project_experiment_dataset_id')
        tmp['sampleID'] = d.get('sample_id')
        tmp['capture'] = d.get('library_selection')
        tmp['libraryType'] = d.get('library_source').title()
        tmp['library'] = None
        if d.get('library_layout') == 'PAIRED':
            tmp['library'] = 1
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
# @param patch SolveRD3 data release
# @param distinct if True, distinct dictionaries will be returned
# @return list of dictionaries
def map_rd3_samples(data, id_suffix, patch, distinct=False):
    out = []
    for d in data:
        tmp = {}
        tmp['id'] = d.get('subject_id') + id_suffix
        tmp['subjectID'] = d.get('subject_id')
        tmp['sampleID'] = d.get('sample_id')
        tmp['tissueType'] = d.get('tissue_type')
        tmp['materialType'] = d.get('sample_type')
        tmp['patch'] = patch
        out.append(tmp)
    if distinct:
        return list(distinct_dict(out, lambda x: ( x['id'], x['sampleID'] ) ))
    else:
        return out

# @title Map new RD3 records
# @description for new records, map them to the intermediate table
# @param data input dataset to process
# @param id_suffix content to append to ID, e.g., "_original"
# @return a list of dictionaries
def map_rd3_new_records(data, id_suffix):
    out = []
    for d in data:
        tmp = {}
        tmp['id'] = d.get('subject_id') + id_suffix
        tmp['EGA'] = d.get('file_ega_id')
        tmp['EGApath'] = d.get('file_path')
        tmp['name'] = d.get('file_name')
        tmp['md5'] = d.get('unencrypted_md5_checksum')
        tmp['typeFile'] = d.get('file_type')
        tmp['filegroupID'] = d.get('file_group_id')
        tmp['batchID'] = d.get('batch_id')
        tmp['experimentID'] = d.get('project_experiment_dataset_id')
        tmp['sampleID'] = d.get('sample_id')
        tmp['subjectID'] = d.get('subject_id')
        tmp['sequencingCentre'] = d.get('sequencing_center')
        tmp['sequencer'] = d.get('platform_model')
        tmp['libraryType'] = d.get('library_source').title()
        tmp['capture'] = d.get('library_selection')
        tmp['seqType'] = d.get('library_strategy')
        tmp['libraryLayout'] = d.get('library_layout')
        tmp['tissueType'] = d.get('tissue_type')
        tmp['materialType'] = d.get('sample_type')
        tmp['project_batch_id'] = d.get('project_batch_id')
        tmp['run_ega_id'] = d.get('run_ega_id')
        tmp['experiment_ega_id'] = d.get('experiment_ega_id')
        out.append(tmp)
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
            if len(tmp['patch']) >= 1:
                tmp_patches = flatten_attr(tmp['patch'], 'id')
                tmp['patch'] = ','.join(map(str, tmp_patches)) + "," + patch
            else:
                tmp['patch'] = patch
            out.append(tmp)
    return out

# set tokens and host
# token = '${molgenisToken}'
# host = 'https://solve-rd.gcc.rug.nl/api/'

token = 'F4dLD+nbEKrWCLfwzpFvxQvvoFsGzgPHf3fgjCOPtD0='
host = 'https://solve-rd-acc.gcc.rug.nl/api/'
rd3 = molgenis_extra(url=host, token=token)


# fetch data
data = rd3.get('rd3_portal_novelomics_experiment', batch_size=1000)
freeze1_subjects = rd3.get(
    entity='rd3_freeze1_subject',
    attributes='id,subjectID,patch',
    batch_size=10000
)
freeze2_subjects = rd3.get(
    entity='rd3_freeze2_subject',
    attributes='id,subjectID,patch',
    batch_size=10000
)
rd3_filetypes = rd3.get('rd3_typeFile')
rd3_seqtypes = rd3.get('rd3_seqType')


# flatten subject IDs
freeze1_ids = flatten_attr(freeze1_subjects, 'subjectID')
freeze2_ids = flatten_attr(freeze2_subjects, 'subjectID')

# process file types, if there are new records add to lookup table
print('References: Looking for new filetypes...')
novelomics_filetypes = flatten_attr(data, 'file_type', distinct=True)
new_rd3_filetypes = identify_new_lookups(rd3_filetypes, 'identifier', novelomics_filetypes)
if len(new_rd3_filetypes):
    print('Identified new file type references:', len(new_rd3_filetypes))
    filetypes_to_upload = []
    for filetype in new_rd3_filetypes:
        filetypes_to_upload.append({'identifier': filetype.get('id'), 'label': filetype.get('label') })
    print('Importing new file type lookups. (update labels manually)')
    # rd3.update_table(data=filetypes_to_upload, entity='rd3_typeFile')
else:
    print('No new filetypes')

# process sequenceTypes
print('References: Looking for new sequencing types...')
novelomics_seqtypes = flatten_attr(data, 'experiment_type', distinct=True)
new_rd3_seqtypes = identify_new_lookups(rd3_seqtypes, 'identifier', novelomics_seqtypes)
if len(new_rd3_seqtypes):
    print('Identified new seqType references:', len(new_rd3_seqtypes))
    seqtypes_to_upload = []
    for seqtype in new_rd3_seqtypes:
        seqtypes_to_upload.append({'identifier': seqtype.get('id'), 'label': seqtype.get('label')})
    print('Importing new seqType lookups. (Update labels manually)')
    # rd3.update_table(data=seqtypes_to_upload, entity='rd3_seqType')
else:
    print('No new sequencing types')

################################ PROCESS NEW LIBRARY TYPE HERE ################################


# Triage all records from the staging area. Use subject ID to determine if the
# record belongs in Freeze1 or Freeze2
print('Triaging records...')
rd3_freeze1 = []
rd3_freeze2 = []
rd3_new = []

for d in data:
    if d.get('subject_id') in freeze1_ids:
        rd3_freeze1.append(d)
    elif d.get('subject_id') in freeze2_ids:
        rd3_freeze2.append(d)
    else:
        rd3_new.append(d)

print('Freeze 1 records to process:', len(rd3_freeze1))
print('Freeze 2 records to process:', len(rd3_freeze2))
print('New RD3 records to process:', len(rd3_new))

# validate triage
if sum(map(len, [rd3_freeze1, rd3_freeze2, rd3_new])) != len(data):
    print('Unable to process all data')
else:
    print('Processed all records')

# push new directly into table
if len(rd3_new):
    print('Importing non-Freeze cases into `rd3_novelomics-experiment`')
    # rd3.update_table(data=rd3_new,entity='rd3_novelomics-experiment')

# get unique IDs for freeze1 and freeze2 patients
rd3_freeze1_ids = flatten_attr(rd3_freeze1, 'subject_id', distinct=True)
rd3_freeze2_ids = flatten_attr(rd3_freeze2, 'subject_id', distinct=True)

# process freeze1 data
if len(rd3_freeze1):
    print('Mapping new freeze1 data...')
    rd3_freeze1_file = map_rd3_files(
        data=rd3_freeze1,
        patch='novelomics_original'
    )
    rd3_freeze1_labinfo = map_rd3_labinfo(
        data=rd3_freeze1,
        id_suffix = '_novelomics_original',
        patch = 'novelomics_original',
        distinct = True
    )
    rd3_freeze1_sample = map_rd3_samples(
        data=rd3_freeze1,
        id_suffix='_novelomics_original',
        patch='novelomics_original',
        distinct = True
    )
    rd3_freeze1_subject = update_rd3_subject(
        data = freeze1_subjects,
        ids = rd3_freeze1_ids,
        patch = 'novelomics_original'
    )
    print('Mapped Freeze1 Files:', len(rd3_freeze1_file))
    print('Mapped Freeze1 Labinfo:', len(rd3_freeze1_labinfo))
    print('Mapped Freeze1 Samples:', len(rd3_freeze1_sample))
    print('Updated Freeze1 Subjects:', len(rd3_freeze1_subject))
    print('Importing Freeze1 Subjects...')
    for f1_subject in rd3_freeze1_subject:
        rd3.update_one(
            entity='rd3_freeze1_subject',
            id_=f1_subject['id'],
            attr='patch',
            value=f1_subject['patch']
        )
    print('Importing Freeze 1 Samples...')
    rd3.update_table(data=rd3_freeze1_sample,entity='rd3_freeze1_sample')
    print('Importing Freeze 1 Labinfo...')
    rd3.update_table(data=rd3_freeze1_labinfo, entity='rd3_freeze1_labinfo_noveomics')
    print('Importing Freeze1 Files...')
    rd3.update_table(data=rd3_freeze1_file,entity='rd3_freeze1_file')

else:
    print('No new freeze1 records to map')

# process freeze2 data
if len(rd3_freeze2):
    print('Mapping new freeze2 data...')
    rd3_freeze2_file = map_rd3_files(
        data=rd3_freeze2,
        patch='novelomics_original'
    )
    rd3_freeze2_labinfo = map_rd3_labinfo(
        data=rd3_freeze2,
        id_suffix = '_novelomics_original',
        patch ='novelomics_original',
        distinct = True
    )
    rd3_freeze2_sample = map_rd3_samples(
        data=rd3_freeze2,
        id_suffix='_novelomics_original',
        patch='novelomics_original',
        distinct = True
    )
    rd3_freeze2_subject = update_rd3_subject(
        data = freeze2_subjects,
        ids = rd3_freeze2_ids,
        patch = 'novelomics_original'
    )
    print('Mapped Freeze2 Files:', len(rd3_freeze2_file))
    print('Mapped Freeze2 Labinfo:', len(rd3_freeze2_labinfo))
    print('Mapped Freeze2 Samples:', len(rd3_freeze2_sample))
    print('Updated Freeze2 Subjects:', len(rd3_freeze2_subject))
    print('Importing Freeze 2 Subjects...')
    for f2_subject in rd3_freeze2_subject:
        rd3.update_one(
            entity='rd3_freeze2_subjects',
            id_=f2_subject['id'],
            attr='patch',
            value=f2_subject['patch']
        )
    print('Importing Freeze 2 Samples...')
    rd3.update_table(data=rd3_freeze2_sample,entity='rd3_freeze2_sample')
    print('Importing Freeze 2 Labinfo...')
    rd3.update_table(data=rd3_freeze2_labinfo,entity='rd3_freeze2_labinfo_novelomics')
    print('Importing Freeze 2 Files...')
    rd3.update_table(data=rd3_freeze2_file,entity='rd3_freeze2_file')
else:
    print('No new freeze2 records to map')

