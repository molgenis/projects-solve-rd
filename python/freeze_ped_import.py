#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_ped_import.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-02
#' MODIFIED: 2022-01-18
#' PURPOSE: extract metadata from PED files and import into Molgenis
#' STATUS: stable
#' PACKAGES: *see imports below*
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import python.rd3tools as rd3tools
from dotenv import load_dotenv
from os import environ, path
from datetime import datetime
import pandas as pd
import re

# set vars
load_dotenv()
currentReleaseType='patch' # or 'release'
currentFreeze = 'freeze1'
currentPatch = 'patch3'
host = environ['MOLGENIS_HOST_ACC']
token = environ['MOLGENIS_TOKEN_ACC']
# host = environ['MOLGENIS_HOST_PROD']
# token = environ['MOLGENIS_TOKEN_PROD']

# build entity IDs and paths based on current freeze and patch
paths = rd3tools.build_rd3_paths(
    freeze = currentFreeze,
    patch = currentPatch,
    baseFilePath = environ['CLUSTER_BASE']
)

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~ 
# Start Molgenis Session and Pull Required Data
#
# For processing the PED files, we will need the following attributes from the
# RD3 `rd3_freeze[x]_subject` where `[x]` is the freeze that the new PED files
# are tied to (e.g., `rd3_freeze2_subject`).
#
#   - `id`: the molgenis row ID; a concatenation of subject ID and release
#   - `subjectID`: RD3 P number
#   - `sex`: patient's sex
#   - `fid`: family ID
#
# It isn't necessary to run extensive checks that compare PED file data with
# the values that are in RD3 as PED files should be considered the most
# up to date.

# init session
rd3 = rd3tools.molgenis(url = host, token = token)

# pull subject metadata for the current freeze
freeze_subject_metadata = rd3.get(
    entity = paths['rd3_subjects'],
    # q = 'patch=freeze1_patch1',
    attributes='id,subjectID,sex1,fid',
    batch_size = 10000
)

# flatten subjectIDs for faster comparison later on
subject_ids = rd3tools.flatten_attr(data=freeze_subject_metadata, attr='subjectID')

# In addition to subject metadata, it is import to pull file metadata to identify
# which have changed and should be processed. We will pull the following
# attributes:
#
#   - `EGA`: the EGA file ID
#   - `name`: the full name of the file
#   - `md5`: checksum
#

# pull file metadata
# freeze_files_metadata = rd3.get(
#     entity = paths['rd3_file'],
#     attributes = 'EGA,name,md5',
#     q = 'typeFile==ped'
# )

# For each record retrieved, add a column `filename` that receives the basename
# of the full file name.
# for freeze_file in freeze_files_metadata:
#     freeze_file['filename'] = path.basename(freeze_file['name'])
#     freeze_file['filename'] = re.split(r'(\.[0-9]{11,}\.cip)', freeze_file['filename'])[0]

#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Process PED files and extract content
#
# Compile a lost of all available PED files on the cluster and process each
# file individually. The processing step parses and restructures the contents
# into RD3 terminology. PED files are essentially tab separated text files
# that have the following columns. Each file is a family of a RD3 subject where
# each line is an individual. Files should have at least one row (the subject).
# Other lines contain data on family members if data was supplied by the data
# submitter.
#
# PED files contain six columns. 
#
#   0. Family ID
#   1. Individual ID
#   2. Paternal ID
#   3. Maternal ID
#   4. Sex: options are "1" (M), "2" (F), or "OTHER" (U)
#   5. Affected Status: "-9" (None), "0" (None), "1" (False), or "2" (True)
#
# See https://zzz.bwh.harvard.edu/plink/data.shtml for more information.
#
# By default all rows are given an "upload" status. If any of the validation
# steps fail, the upload status is set to FALSE. See the script `rd3tools.py`
# for more information on how values are validated.

# Create a list of all PED files that are stored at a specific path on the
# cluster. Make sure all non-PED files are removed.
available_ped_files_raw = rd3tools.cluster_list_files(path = paths['cluster_ped'])
available_ped_files = []
for file in available_ped_files_raw:
    if re.search(r'(\.ped|\.ped.cip)$', file.get('filename')):
        available_ped_files.append(file)

rd3tools.status_msg(f'Processing {len(available_ped_files)} PED files')

# ~ 2a ~
# For each file, extract contents, validate data and transform into RD3
# terminology.
raw_ped_data = []
starttime = datetime.utcnow().strftime('%H:%M:%S.%f')[:-4]
for pedfile in available_ped_files:
    rd3tools.status_msg('Processing file {}'.format(pedfile['filename']))
    # result = rd3tools.find_dict(
    #     data = freeze_files_metadata,
    #     attr = 'filename',
    #     value = pedfile['filename'] + '.cip'
    # )
    # should_process = False
    # if result:
    #     rd3tools.status_msg('Evaluating checksums with file metadata')
    #     md5_result = rd3tools.cluster_run_checksum(path = pedfile['filepath'])
    #     if result[0]['md5'] != md5_result:
    #         rd3tools.status_msg('Checksum differs. Data will be processed')
    #         should_process = True
    #     else:
    #         rd3tools.status_msg('Checksum is the same. Moving to next file')
    #         continue
    # else:
    #     rd3tools.status_msg('File {} does not exist in RD3'.format(pedfile['filename']))
    # if should_process:
        # should_process = False
    rd3tools.status_msg('Parsing file')
    contents = rd3tools.cluster_read_file(path = pedfile['filepath'])
    data = rd3tools.ped_extract_contents(
        contents = contents,
        ids = subject_ids,
        filename = file['filename']
    )
    raw_ped_data.append(data[0])

# print summary
endtime = datetime.utcnow().strftime('%H:%M:%S.%f')[:-4]
rd3tools.status_msg(
    "Processed PED files in {}"
    .format(
        datetime.strptime(endtime,'%H:%M:%S.%f') - datetime.strptime(starttime, '%H:%M:%S.%f')
    )
)

# file data and prepare for import
rd3tools.status_msg('Removing cases where `upload=False`')
pedigree_data = []
for pf in raw_ped_data:
    if pf['upload']:  # this may need to be adjusted in the future
        pedigree_data.append(pf)
        
#//////////////////////////////////////

# ~ 2b ~
# Validate new PED data against RD3 data
#
# For all records where "upload" is TRUE, validate relevant attributes against
# values that exist in RD3. At the moment, we are validating sex codes. It is
# unlikely that these values will change since the initial release, but it is
# good to confirm the values in case there was an issue somewhere in the data
# processing, file generation, or somewhere else. 

patient_sex_codes_validation = []
for d in pedigree_data:
    q = rd3tools.find_dict(
        data = freeze_subject_metadata,
        attr = 'subjectID',
        value = d['subjectID']
    )[0]
    if q:
        d['id'] = q['id']
        if ('sex1' in d) and ('sex1' in q):
            if d['sex1'] == q['sex1']['identifier']:
                rd3tools.status_msg('Sex codes are identical')
            if d['sex1'] != q['sex1']['identifier']:
                rd3tools.status_msg(
                    'Sex codes for {} do not match: PED={}, RD3={}'
                    .format(d['subjectID'], q['sex1']['identifier'], d['sex1'])
                )
                patient_sex_codes_validation.append({
                    'subjectID': d['subjectID'],
                    'rd3_freeze_sex1': q['sex1']['identifier'],
                    'ped_file_sex1': d['sex1']
                })
    else:
        rd3tools.status_msg('Error: no match for {} found'.format(d['subjectID']))

# Warn if cases exist, manually verify each one and correct where applicable
if patient_sex_codes_validation:
    raise SystemError('Inconsistencies detected in sex codes. Manually verify each case')

# If needed, write data to file
# import pandas as pd
# pd.DataFrame(patient_sex_codes_validation).to_csv(f'data/patient_{currentFreeze}-{currentPatch}_sex_codes_review.csv',index=False)
del d, q

#//////////////////////////////////////

# ~ 2c ~
# Validate and Merge RD3 data with new PED data
#
# Before the data can be imported into RD3, it is import to confirm that all IDs
# exist in RD3. Since ID columns are referenced by other columns, all unknown
# IDs will throw an error. The following step will examine subject ID, 
# maternal ID, and paternal ID. IDs that are listed in the PED files will be
# used to search the `rd3_freeze[x]_subject` table. If the ID exists, the ID
# will be added to the new record.

for d in pedigree_data:
    rd3tools.status_msg('Updated IDs for {}'.format(d['subjectID']))
    subject_q = rd3tools.find_dict(
        data = freeze_subject_metadata,
        attr = 'subjectID',
        value = d['subjectID']
    )[0]
    # Merge corresponding table key
    if subject_q:
        d['id'] = subject_q['id']
    else:
        rd3tools.status_msg('Error: no match for {} found in RD3'.format(d['subjectID']))
    # Does the maternal ID exist?
    if d['mid']:
        mid_q = rd3tools.find_dict(
            data = freeze_subject_metadata,
            attr = 'subjectID',
            value = d['mid']
        )[0]
        if mid_q:
            d['mid'] = mid_q['id']
        else:
            rd3tools.status_msg('given maternal ID {} does not exist in RD3'.format(d['mid']))
    # Does the paternal ID exist?
    if d['pid']:
        pid_q = rd3tools.find_dict(
            data = freeze_subject_metadata,
            attr = 'subjectID',
            value = d['pid']
        )[0]
        if pid_q:
            d['pid'] = pid_q['id']
        else:
            rd3tools.status_msg('given paternal ID {} does not exist in RD3'.format(d['pid']))      
    # Do the family IDs match? Or are there multiple FIDs? Merge RD3 value
    if d.get('fid') and subject_q.get('fid'):
        subject_fids = subject_q['fid'].split(', ')
        if d['fid'] in subject_fids:
            if len(subject_fids) > 1:
                rd3tools.status_msg('Multiple family IDs found. Merging...')
                d['fid'] = subject_q['fid']
              
              
# for d in pedigree_data:
#     q = rd3tools.find_dict(
#         data = pedigree_data, 
#         attr = 'id',
#         value = d['id']
#     )
#     if q:
#         for record in q:
#             if record['fid'] != d['fid']:
#                 rd3tools.status_msg('Found multiple FIDs. Merging...')
#                 d['fid'] = f"{d['fid']}, {record['fid']}"  

# ~ 2d ~
# Save data for import
#
# It is much easier to write the data to file, and then import into RD3. This
# way you can switch from ACC to PROD much easier or you can focus on the data
# import without having to reprocess PED data. This saves a lot of time!

filename = f'data/rd3_{currentFreeze}-{currentPatch}_ped_data.csv'
pd.DataFrame(pedigree_data).to_csv(filename, index=False)


#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import Data
#
# Importing data into RD3 is rather straightforward. All data is imported into
# subjects table of the current freeze.
#

# import data from file (if needed)
filename = f'data/rd3_{currentFreeze}-{currentPatch}_ped_data.csv'
pedigree_data = pd.read_csv(filename)


# prepare objects to import
upload_fid = pedigree_data[['id','fid']].dropna().to_dict('records')
upload_mid = pedigree_data[['id','mid']].dropna().to_dict('records')
upload_pid = pedigree_data[['id','pid']].dropna().to_dict('records')
upload_clinical = pedigree_data[['id','clinical_status']].dropna().to_dict('records')

print(f'Updating FID for {len(upload_fid)} records')
print(f'Updating MID for {len(upload_mid)} records')
print(f'Updating PID for {len(upload_pid)} records')
print(f'Updating Clinical Status for {len(upload_clinical)} records')


# ~ 3b ~
# Import
#
# Sometimes the `batch_update_one_attr` works and sometimes it doesn't. You may
# encounter a lot of timeout errors and sometimes you won't. I have no idea why
# but it is possible to update the values the good old fashion way.

# import into the subjects table of the current freeze
# rd3.batch_update_one_attr(entity=paths['rd3_subjects'], attr='fid', values=upload_fid)
# rd3.batch_update_one_attr(entity=paths['rd3_subjects'], attr='mid', values=upload_mid)
# rd3.batch_update_one_attr(entity=paths['rd3_subjects'], attr='pid', values=upload_pid)
# rd3.batch_update_one_attr(entity=paths['rd3_subjects'], attr='clinical_status', values=upload_clinical)

# otherwise, import the old way.

# update fid
for index,el in enumerate(upload_fid):
    rd3tools.status_msg('{} Updating FID for {}'.format(index,el['id']))
    rd3.update_one(
        entity = paths['rd3_subjects'],
        id_ = el['id'],
        attr = 'fid',
        value = el['fid']
    )
    
# update mid
for index,el in enumerate(upload_mid):
    rd3tools.status_msg('{} Updating MID for {}'.format(index,el['id']))
    rd3.update_one(
        entity = paths['rd3_subjects'],
        id_ = el['id'],
        attr = 'mid',
        value = el['mid']
    )

# update pid
for index,el in enumerate(upload_pid):
    rd3tools.status_msg('{} Updating PID for {}'.format(index,el['id']))
    rd3.update_one(
        entity = paths['rd3_subjects'],
        id_ = el['id'],
        attr = 'pid',
        value = el['pid']
    )


# update clinical status
for index,el in enumerate(upload_clinical[840:]):
    rd3tools.status_msg('{} Updating Clinical for {}'.format(index,el['id']))
    rd3.update_one(
        entity = paths['rd3_subjects'],
        id_ = el['id'],
        attr = 'clinical_status',
        value = el['clinical_status']
    )
