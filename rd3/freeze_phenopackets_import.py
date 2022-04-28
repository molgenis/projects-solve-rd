#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_phenopackets_import.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-02
#' MODIFIED: 2022-01-12
#' PURPOSE: push phenopackets metadata into RD3
#' STATUS: stable
#' PACKAGES: os, json, requests, urlib.parse, molgenis.client, dotenv
#' COMMENTS: Run in the same folder as all phenopackets files
#'////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis import Molgenis
from rd3.utils.clustertools import clustertools
from rd3.utils.utils import (
    buildRd3Paths,
    phenotools,
    statusMsg
)
import python.rd3tools as rd3tools
from datatable import dt,f,count,as_type,fread,rbind,by
from dotenv import load_dotenv
from os import environ
from datetime import datetime
# import pandas as pd
import re

# set vars
load_dotenv()
currentReleaseType='patch' # or 'release'
currentFreeze = 'freeze1' # 'freeze2'
currentPatch = 'patch3' # 'patch1'
host = environ['MOLGENIS_HOST_ACC']
token = environ['MOLGENIS_TOKEN_ACC']
# host = environ['MOLGENIS_HOST_PROD']
# token = environ['MOLGENIS_TOKEN_PROD']

# build entity IDs and paths based on current freeze and patch
paths = buildRd3Paths(
    freeze = currentFreeze,
    patch = currentPatch,
    baseFilePath = environ['CLUSTER_BASE']
)

# def __unpack__phenotypicfeatures(phenotypicFeatures):
#     """Unpack Phenotypic Features
#     Extract `phenotypicFeatures` and separate into observed and unobserved
#     phenotypic codes
#     @param phenotypicFeatures : output from data['phenopacket']['phenotypicFeatures']
#     @return a dictionary with observed and unobserved phenotype codes
#     """
#     phenotype = []
#     hasNotPhenotype = []
#     for pheno in phenotypicFeatures:
#         if pheno:
#             hpo_id = re.sub(r'^(HP:)', 'HP_', pheno['type']['id'])
#             if not (hpo_id in phenotype) and not (hpo_id in hasNotPhenotype):
#                 if 'negated' in pheno:
#                     if pheno['negated']:
#                         hasNotPhenotype.append(hpo_id)
#                     if not pheno['negated']:
#                         phenotype.append(hpo_id)
#                 else:
#                     phenotype.append(hpo_id)
#     return {'phenotype': phenotype, 'hasNotPhenotype': hasNotPhenotype}

# def __unpack__diseases(data):
#     """Unpack Diseases
#     Extract disease IDs Unique ontologies: ['HP', 'Orphanet', 'HGNC', 'OMIM']
#     @param data : list of dictionaries from data['phenopacket']['diseases]
#     @return dict with list of diagnostic- and onset codes
#     """
#     ids_to_recode = {
#         #
#         # To add a new mapping, use the following format:
#         # 'INCORRECT_CODE' : {'old': 'INCORRECT_CODE', 'new': 'NEW_CODE'}
#         #
#         'MIM_159000': {'old':'MIM_159000','new':'MIM_609200'},
#         'MIM_159001': {'old':'MIM_159001','new':'MIM_181350'},
#         'MIM_607569': {'old':'MIM_607569','new':'MIM_603689'},
#         'ORDO_856': {'old': 'ORDO_856', 'new': ''},
#         'ORDO_ 104010': {'old': 'ORDO_ 104010', 'new':'ORDO_104010'}
#     }
#     dx = []  # diagnostic codes
#     ao = []  # onset codes
#     for d in data:
#         if 'term' in d:
#             if 'id' in d['term']:
#                 code1 = d['term']['id']
#                 if re.search(r'^((Orphanet:)|(ORDO:))', code1):
#                     code1 = re.sub(r'^((Orphanet:)|(ORDO:))', 'ORDO_', code1)
#                 if re.search(r'^((OMIM:)|(MIM:))', code1):
#                     code1 = re.sub(r'^((OMIM:)|(MIM:))', 'MIM_', code1)
#                 if code1 in ids_to_recode:
#                     code1 = ids_to_recode[code1]['new']
#                 if not (code1 in dx):
#                     dx.append(code1)
#         if 'classOfOnset' in d:
#             if 'id' in d['classOfOnset']:
#                 code2 = d['classOfOnset']['id']
#                 code2 = re.sub(r'^(HP:)', 'HP_', code2)
#                 if not (code2 in ao):
#                     ao.append(code2)
#     return {'dx': dx, 'ao': ao}

# def __recode__sex(value):
#     """Recorde Sex into RD3 terminology
#     @param value (str) : a value indicating sex
#     @return string and/or error message
#     """
#     mappings = {'female': 'F', 'male': 'M', 'unknown_sex': 'U', 'other_sex': 'UD'}
#     try:
#         return mappings[value.lower()]
#     except KeyError:
#         statusMsg('Unknown sex code: {}'.format(value))
#         return value
#     except AttributeError:
#         statusMsg('Unknown sex code: {}'.format(value))
#         return value

# def __recode__date(value):
#     """Format Date
#     @param value : string containing date to recode
#     @return a string containing yyyy-mm-dd
#     """
#     if value == '':
#         return value
#     return re.sub(r'(T00:00:00Z)', '', value).split('-')[0]


# Disease Code Mappings
# To add a new mapping, use the following format:
# 'INCORRECT_CODE' : 'NEW_CODE'
diseaseCodeMappings = {
    'MIM_159000': 'MIM_609200',
    'MIM_159001': 'MIM_181350',
    'MIM_607569': 'MIM_603689',
    'ORDO_856': '', # no known mapping
    'ORDO_ 104010': 'ORDO_104010'
}


# /////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Start Molgenis Session and Pull Required Data
# 
# In order to process new phenopacket files, it is important to compare values
# with new exiting RD3 metadata. This allows us to import the values that have
# changed rather than everything. Once the contents of the files have been
# processed and evaluated, we can import them into RD3. These values will be
# imported into the `subject` and `subjectinfo` tables. The attributes that
# are managed by this script are listed in the GET requests below.
#
rd3 = Molgenis(url = host, token=token)

# pull subject metadata for the current freeze
freeze=rd3.get(
    entity=paths['rd3_subjects'],
    attributes='id,subjectID,clinical_status,disease,phenotype,hasNotPhenotype,phenopacketsID,patch',
    batch_size=10000
)

# pull subjectinfo data
freeze_info=rd3.get(
    entity=paths['rd3_subjectinfo'],
    attributes='id,dateofBirth,ageOfOnset,patch',
    batch_size=10000
)

# extract subject IDs for later
# freeze_ids = rd3tools.flatten_attr(freeze, 'id')
freeze_ids=[row['id'] for row in freeze]

# pull HPO and disease codes, and then flatten
hpo_codes_raw = rd3.get(entity = 'rd3_phenotype', batch_size = 10000)
disease_codes_raw = rd3.get(entity = 'rd3_disease', batch_size = 10000)

# hpo_codes = rd3tools.flatten_attr(hpo_codes_raw, 'id')
# disease_codes = rd3tools.flatten_attr(disease_codes_raw, 'id')
hpo_codes=[row['id'] for row in hpo_codes_raw]
disease_codes=[row['id'] for row in disease_codes_raw]

del hpo_codes_raw, disease_codes_raw

#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Read and process phenopacket data
#
# Compile a list of all available Phenopacket files on the cluster and process
# each file individually. The processing step evaluates the contents of all
# nested objects in the json file, specifically we are interested in the
# 'phenopacket' property. In this property, we can extract metadata on the
# subject, phenotypicFeatures, and diseases.
#
# Values aren't compared to existing data in RD3 as files for all patients
# are regenerated. Therefore, we can processing and import the data directly.
# All invalid or unknown HPO-, disease-, and onset codes are flagged for
# manual review. These codes will either need to be added to the appropriate
# lookup table or need to be mapped to a new value. These cases should be
# reconciled before importing into RD3.
#
# Using the object `shouldProcess`, you can process certain data elements
# in the phenopacket files. This may be useful for if you need to refresh
# only disease codes or disease codes.
#

# create list of all available JSON files
# allFiles = rd3tools.cluster_list_files(path = paths['cluster_phenopacket'])
allFiles=clustertools.listFiles(path=paths['cluster_phenopacket'])
phenopacketFiles = [file for file in allFiles if re.search(r'(.json)$', file['filename'])]
statusMsg('Found', len(phenopacketFiles), 'phenopacket files')


# init loop params and objects
shouldProcess = {'subject': True, 'pheno': True, 'dx': True} # set props to process
hpo_codes_not_found = []
disease_codes_not_found = []
onset_codes_not_found = []
unavailable = []
phenopackets = []

# start file processing
statusMsg('Starting file processing...')
for file in phenopacketFiles:
    statusMsg('Processing file: {}'.format(file['filename']))
    # f = rd3tools.cluster_read_json(path = file['filepath'])
    json=clustertools.readJson(path=file['path'])
    result = {
        'id': json['phenopacket']['id'] + '_original',
        'dateofBirth': None,
        'sex1': None,
        'phenotype': None,
        'hasNotPhenotype': None,
        'disease': None,
        'ageOfOnset': None,
        'phenopacketsID': file['filename']
    }
    # make sure `subject` exists before processing subattributes
    if shouldProcess['subject']:
        if 'subject' in json['phenopacket']:
            if 'dateOfBirth' in json['phenopacket']['subject']:
                result['dateofBirth'] = phenotools.formatDate(
                    value=json['phenopacket']['subject']['dateOfBirth']
                )
            if 'sex' in json['phenopacket']['subject']:
                result['sex1'] = phenotools.recodeSexCodes(
                    value=json['phenopacket']['subject']['sex']
                )
    # make sure `phenotypicFeatures` exists
    if shouldProcess['pheno']:
        if 'phenotypicFeatures' in json['phenopacket']:
            phenotypic_features = phenotools.unpackPhenotypicFeatures(
                data = f['phenopacket']['phenotypicFeatures']
            )
            # triage HPO into `has` and `hasnot`
            patient_hpo_has = []
            patient_hpo_hasnot = []
            # ObservedPhenotypes: validate codes and isolate unknown cases
            for code in phenotypic_features['phenotype']:
                if code in hpo_codes:
                    patient_hpo_has.append(code)
                else:
                    statusMsg('Unknown HPO code:',str(code))
                    hpo_codes_not_found.append({'id': result['id'], 'hpo': code})
                    phenotypic_features['phenotype'].remove(code)
            del code
            # Unobserved Phenotypes: validate codes and isolate unknown cases
            for code in phenotypic_features['hasNotPhenotype']:
                if code in hpo_codes:
                    patient_hpo_hasnot.append(code)
                else:
                    statusMsg('Unknown HPO code:',str(code))
                    hpo_codes_not_found.append({'id': result['id'], 'hpo': code})
                    phenotypic_features['hasNotPhenotype'].remove(code)
            result['phenotype'] = ','.join(patient_hpo_has)
            result['hasNotPhenotype'] = ','.join(patient_hpo_hasnot)
            del code
    # make sure `diseases` exist first
    if shouldProcess['dx']:
        if 'diseases' in json['phenopacket']:
            diseases = phenotools.unpackDiseaseCodes(
                data=json['phenopacket']['diseases'],
                mappings=diseaseCodeMappings
            )
            patient_diseases_has = []
            # triage dx IDs: isolate invalid codes for review
            for code in diseases['diagnostic']:
                if code in disease_codes:
                    patient_diseases_has.append(code)
                else:
                    if code != '':
                        statusMsg('Unknown disease code:',str(code))
                        disease_codes_not_found.append({'id': result['id'], 'code': code})
                        diseases['dx'].remove(code)
            result['disease'] = ','.join(patient_diseases_has)
            # triage onset IDs (HPO): isolate invalid codes for review
            if len(diseases['onset']) > 0:
                valid_onset_codes = []
                for code in diseases['onset']:
                    if code in hpo_codes:
                        valid_onset_codes.append(code)
                    else:
                        statusMsg('Unknown onset code:',str(code))
                        onset_codes_not_found.append({'id': result['id'], 'onset': code})
                result['ageOfOnset'] = ','.join(valid_onset_codes)
    # make sure participant ID exists in current release
    if result['id'] in freeze_ids:
        phenopackets.append(result)
    else:
        statusMsg('Participant does not exist in current freeze')
        unavailable.append(result)

# print counts. If items 2-5 are 0 or empty, move to section 4. Otherwise,
# proceed to step 3.
print('Count of Phenopackets PatientIDs that exist in RD3:', len(phenopackets))
print('Count of Phenopackets PatientIDs that do notexist in RD3:', len(unavailable))
print('Count of unknown HPO codes:', len(hpo_codes_not_found))
print('Count of unknown disease codes:', len(disease_codes_not_found))
print('Count of unknown onset codes', len(onset_codes_not_found))

#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Manually Review Flagged Cases
#
# Before importing data into RD3, check the following objects.
#
#   - [ ] unavailable: subjects that do not exist in the current freeze
#   - [ ] HPO codes not found: HPO codes that aren't in RD3
#   - [ ] Disease codes not found: any OMIM codes that aren't in RD3
#
# For unavailable subjects, pull subject IDs from other releases to make sure
# the files aren't associated with the wrong freeze. If no explanation can be
# found, send a list of IDs and filenames to SolveRD.
#
# If there are no unknown HPO codes, disease codes, and subjects, then you may
# skip to the next section.
#

# ~ 3a ~ 
# Check unknown disease and onset codes
# Before you can import data, manually review each disease code that was flagged
# as unknown. In some cases, the code was changed or there is an issue with the
# value itself (e.g., extra space or character, unexpected format, etc.). For
# many of these issues, you can add a new mapping in the function located at the
# top of this script: `__unpack__diseases__`. In this function, add a new entry in
# the object `idsToRecode` (see function to see how to structure the mappings).
# For any code that is unknown contact the SolveRD data team or add them manually
# to the disease code reference table.

# view unique entries
list(set([row['code'] for row in disease_codes_not_found]))
list(set([row['onset'] for row in onset_codes_not_found]))
# rd3tools.flatten_attr(disease_codes_not_found, 'code', distinct = True)
# rd3tools.flatten_attr(onset_codes_not_found, 'onset', distinct = True)

# ~ 3b ~ 
# Investigate Unknown HPO Codes
# For each code listed in 'hpo_codes_not_found', investigate the code to
# determine how to reconcile the invalid code. In some instances, you can
# add a new record in `rd3_phenotype`, but some codes may have changed or
# are no longer used. If you are still unable to find an answer, contact
# the data provider.
#

# prep "HPO codes to verify" dataset
hpoCodes = dt.Frame(
    hpo_codes_not_found,
    types = {'id': str, 'hpo': str}
)[
    :, {
        'frequency': count(),
        'url': None,
        'status': 'not.found',
        'action': None
    },
    by(f.hpo)
]

# set url for HPO code if it exists
hpoCodes['url'] = dt.Frame([
    f'http://purl.obolibrary.org/obo/{d}'
    for d in hpoCodes['hpo'].to_list()[0]
])

# manually check each link and search for code. Either add the code or follow
# up with SolveRD project data coordinators.
rbind(fread('data/unknown_hpo_codes.csv'), hpoCodes).to_csv('data/unknown_hpo_codes.csv')


# ~ 3c ~ 
# Investivate 'unavailable' subjects (i.e., subjects that do not exist in 
# the current freeze). If subjects were added to 'unavailble', pull 
# subjectIDs from another freeze(s) to see these subjects exist in another freeze
#
otherFreezeIDs = rd3tools.flatten_attr(
    rd3.get('rd3_freeze1_subject',attributes='id',batch_size=10000),
    'id'
)
novelOmicsIDs = rd3tools.flatten_attr(
    rd3.get('rd3_novelomics_subject',attributes='id',batch_size=10000),
    'id'
)

unknownSubjects = dt.Frame(unavailable, types = {
    'id': str,
    'dateofBirth': str,
    'sex1': str,
    'phenotype': str,
    'hasNotPhenotype': str,
    'disease': str,
    'ageOfOnset': str,
    'phenopacketsID': str
})[:, {
    'id': f.id,
    'phenopacketsID': f.phenopacketsID,
    'release': f'{currentFreeze}_{currentPatch}',
    'foundInFreeze1': False,
    'foundInFreeze2': False,
    'foundInNovelOmics': False
}]

# Check to see if current ID exists in the other releases

unknownSubjects['foundInFreeze1'] = dt.Frame([
    d in otherFreezeIDs for d in unknownSubjects['id'].to_list()[0]
])

unknownSubjects['foundInNovelOmics'] = dt.Frame([
    d in novelOmicsIDs for d in unknownSubjects['id'].to_list()[0]
])

# remove the RD3 '_original' suffix from subject IDs (before sending)
unknownSubjects['id'] = dt.Frame([
    d.replace('_original','') for d in unknownSubjects['id'].to_list()[0]
])

unknownSubjects[:,
    dt.update(
        foundInFreeze1 = as_type(f.foundInFreeze1, str),
        foundInFreeze2 = as_type(f.foundInFreeze2, str),
        foundInNovelOmics = as_type(f.foundInNovelOmics, str),
    )
]

unknownSubjects.to_csv('data/rd3_freeze1_patch3_missing_subjects.csv')

# Merge files if applicable
# allUnknownSubjects = rbind(
#     fread('data/rd3_freeze1_patch3_missing_subjects.csv'),
#     fread('data/rd3_freeze2_patch1_missing_subjects.csv'),
# )
#
# allUnknownSubjects[
#     (f.foundInFreeze1 == False) &
#     (f.foundInFreeze2 == False) &
#     (f.foundInNovelOmics == False),
#     :
# ]
#
# allUnknownSubjects[:,
#     dt.update(
#         foundInFreeze1 = as_type(f.foundInFreeze1, str),
#         foundInFreeze2 = as_type(f.foundInFreeze2, str),
#         foundInNovelOmics = as_type(f.foundInNovelOmics, str),
#     )
# ]
# allUnknownSubjects.to_csv('data/rd3_unknown_subjects_20220112.csv')

#
# Before moving any further, all objects defined in section 3 must be resolved.
# Check the following items
#
# - [ ] Disease Codes: all unknown disease codes should be resolved. Codes should
#           either be recoded, added manually to RD3, or discussed with the
#           data provider.
# - [ ] HPO Codes: all HPO codes should be resolved. Manually verify each code by
#           recoding values, manually adding new records to the Phenotype lookup
#           table, or discussing cases with the data provider. 
# - [ ] Unknown Subjects: For any unknown subjects (i.e., do not exist in 
#           any of the freezes), contact the data provider.
#

# prep for import
# del update_dob, update_sex1, update_phenotype, update_hasNotPhenotype, update_disease, update_phenopacketsID

#//////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# IMPORT NEW DATA
# If everything has been resolved or you are importing a few attributes, run
# the following lines (or the lines that apply to you). Before importing any
# data, make sure you add a new entry in the reference table "Patch Informat".

# add new patch record (only do this once!)
newPatch = {
    'id': f'{currentFreeze}_{currentPatch}',
    'type': currentReleaseType,
    'date': str(datetime.now().strftime('%Y-%m-%d')),
    
    # For description, use the following format for patches or use
    # "FreezeX Original Data" for new releases
    'description': f'{currentFreeze.title()} {currentPatch.title()}'
}

rd3.add(entity = 'rd3_patch', data = newPatch)

# prep data for import
update_dob = rd3tools.select_keys(phenopackets, ['id', 'dateofBirth'])
update_sex1 = rd3tools.select_keys(phenopackets, ['id', 'sex1'])
update_phenotype = rd3tools.select_keys(phenopackets, ['id', 'phenotype'])
update_hasNotPhenotype = rd3tools.select_keys(phenopackets, ['id', 'hasNotPhenotype'])
update_disease = rd3tools.select_keys(phenopackets, ['id', 'disease'])
update_phenopacketsID = rd3tools.select_keys(phenopackets, ['id', 'phenopacketsID'])
update_ageofonset = rd3tools.select_keys(phenopackets, ['id', 'ageOfOnset'])

# set patch information for current release
update_patch = []
for d in phenopackets:
    if not (d['id'] in freeze_ids):
        statusMsg('ID {} not found'.format(d['id']))
    subjectPatch = [p for p in freeze if d['id'] == p['id']]
    newSubjectPatch = rd3tools.flatten_attr(subjectPatch[0]['patch'], 'id')
    newSubjectPatch.append(f'{currentFreeze}_{currentPatch}')
    update_patch.append({'id': d['id'], 'patch': ','.join(newSubjectPatch)})


# import data
rd3.batch_update_one_attr(paths['rd3_subjectinfo'], 'dateofBirth', update_dob)
if len(rd3tools.flatten_attr(update_ageofonset, 'ageOfOnset', distinct=True)) > 1:
    rd3.batch_update_one_attr(paths['rd3_subjectinfo'], 'ageOfOnset', update_ageofonset)

rd3.batch_update_one_attr(paths['rd3_subjects'], 'sex1', update_sex1)
rd3.batch_update_one_attr(paths['rd3_subjects'], 'phenotype', update_phenotype)
rd3.batch_update_one_attr(paths['rd3_subjects'], 'hasNotPhenotype', update_hasNotPhenotype)
rd3.batch_update_one_attr(paths['rd3_subjects'], 'disease', update_disease)
rd3.batch_update_one_attr(paths['rd3_subjects'], 'phenopacketsID', update_phenopacketsID)

rd3.batch_update_one_attr(paths['rd3_subjects'], 'patch', update_patch)
rd3.batch_update_one_attr(paths['rd3_subjectinfo'], 'patch', update_patch)

#//////////////////////////////////////////////////////////////////////////////

# IMPORT REVISED FILES
# find cases where phenotype and disease codes are missing

# triage records: 
update_disease = []
update_phenotype = []
update_hasNotPhenotype = []
for p in freeze:
    r = rd3tools.find_dict(phenopackets, 'id', p['id'])
    if len(p['disease']) == 0:
        if r:
            if len(r[0]['disease']) > 0:
                update_disease.append({
                    'id': r[0].get('id'),
                    'disease': r[0].get('disease')
                })
    if len(p['phenotype']) == 0:
        if r:
            if len(r[0]['phenotype']) > 0:
                update_phenotype.append({
                    'id': r[0].get('id'),
                    'phenotype': r[0].get('phenotype')
                })
    if len(p['hasNotPhenotype']) == 0:
        if r:
            if len(r[0]['hasNotPhenotype']) > 0:
                update_hasNotPhenotype.append({
                    'id': r[0].get('id'),
                    'hasNotPhenotype': r[0].get('hasNotPhenotype')
                })


update_dob = []
update_onset = []
for fi in freeze_info:
    r = rd3tools.find_dict(phenopackets, 'id', fi['id'])
    if r:
        if not ('dateofBirth' in fi):
            if r[0]['dateofBirth']:
                update_dob.append({
                    'id': r[0].get('id'),
                    'dateofBirth': r[0].get('dateofBirth')
                })
        if not ('ageOfOnset' in fi):
            if r[0]['ageOfOnset']:
                update_onset.append({
                    'id': r[0].get('id'),
                    'ageOfOnset': r[0].get('ageOfOnset')
                })

# check
statusMsg('Disease cases to update: ', len(update_disease))
statusMsg('Phenotype cases to update: ', len(update_phenotype))
statusMsg('HasNotPhenotype cases to update: ', len(update_hasNotPhenotype))
statusMsg('DateofBirth cases to update', len(update_dob))
statusMsg('Onset cases to update', len(update_onset))

# import
rd3.batch_update_one_attr(paths['rd3_subject'], 'disease', update_disease)
rd3.batch_update_one_attr(paths['rd3_subject'], 'phenotype', update_phenotype)
rd3.batch_update_one_attr(paths['rd3_subject'], 'hasNotPhenotype', update_hasNotPhenotype)
rd3.batch_update_one_attr(paths['rd3_subjectinfo'], 'dateofBirth', update_dob)
rd3.batch_update_one_attr(paths['rd3_subjectinfo'], 'ageOfOnset', update_onset)


# if you need to update patch info
update_patch_ids = list(
    set(
        rd3tools.flatten_attr(update_disease, 'id') +
        rd3tools.flatten_attr(update_phenotype, 'id') +
        rd3tools.flatten_attr(update_hasNotPhenotype, 'id') +
        rd3tools.flatten_attr(update_dob, 'id') +
        rd3tools.flatten_attr(update_onset, 'id')
    )
)
update_patch = []
for id in update_patch_ids:
    r = rd3tools.find_dict(freeze, 'id', id)
    if r:
        patches = rd3tools.flatten_attr(r[0]['patch'], 'id')
        if not ('freeze1_patch1' in patches):
            patches.append('freeze1_patch1')
            update_patch.append({'id': id, 'patch': ','.join(patches)})

rd3.batch_update_one_attr(paths['rd3_subject'], 'patch', update_patch)
rd3.batch_update_one_attr(paths['rd3_subjectinfo'], 'patch', update_patch)

# //////////////////////////////////////

# for testing
# find distinct values for testing and recoding
# dist_sex = []
# dist_solved_status = []
# dist_disease_codes = []
# for p in phenopackets:
#     if p['sex1']:
#         if p['sex1'] not in dist_sex:
#             dist_sex.append(p['sex1'])
#     if p['solved'] not in dist_solved_status:
#         dist_solved_status.append(p['solved'])
#     if p['disease']:
#         for dx in p['disease']:
#             if dx not in dist_disease_codes:
#                 dist_disease_codes.append(dx)
# 
# # for fixing ageOfonset codes:
# for ao in update_onset:
#     ao['ageOfOnset'] = ','.join(list(set(ao['ageOfOnset'].split(','))))
# 
# # find the index of manually run case
# ['' in f['filename'] for f in files].index(True)
#
# write csv
# import csv
# def write_csv(path, data):
#     headers = list(data[0].keys())
#     with open(path, 'w') as file:
#         writer = csv.DictWriter(file, fieldnames = headers)
#         writer.writeheader()
#         writer.writerows(data)
#     file.close()

# write_csv(
#     path = "data/rd3_freeze1patch1_disease_codes_unknown.csv",
#     data = disease_codes_not_found
# )

# write_csv(
#     path = "data/rd3_freeze1patch1_hpo_codes_unknown.csv",
#     data = hpo_codes_not_found
# )
