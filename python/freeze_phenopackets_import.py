# '////////////////////////////////////////////////////////////////////////////
# ' FILE: freeze_phenopackets_import.py
# ' AUTHOR: David Ruvolo
# ' CREATED: 2021-06-02
# ' MODIFIED: 2021-08-02
# ' PURPOSE: push phenopackets metadata into RD3
# ' STATUS: working
# ' PACKAGES: os, json, requests, urlib.parse, molgenis.client
# ' COMMENTS: Run in the same folder as all phenopackets files
# '////////////////////////////////////////////////////////////////////////////

import os
import json
import re
import time
# import requests
# from urllib.parse import quote_plus, urlparse, parse_qs
# import molgenis.client as molgenis
import python.rd3tools as rd3tools
config = rd3tools.load_yaml_config('python/_config.yml')

# //////////////////////////////////////


# @title unpack phenotypicFeatures
# @description extract `phenotypicFeatures` from and separate into observed and unobserved phenotypes
# @param phenotypicFeatures list of dictionaries from data['phenopacket']['phenotypicFeatures']
# @return a dictionary with two lists of HPO IDs
def __unpack__phenotypicfeatures(phenotypicFeatures):
    phenotype = []
    hasNotPhenotype = []
    for pheno in phenotypicFeatures:
        if pheno:
            hpo_id = re.sub(r'^(HP:)', 'HP_', pheno['type']['id'])
            if not (hpo_id in phenotype) and not (hpo_id in hasNotPhenotype):
                if 'negated' in pheno:
                    if pheno['negated']:
                        hasNotPhenotype.append(hpo_id)
                    if not pheno['negated']:
                        phenotype.append(hpo_id)
                else:
                    phenotype.append(hpo_id)
    return {'phenotype': phenotype, 'hasNotPhenotype': hasNotPhenotype}

# @title Unpack Diseases
# @description extract disease IDs
#   Unique ontologies: ['HP', 'Orphanet', 'HGNC', 'OMIM']
# @param data list of dictionaries from data['phenopacket']['diseases]
# @return a list of disease IDs
def __unpack__diseases(data):
    ids_to_recode = {
        'MIM_159000': {'old':'MIM_159000','new':'MIM_609200'},
        'MIM_159001': {'old':'MIM_159001','new':'MIM_181350'},
        'MIM_607569': {'old':'MIM_607569','new':'MIM_603689'},
        'ORDO_856': {'old': 'ORDO_856', 'new': ''}
    }
    dx = []
    ao = []
    for d in data:
        if 'term' in d:
            if 'id' in d['term']:
                code = d['term']['id']
                if re.search(r'^(Orphanet:)', code):
                    code = re.sub(r'^(Orphanet:)', 'ORDO_', code)
                if re.search(r'^(OMIM:)', code):
                    code = re.sub(r'^(OMIM:)', 'MIM_', code)
                if code in ids_to_recode:
                    code = ids_to_recode[code]['new']
                dx.append(code)
        if 'classOfOnset' in d:
            if 'id' in d['classOfOnset']:
                code = re.sub(r'^(HP:)', 'HP_', d['classOfOnset']['id'])
                ao.append(code)
    return {'dx': dx, 'ao': ao}

# @title Records Sex
# @describe record phenopackets sex values into RD3 terminology
# @param value string containing a sex
# @return a string
def __recode__sex(value):
    if value.lower() == 'female':
        return 'F'
    elif value.lower() == 'male':
        return 'M'
    elif value.lower() == 'unknown_sex':
        return 'U'
    else:
        return value

# @title Format date
# @describe format date
# @param value string containing date to recode
# @return a string containing yyyy-mm-dd
def __recode__date(value):
    if value != '':
        return re.sub(r'(T00:00:00Z)', '', value).split('-')[0]
    else:
        return value

# //////////////////////////////////////


# init molgenis sesssion
rd3 = rd3tools.molgenis(config['hosts']['prod'], token = config['tokens']['prod'])

# pull freeze2 data
freeze = rd3.get(
    entity = config['releases']['freeze2']['subject'],
    attributes = 'id,subjectID',
    batch_size = 10000
)
freeze_ids = rd3tools.flatten_attr(freeze, 'id')

# get HPO and disease codes
hpo_codes_raw = rd3.get(entity = 'rd3_phenotype', batch_size = 10000)
disease_codes_raw = rd3.get(entity = 'rd3_disease', batch_size = 10000)

hpo_codes = rd3tools.flatten_attr(hpo_codes_raw, 'id')
disease_codes = rd3tools.flatten_attr(disease_codes_raw, 'id')

# disease_error_file = open('disease_errors.txt', 'w')
# hpo_error_file = open('hpo_errors.txt', 'w')

# load and parse phenopackets
# also run a check to make sure IDs exist in RD3
files = rd3tools.cluster_list_files(
    path = config['releases']['base'] + config['releases']['freeze2']['phenopacket']
)
proc = {'subject': True, 'pheno': False, 'dx': False}
hpo_codes_not_found = []
disease_codes_not_found = []
onset_codes_not_found = []
unavailable = []
phenopackets = []
for file in files:
    rd3tools.status_msg('Reading file: {}'.format(file['filename']))
    f = rd3tools.cluster_read_json(path = file['filepath'])
    p = {
        'id': f['phenopacket']['id'] + '_original',
        'dateofBirth': None,
        'sex1': None,
        'phenotype': [],
        'hasNotPhenotype': [],
        'disease': [],
        'ageOfOnset': [],
        'phenopacketsID': file['filename']
    }
    rd3tools.status_msg('Processing data...')
    # make sure `subject` exists before processing subattributes
    if proc['subject']:
        if 'subject' in f['phenopacket']:
            rd3tools.status_msg('File has `subject` metadata')
            if 'dateOfBirth' in f['phenopacket']['subject']:
                rd3tools.status_msg('Extracting dateOfBirth')
                p['dateofBirth'] = __recode__date(f['phenopacket']['subject']['dateOfBirth'])
            if 'sex' in f['phenopacket']['subject']:
                rd3tools.status_msg('Extracting sex')
                p['sex1'] = __recode__sex(f['phenopacket']['subject']['sex'])
    # make sure `phenotypicFeatures` exists
    if proc['pheno']:
        if 'phenotypicFeatures' in f['phenopacket']:
            rd3tools.status_msg('File has `phenotypicFeatures`')
            phenotypic_features = __unpack__phenotypicfeatures(
                phenotypicFeatures = f['phenopacket']['phenotypicFeatures']
            )
            # triage HPO into `has` and `hasnot`
            patient_hpo_has = []
            patient_hpo_hasnot = []
            # validate codes and isolate unknown cases
            rd3tools.status_msg('Validating phenotype codes')
            for hc1 in phenotypic_features['phenotype']:
                if hc1 in hpo_codes:
                    patient_hpo_has.append(hc1)
                else:
                    print({'id': p['id'], 'hpo': hc1})
                    hpo_codes_not_found.append({'id': p['id'], 'hpo': hc1})
                    phenotypic_features['phenotype'].remove(hc1)
            # validate codes and isolate unknown cases
            for hc2 in phenotypic_features['hasNotPhenotype']:
                if hc2 in hpo_codes:
                    patient_hpo_hasnot.append(hc2)
                else:
                    print({'id': p['id'], 'hpo': hc2})
                    hpo_codes_not_found.append({'id': p['id'], 'hpo': hc2})
                    phenotypic_features['hasNotPhenotype'].remove(hc2)
            p['phenotype'] = ','.join(patient_hpo_has)
            p['hasNotPhenotype'] = ','.join(patient_hpo_hasnot)
    # make sure `diseases` exist first
    if proc['dx']:
        if 'diseases' in f['phenopacket']:
            rd3tools.status_msg('Validating disease codes')
            diseases = __unpack__diseases(f['phenopacket']['diseases'])
            patient_diseases_has = []
            # triage dx IDs: isolate invalid codes for review
            for dx in diseases['dx']:
                if dx in disease_codes:
                    patient_diseases_has.append(dx)
                else:
                    print({'id': p['id'], 'dx': dx})
                    disease_codes_not_found.append({'id': p['id'], 'code': dx})
                    diseases['dx'].remove(dx)
            p['disease'] = ','.join(patient_diseases_has)
            # triage onset IDs (HPO): isolate invalid codes for review
            rd3tools.status_msg('Validating onset codes')
            valid_onset_codes = []
            for ao in diseases['ao']:
                if ao in disease_codes:
                    valid_onset_codes.append(ao)
                else:
                    print({'id': p['id'], 'onset': ao})
                    onset_codes_not_found.append({'id': p['id'], 'onset': ao})
            p['ageOfOnset'] = ','.join(diseases['ao'])
    # make sure participant ID exists in current release
    if p['id'] in freeze_ids:
        phenopackets.append(p)
    else:
        rd3tools.status_msg('Participant does not exist in current freeze')
        unavailable.append(p)

# display message
print('Count of Phenopackets PatientIDs that exist in RD3:', len(phenopackets))
print('Count of Phenopackets PatientIDs that do notexist in RD3:', len(unavailable))
print('Count of unknown HPO codes:', len(hpo_codes_not_found))
print('Count of unknown disease codes:', len(disease_codes_not_found))
print('Count of unknown onset codes', len(onset_codes_not_found))

# import data
# test = list(filter(lambda d: d['id'] in '', phenopackets)) # enter test case

# prep for import
# del update_dob, update_sex1, update_phenotype, update_hasNotPhenotype, update_disease, update_phenopacketsID
update_dob = rd3tools.select_keys(phenopackets, ['id', 'dateOfBirth'])
update_sex1 = rd3tools.select_keys(phenopackets, ['id', 'sex1'])
update_phenotype = rd3tools.select_keys(phenopackets, ['id', 'phenotype'])
update_hasNotPhenotype = rd3tools.select_keys(phenopackets, ['id', 'hasNotPhenotype'])
update_disease = rd3tools.select_keys(phenopackets, ['id', 'disease'])
update_phenopacketsID = rd3tools.select_keys(phenopackets, ['id', 'phenopacketsID'])
update_ageofonset = rd3tools.select_keys(phenopackets, ['id', 'ageOfOnset'])

# import
rd3.batch_update_one_attr('rd3_freeze2_subjectinfo', 'dateofBirth', update_dob)
if len(rd3tools.flatten_attr(update_ageofonset, 'ageOfOnset', distinct=True)) > 1:
    rd3.batch_update_one_attr('rd3_freeze2_subjectinfo', 'ageOfOnset', update_ageofonset)
rd3.batch_update_one_attr('rd3_freeze2_subject', 'sex1', update_sex1)
rd3.batch_update_one_attr('rd3_freeze2_subject', 'phenotype', update_phenotype)
rd3.batch_update_one_attr('rd3_freeze2_subject','hasNotPhenotype', update_hasNotPhenotype)
rd3.batch_update_one_attr('rd3_freeze2_subject', 'disease', update_disease)
rd3.batch_update_one_attr('rd3_freeze2_subject', 'phenopacketsID', update_phenopacketsID)

# //////////////////////////////////////

# for testing
# find distinct values for testing and recoding
dist_sex = []
dist_solved_status = []
dist_disease_codes = []
for p in phenopackets:
    if p['sex1']:
        if p['sex1'] not in dist_sex:
            dist_sex.append(p['sex1'])
    if p['solved'] not in dist_solved_status:
        dist_solved_status.append(p['solved'])
    if p['disease']:
        for dx in p['disease']:
            if dx not in dist_disease_codes:
                dist_disease_codes.append(dx)

# find the index of manually run case
['' in f['filename'] for f in files].index(True)

for d in update_dob:
    d['dateofBirth'] = d.pop('dateOfBirth')