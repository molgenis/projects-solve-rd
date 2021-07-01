# '////////////////////////////////////////////////////////////////////////////
# ' FILE: freeze_phenopackets_import.py
# ' AUTHOR: David Ruvolo
# ' CREATED: 2021-06-02
# ' MODIFIED: 2021-06-04
# ' PURPOSE: push phenopackets metadata into RD3
# ' STATUS: working
# ' PACKAGES: os, json, requests, urlib.parse, molgenis.client
# ' COMMENTS: Run in the same folder as all phenopackets files
# '////////////////////////////////////////////////////////////////////////////

# import os
# import json
# import re
# import requests
# from urllib.parse import quote_plus, urlparse, parse_qs
# import molgenis.client as molgenis
import python.rd3tools as rd3tools

config = rd3tools.load_yaml_config('python/_config.yml')

# //////////////////////////////////////

# @title read_phenopacket
# @description read phenopacket file
# @param filename name of the file
# @return a dictionary containing phenopackets data
def read_phenopacket(filename):
    file = open(filename, 'r')
    return json.load(file)


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
# @param diseases list of dictionaries from data['phenopacket']['diseases]
# @return a list of disease IDs
def __unpack__diseases(diseases):
    ids_to_recode = {
        'MIM_159000': {'old':'MIM_159000','new':'MIM_609200'},
        'MIM_159001': {'old':'MIM_159001','new':'MIM_181350'},
        'MIM_607569': {'old':'MIM_607569','new':'MIM_603689'},
        'ORDO_856': {'old': 'ORDO_856', 'new': ''}
    }
    out = []
    for disease in diseases:
        code = disease['term']['id']
        if re.search(r'^(Orphanet:)', code):
            code = re.sub(r'^(Orphanet:)', 'ORDO_', code)
        if re.search(r'^(OMIM:)', code):
            code = re.sub(r'^(OMIM:)', 'MIM_', code)
        if code in ids_to_recode:
            code = ids_to_recode[code]['new']
        out.append(code)
    return out

# Unique ontologies: ['HP', 'Orphanet', 'HGNC', 'OMIM']

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

# @title select keys
# @describe reduce list of dictionaries to named keys
# @param data list of dictionaries to select
# @param keys an array of values
# @return a list of dictionaries
def select_keys(data, keys):
    return list(map(lambda x: {k: v for k, v in x.items() if k in keys}, data))

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

# @title Unpack Phenopackets JSON string
# @description extract key information and reshape into RD3
# @param data result from `read_phenopacket`
# @param filename name of the file
# @param a dictionary containing extracted metadata
def unpack_phenopacket(data, filename):
    out = {
        'id': data['phenopacket']['id'] + '_original',
        'dateofBirth': __recode__date(data['phenopacket']['subject']['dateOfBirth']),
        'sex1': __recode__sex(data['phenopacket']['subject']['sex']),
        'phenotype': [],
        'hasNotPhenotype': [],
        'disease': [],
        'phenopacketsID': filename
    }
    if len(data['phenopacket']['phenotypicFeatures']):
        phenotypic_features = __unpack__phenotypicfeatures(
            data['phenopacket']['phenotypicFeatures'])
        out['phenotype'] = ','.join(phenotypic_features['phenotype'])
        out['hasNotPhenotype'] = ','.join(
            phenotypic_features['hasNotPhenotype'])
    if len(data['phenopacket']['diseases']):
        diseases = __unpack__diseases(data['phenopacket']['diseases'])
        out['disease'] = ','.join(diseases)
    return out

# //////////////////////////////////////


# init molgenis sesssion
rd3 = rd3tools.molgenis(
    url = config['hosts'][config['run']['env']],
    token = config['tokens'][config['run']['env']]
)

# pull freeze2 data
freeze2 = rd3.get(
    entity = config['releases'][config['run']['release']]['subject'],
    attributes = 'id,subjectID',
    batch_size = 10000
)
freeze2_ids = flatten_attr(freeze2, 'id')

# get HPO and disease codes
hpo_codes_raw = rd3.get(entity = 'rd3_phenotype', batch_size = 10000)
disease_codes_raw = rd3.get(entity = 'rd3_disease', batch_size = 10000)

hpo_codes = flatten_attr(hpo_codes_raw, 'id')
disease_codes = flatten_attr(disease_codes_raw, 'id')

# disease_error_file = open('disease_errors.txt', 'w')
# hpo_error_file = open('hpo_errors.txt', 'w')

# load and parse phenopackets
# also run a check to make sure IDs exist in RD3
files = rd3tools.cluster_list_files(
    path = config['releases']['base'] + config['releases'][config['run']['release']]['phenopacket']
)
hpo_codes_not_found = []
disease_codes_not_found = []
unavailable = []
phenopackets = []
for file in files:
    f = rd3tools.cluster_read_json(path = file)
    # p = unpack_phenopacket(data=f, filename=file)
    p = {
        'id': f['phenopacket']['id'] + '_original',
        'dateofBirth': __recode__date(f['phenopacket']['subject']['dateOfBirth']),
        'sex1': __recode__sex(f['phenopacket']['subject']['sex']),
        'phenotype': [],
        'hasNotPhenotype': [],
        'disease': [],
        'phenopacketsID': file
    }
    if f['phenopacket']['phenotypicFeatures']:
        phenotypic_features = __unpack__phenotypicfeatures(f['phenopacket']['phenotypicFeatures'])
        patient_hpo_has = []
        patient_hpo_hasnot = []
        for hc1 in phenotypic_features['phenotype']:
            if hc1 in hpo_codes:
                patient_hpo_has.append(hc1)
            else:
                print({'id': p['id'], 'hpo': hc1})
                hpo_codes_not_found.append({'id': p['id'], 'hpo': hc1})
                phenotypic_features['phenotype'].remove(hc1)
        for hc2 in phenotypic_features['hasNotPhenotype']:
            if hc2 in hpo_codes:
                patient_hpo_hasnot.append(hc2)
            else:
                print({'id': p['id'], 'hpo': hc2})
                hpo_codes_not_found.append({'id': p['id'], 'hpo': hc2})
                phenotypic_features['hasNotPhenotype'].remove(hc2)
        p['phenotype'] = ','.join(patient_hpo_has)
        p['hasNotPhenotype'] = ','.join(patient_hpo_hasnot)
    if f['phenopacket']['diseases']:
        diseases = __unpack__diseases(f['phenopacket']['diseases'])
        patient_diseases_has = []
        for dx in diseases:
            if dx in disease_codes:
                patient_diseases_has.append(dx)
            else:
                print({'id': p['id'], 'dx': dx})
                disease_codes_not_found.append({'id': p['id'], 'code': dx})
                diseases.remove(dx)
        p['disease'] = ','.join(patient_diseases_has)
    if p['id'] in freeze2_ids:
        phenopackets.append(p)
    else:
        unavailable.append(p)

# display message
print('Count of Phenopackets PatientIDs that exist in RD3:', len(phenopackets))
print('Count of Phenopackets PatientIDs that do notexist in RD3:', len(unavailable))
print('Count of unknown HPO codes:', len(hpo_codes_not_found))
print('Count of unknown disease codes:', len(disease_codes_not_found))

# import data
# test = list(filter(lambda d: d['id'] in '', phenopackets)) # enter test case

# prep for import
# del update_dob, update_sex1, update_phenotype, update_hasNotPhenotype, update_disease, update_phenopacketsID
update_dob = select_keys(phenopackets, ['id', 'dateofBirth'])
update_sex1 = select_keys(phenopackets, ['id', 'sex1'])
update_phenotype = select_keys(phenopackets, ['id', 'phenotype'])
update_hasNotPhenotype = select_keys(phenopackets, ['id', 'hasNotPhenotype'])
update_disease = select_keys(phenopackets, ['id', 'disease'])
update_phenopacketsID = select_keys(phenopackets, ['id', 'phenopacketsID'])

# import
rd3.batch_update_one_attr('rd3_freeze2_subjectinfo', 'dateofBirth', update_dob)
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
