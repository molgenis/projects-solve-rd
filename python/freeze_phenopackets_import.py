#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_phenopackets_import.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-02
#' MODIFIED: 2021-08-03
#' PURPOSE: push phenopackets metadata into RD3
#' STATUS: working
#' PACKAGES: os, json, requests, urlib.parse, molgenis.client
#' COMMENTS: Run in the same folder as all phenopackets files
#'////////////////////////////////////////////////////////////////////////////

import re
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
                code1 = d['term']['id']
                if re.search(r'^((Orphanet:)|(ORDO:))', code1):
                    code1 = re.sub(r'^((Orphanet:)|(ORDO:))', 'ORDO_', code1)
                if re.search(r'^((OMIM:)|(MIM:))', code1):
                    code1 = re.sub(r'^((OMIM:)|(MIM:))', 'MIM_', code1)
                if code1 in ids_to_recode:
                    code1 = ids_to_recode[code1]['new']
                dx.append(code1)
        if 'classOfOnset' in d:
            if 'id' in d['classOfOnset']:
                code2 = d['classOfOnset']['id']
                code2 = re.sub(r'^(HP:)', 'HP_', code2)
                ao.append(code2)
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
    entity = config['releases']['freeze1']['subject'],
    attributes = 'id,subjectID,clinical_status,disease,phenotype,hasNotPhenotype,phenopacketsID,patch',
    batch_size = 10000
)
freeze_ids = rd3tools.flatten_attr(freeze, 'id')

freeze_info = rd3.get(
    entity = config['releases']['freeze1']['subjectinfo'],
    attributes = 'id,dateofBirth,ageOfOnset,patch',
    batch_size = 10000
)


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
    path = config['releases']['base'] + config['releases']['freeze1-patch1']['phenopacket']
)

# make sure there are only json files
files2 = []
for f in files:
    if re.search(r'(.json)$', f['filename']):
        files2.append(f)

rd3tools.status_msg('Will process', len(files2), 'files')


# init loop params and objects
proc = {'subject': True, 'pheno': True, 'dx': True}
hpo_codes_not_found = []
disease_codes_not_found = []
onset_codes_not_found = []
unavailable = []
phenopackets = []
for file in files2:
    rd3tools.status_msg('Processing file: {}'.format(file['filename']))
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
    # make sure `subject` exists before processing subattributes
    if proc['subject']:
        if 'subject' in f['phenopacket']:
            if 'dateOfBirth' in f['phenopacket']['subject']:
                p['dateofBirth'] = __recode__date(f['phenopacket']['subject']['dateOfBirth'])
            if 'sex' in f['phenopacket']['subject']:
                p['sex1'] = __recode__sex(f['phenopacket']['subject']['sex'])
    # make sure `phenotypicFeatures` exists
    if proc['pheno']:
        if 'phenotypicFeatures' in f['phenopacket']:
            phenotypic_features = __unpack__phenotypicfeatures(
                phenotypicFeatures = f['phenopacket']['phenotypicFeatures']
            )
            # triage HPO into `has` and `hasnot`
            patient_hpo_has = []
            patient_hpo_hasnot = []
            # validate codes and isolate unknown cases
            for hc1 in phenotypic_features['phenotype']:
                if hc1 in hpo_codes:
                    patient_hpo_has.append(hc1)
                else:
                    rd3tools.status_msg('Unknown HPO code: {}'.format(hc1))
                    hpo_codes_not_found.append({'id': p['id'], 'hpo': hc1})
                    phenotypic_features['phenotype'].remove(hc1)
            # validate codes and isolate unknown cases
            for hc2 in phenotypic_features['hasNotPhenotype']:
                if hc2 in hpo_codes:
                    patient_hpo_hasnot.append(hc2)
                else:
                    rd3tools.status_msg('Unknown HPO code: {}'.format(hc2))
                    hpo_codes_not_found.append({'id': p['id'], 'hpo': hc2})
                    phenotypic_features['hasNotPhenotype'].remove(hc2)
            p['phenotype'] = ','.join(patient_hpo_has)
            p['hasNotPhenotype'] = ','.join(patient_hpo_hasnot)
    # make sure `diseases` exist first
    if proc['dx']:
        if 'diseases' in f['phenopacket']:
            diseases = __unpack__diseases(f['phenopacket']['diseases'])
            patient_diseases_has = []
            # triage dx IDs: isolate invalid codes for review
            for dx in diseases['dx']:
                if dx in disease_codes:
                    patient_diseases_has.append(dx)
                else:
                    rd3tools.status_msg('Unknown disease code: {}'.format(dx))
                    disease_codes_not_found.append({'id': p['id'], 'code': dx})
                    diseases['dx'].remove(dx)
            p['disease'] = ','.join(patient_diseases_has)
            # triage onset IDs (HPO): isolate invalid codes for review
            if len(diseases['ao']) > 0:
                valid_onset_codes = []
                for ao in diseases['ao']:
                    if ao in hpo_codes:
                        valid_onset_codes.append(ao)
                    else:
                        rd3tools.status_msg('Unknown onset code: {}'.format(ao))
                        onset_codes_not_found.append({'id': p['id'], 'onset': ao})
                p['ageOfOnset'] = ','.join(valid_onset_codes)
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


# check unique entries
rd3tools.flatten_attr(hpo_codes_not_found, 'hpo', distinct = True)
rd3tools.flatten_attr(disease_codes_not_found, 'code', distinct = True)
rd3tools.flatten_attr(onset_codes_not_found, 'onset', distinct = True)

# import data
# test = list(filter(lambda d: d['id'] in '', phenopackets)) # enter test case

# prep for import
# del update_dob, update_sex1, update_phenotype, update_hasNotPhenotype, update_disease, update_phenopacketsID

#//////////////////////////////////////

# IMPORT NEW RELEASE
# if you batch importing data, use the following code block.
# Otherwise, skip to the next section


update_dob = rd3tools.select_keys(phenopackets, ['id', 'dateOfBirth'])
update_sex1 = rd3tools.select_keys(phenopackets, ['id', 'sex1'])
update_phenotype = rd3tools.select_keys(phenopackets, ['id', 'phenotype'])
update_hasNotPhenotype = rd3tools.select_keys(phenopackets, ['id', 'hasNotPhenotype'])
update_disease = rd3tools.select_keys(phenopackets, ['id', 'disease'])
update_phenopacketsID = rd3tools.select_keys(phenopackets, ['id', 'phenopacketsID'])
update_ageofonset = rd3tools.select_keys(phenopackets, ['id', 'ageOfOnset'])


rd3.batch_update_one_attr('rd3_freeze2_subjectinfo', 'dateofBirth', update_dob)
if len(rd3tools.flatten_attr(update_ageofonset, 'ageOfOnset', distinct=True)) > 1:
    rd3.batch_update_one_attr('rd3_freeze2_subjectinfo', 'ageOfOnset', update_ageofonset)
rd3.batch_update_one_attr('rd3_freeze2_subject', 'sex1', update_sex1)
rd3.batch_update_one_attr('rd3_freeze2_subject', 'phenotype', update_phenotype)
rd3.batch_update_one_attr('rd3_freeze2_subject','hasNotPhenotype', update_hasNotPhenotype)
rd3.batch_update_one_attr('rd3_freeze2_subject', 'disease', update_disease)
rd3.batch_update_one_attr('rd3_freeze2_subject', 'phenopacketsID', update_phenopacketsID)


#//////////////////////////////////////

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
rd3tools.status_msg('Disease cases to update: ', len(update_disease))
rd3tools.status_msg('Phenotype cases to update: ', len(update_phenotype))
rd3tools.status_msg('HasNotPhenotype cases to update: ', len(update_hasNotPhenotype))
rd3tools.status_msg('DateofBirth cases to update', len(update_dob))
rd3tools.status_msg('Onset cases to update', len(update_onset))

# import
rd3.batch_update_one_attr("rd3_freeze1_subject", 'disease', update_disease)
rd3.batch_update_one_attr('rd3_freeze1_subject', 'phenotype', update_phenotype)
rd3.batch_update_one_attr('rd3_freeze1_subject', 'hasNotPhenotype', update_hasNotPhenotype)
rd3.batch_update_one_attr('rd3_freeze1_subjectinfo', 'dateofBirth', update_dob)
rd3.batch_update_one_attr('rd3_freeze1_subjectinfo', 'ageOfOnset', update_onset)


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

rd3.batch_update_one_attr('rd3_freeze1_subject', 'patch', update_patch)
rd3.batch_update_one_attr('rd3_freeze1_subjectinfo', 'patch', update_patch)

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
