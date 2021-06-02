#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_phenopackets_import.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-02
#' MODIFIED: 2021-06-02
#' PURPOSE: push phenopackets metadata into RD3
#' STATUS: in.progress
#' PACKAGES: NA
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import os
import json
import re
import requests
from urllib.parse import quote_plus, urlparse, parse_qs
import molgenis.client as molgenis

#//////////////////////////////////////

# @title Molgenis Extra
# @describe extend class Molgenis Session
# @param molgenis.Session required Session object
class molgenis_extra(molgenis.Session):
    def batch_update_one_attr(self, entity, attr, values):
        add='No new data'
        for i in range(0, len(values), 1000):
            add='Update did tot go OK'
            """Updates one attribute of a given entity with the given values of the given ids"""
            response = self._session.put(
                self._api_url + "v2/" + quote_plus(entity) + "/" + attr,
                headers=self._get_token_header_with_content_type(),
                data=json.dumps({'entities': values[i:i+1000]})
            )
            if response.status_code == 200:
                add='Update went OK'
            else: 
                try:
                    response.raise_for_status()
                except requests.RequestException as ex:
                    self._raise_exception(ex)
                return response
        return add

# @title read_phenopacket
# @description read phenopacket file
# @param filename name of the file
# @return a dictionary containing phenopackets data
def read_phenopacket(filename):
    file = open(filename)
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
            if 'negated' in pheno:
                if pheno['negated']:
                    phenotype.append(hpo_id)
                if not pheno['negated']:
                    hasNotPhenotype.append(hpo_id)
            else:
                hasNotPhenotype.append(hpo_id)
    return {'phenotype': phenotype, 'hasNotPhenotype': hasNotPhenotype}

# @title Unpack Diseases
# @description extract disease IDs
# @param diseases list of dictionaries from data['phenopacket']['diseases]
# @return a list of disease IDs
def __unpack__diseases(diseases):
    out = []
    for disease in diseases:
        code = disease['term']['id']
        if re.search(r'^(Orphanet:)', code):
            code = re.sub(r'^(Orphanet:)', 'ORDO_', code)
        if re.search(r'^(OMIM:)', code):
            code = re.sub(r'^(OMIM:)', 'MIM_', code)
        out.append(code)
    return out

# Unique ontologies: ['HP', 'Orphanet', 'HGNC', 'OMIM']


# @title Recode solved status value
# @describe recode phenopackets status into RD3 terminology
# @param value string containing a value to record
def __recode__solvedstatus(value):
    if value.lower() in ['unsolved', 'unknown']:
        return 'false'
    elif value.lower() == 'solved':
        return 'true'
    else:
        return value

# @title Records Sex
# @describe record phenopackets sex values into RD3 terminology
# @param value string containing a sex
# @return a string
def __recode__sex(value):
    if value.lower() == 'female': return 'F'
    elif value.lower() == 'male': return 'M'
    elif value.lower() == 'uknown_sex': return 'U'
    else: return value

# @title Format date
# @describe format date
# @param value string containing date to recode
# @return a string containing yyyy-mm-dd
def __recode__date(value):
    if value is not '':
        return re.sub('(T00:00:00Z)', '', value).split('-')[0]
    else:
        return value

# @title Unpack Phenopackets JSON string
# @description extract key information and reshape into RD3
# @param data result from `read_phenopacket`
# @param a dictionary containing extracted metadata
def unpack_phenopacket(data):
    out = {
        'id': data['phenopacket']['id'] + '_original',
        'dateofBirth': __recode__date(data['phenopacket']['subject']['dateOfBirth']),
        'sex1': __recode__sex(data['phenopacket']['subject']['sex']),
        'solved': __recode__solvedstatus(data['interpretation']['resolutionStatus']),
        'phenotype': [],
        'hasNotPhenotype': [],
        'disease': []
    }
    if len(data['phenopacket']['phenotypicFeatures']):
        phenotypic_features = __unpack__phenotypicfeatures(data['phenopacket']['phenotypicFeatures'])
        out['phenotype'] = ','.join(phenotypic_features['phenotype'])
        out['hasNotPhenotype'] = ','.join(phenotypic_features['hasNotPhenotype'])
    if len(data['phenopacket']['diseases']):
        diseases = __unpack__diseases(data['phenopacket']['diseases'])
        out['disease'] = ','.join(diseases)
    return out

# @title select keys
# @describe reduce list of dictionaries to named keys
# @param data list of dictionaries to select
# @param keys an array of values
# @return a list of dictionaries
def select_keys(data, keys):
    return list(map(lambda x: {k:v for k,v in x.items() if k in keys}, data))

#//////////////////////////////////////

# init molgenis sesssion
# os.environ['molgenisToken'] = ''
env = 'prod'
api = {
    'host': {
        'prod': 'https://solve-rd.gcc.rug.nl/api/',
        'acc': 'https://solve-rd-acc.gcc.rug.nl/api/'
    },
    'token': os.getenv('molgenisToken') if os.getenv('molgenisToken') is not None else None
}

# init session
rd3 = molgenis_extra(url=api['host'][env], token=api['token'])


# load and parse phenopackets
files = os.listdir()
phenopackets = []
for file in files:
    f = read_phenopacket(file)
    phenopackets.append(unpack_phenopacket(f))


# import data
test = list(filter(lambda d: d['id'] in '', phenopackets)) # enter test case
rd3.batch_update_one_attr('rd3_freeze2_subjectinfo', 'dateofBirth', select_keys(test, ['id', 'dateofBirth']))
rd3.batch_update_one_attr('rd3_freeze2_subject', 'sex1', select_keys(test, ['id', 'sex1']))
rd3.batch_update_one_attr('rd3_freeze2_subject', 'solved', select_keys(test, ['id', 'solved']))
rd3.batch_update_one_attr('rd3_freeze2_subject', 'phenotype', select_keys(test, ['id', 'phenotype']))
rd3.batch_update_one_attr('rd3_freeze2_subject', 'hasNotPhenotype', select_keys(test, ['id', 'hasNotPhenotype']))
rd3.batch_update_one_attr('rd3_freeze2_subject', 'disease', select_keys(test, ['id', 'disease']))


#//////////////////////////////////////

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
