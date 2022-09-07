#'////////////////////////////////////////////////////////////////////////////
#' FILE: phenopackettools.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-08-04
#' MODIFIED: 2022-09-06
#' PURPOSE: misc functions for extracting and formatting data
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import recodeValue
import re

def recodeSexCodes(value):
  """Recode Phenopacket set value
  Record phenopackets sex values into RD3 terminology

  @param value string containing a sex code to recode
  @return string containing an RD3 sex code
  """
  mappings = {'FEMALE': 'F', 'MALE': 'M', 'UNKNOWN_SEX': 'U'}
  return recodeValue(mappings=mappings, value=value, label="Sex Code")
  
def formatDateOfBirth(value):
  """Format date

  @param value string containing date to recode
  @return date string in yyyy-mm-dd format
  """
  try:
      return re.sub(r'(T00:00:00Z)$', '', value).split('-')[0]
  except:
      return value
      
def unpackPhenotypicFeatures(data):
  """Unpack Phenotypic Features data
  Extract `phenotypicFetures` and separate into 'observed' and
  'unobserved' phenotypes.

  @param data output from data['phenopacket']['phenotypicFeatures']
  @return dictionary with lists for observed and unobserved phenotypes
  """
  result = {'phenotype': [], 'hasNotPhenotype': []}
  for row in data:
    if 'type' in row:
      hpoId = row['type']['id'].replace('HP:','HP_')
      if not (hpoId in result['phenotype']) and not (hpoId in result['hasNotPhenotype']):
        if row.get('negated'):
          if row['negated']:
            result['hasNotPhenotype'].append(hpoId)
          if not row['negated']:
            result['phenotype'].append(hpoId)
        else:
          result['phenotype'].append(hpoId)
  return result
  

def unpackDiseaseCodes(data, mappings: dict = None):
  """Unpack Diseases
  Extract disease IDs Unique ontologies: ['HP', 'Orphanet', 'HGNC', 'OMIM']
  and recode where necessary.

  @param data list of dictionaries from data['phenopacket']['diseases]
  @param mappings a dictionary containing 'incorrect' codes and new mappings

  @return dict with list of diagnostic- and onset codes
  """
  codes = {'diagnostic': [], 'onset': []}
  for row in data:
    if 'term' in row:
      if 'id' in row['term']:
        code1 = row['term']['id']
        if re.search(r'^((Orphanet:)|(ORDO:))', code1):
          code1 = re.sub(r'^((Orphanet:)|(ORDO:))', 'ORDO_', code1)
        if re.search(r'^((OMIM:)|(MIM:))', code1):
          code1 = re.sub(r'^((OMIM:)|(MIM:))', 'MIM_', code1)
        if mappings and (code1 in mappings):
          code1 = recodeValue(mappings=mappings,value=code1,label='Disease Code')
        if not (code1 in codes['diagnostic']):
          codes['diagnostic'].append(code1)
    if 'classOfOnset' in row:
      if 'id' in row['classOfOnset']:
        code2 = row['classOfOnset']['id']
        code2 = re.sub(r'^(HP:)', 'HP_', code2)
        if not (code2 in codes['onset']):
          codes['onset'].append(code2)
  return codes