#///////////////////////////////////////////////////////////////////////////////
# FILE: utils.py
# AUTHOR: David Ruvolo
# CREATED: 2023-05-10
# MODIFIED: 2023-05-10
# PURPOSE: misc functions
# STATUS: stable
# PACKAGES: NA
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import recodeValue
from csv import QUOTE_ALL

def to_csv(path,data):
  return data.to_pandas().to_csv(path,index=False,encoding='UTF-8',quoting=QUOTE_ALL)

def to_csv_str(data):
  return data.to_pandas().to_csv(index=False,encoding='UTF-8',quoting=QUOTE_ALL)


def recodeCommaStrings(mappings,value):
  codes = value.split(',')
  terms = []
  for code in codes:
    value = recodeValue(mappings, code,'HPO')
    terms.append(value)
  return ','.join(terms)