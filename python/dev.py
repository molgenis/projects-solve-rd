#'////////////////////////////////////////////////////////////////////////////
#' FILE: dev.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-04-28
#' MODIFIED: 2021-04-28
#' PURPOSE: test script for extracting values from RD3
#' STATUS: in.progress
#' PACKAGES: molgenis.client
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

# imports
import os
from python.novelomics_experiment_v2 import distinct_dict
import molgenis.client as molgenis

# init session
# os.environ['molgenisToken'] = ''
rd3 = molgenis.Session(
    url='https://solve-rd-acc.gcc.rug.nl/api/',
    token=os.environ['molgenisToken']
)

# @title Distinct Dictionary Attributes
# @description get distinct values of an attribute from a list of dictionaries
# @param data input dataset
# @param key1 target key of a dictionary
# @param key2 target key of a nested dictionary in a dictionary
# @return list of distinct values
def distinct_dict_attribs(data, key1, key2):
    out = []
    for d in data:
        try:
            value = d.get(key1, {}).get(key2)
        except:
            return None
        out.append(value)
    return list(set(out))

# pull data
tissue_types = rd3.get(entity='rd3_freeze1_sample',attributes='tissueType', batch_size=10000)
x = distinct_dict_attribs(tissue_types, 'tissueType', 'identifier')


erns = rd3.get(entity='rd3_freeze1_sample', attributes='ERN')
y = distinct_dict_attribs(erns, 'ERN', 'identifier')
