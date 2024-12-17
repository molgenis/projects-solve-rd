"""Mapping script for RD3 EMX1 to EMX2"""

from os import environ
from dotenv import load_dotenv
import molgenis.client
from rd3tools.utils import flatten_data
from datatable import dt, f
load_dotenv()

rd3 = molgenis.client.Session(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# retrieve first n family identifiers
families = rd3.get('solverd_subjects', attributes='fid')
fids = []
for row in families:
    if 'fid' in row.keys():
        if row['fid'] not in fids:
            fids.append(row['fid'])

QUERY = ','.join([f"fid=q={fid}" for fid in fids[:5]])
subjects = rd3.get('solverd_subjects', q=QUERY)


subjects_dt = dt.Frame(flatten_data(subjects, 'subjectID|id|value'))


emx2_individuals = []
for subject in subjects:
    new_subject_entry = {}

    # map 'subjectID' to 'id' in the individuals table
    new_subject_entry['id'] = subject['subjectID']

    # map: 'sex1' to 'gender at birth'
    subject_sex1 = subject['sex1']['id']
    if subject_sex1 == 'M':
        new_subject_entry['gender at birth'] = 'assigned male at birth'
    elif subject_sex1 == 'F':
        new_subject_entry['gender at birth'] = 'assigned female at birth'
    else:
        print(f"Value {subject_sex1} cannot be mapped")

    emx2_individuals.append(new_subject_entry)

dt.Frame(emx2_individuals).to_csv('Individuals.csv')
