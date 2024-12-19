"""Mapping script for RD3 EMX1 to EMX2"""

from os import environ
from dotenv import load_dotenv
import molgenis.client
#from rd3tools.utils import flatten_data
import pandas as pd
#from datatable import dt, f
load_dotenv()

# connect to the environment and log in
rd3 = molgenis.client.Session(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# retrieve first n family identifiers
families = rd3.get('solverd_subjects', attributes='fid')
fids = []
for row in families:
    if 'fid' in row.keys():
        if row['fid'] not in fids:
            fids.append(row['fid'])

# get the subjects from the first 5 families
QUERY = ','.join([f"fid=q={fid}" for fid in fids[:5]])
subjects = rd3.get('solverd_subjects', q=QUERY, uploadable=True)
subjects = rd3.get('solverd_subjects', q=QUERY)

# get the subject IDs - necessary to retrieve the subject info 
IDs = [subject['subjectID'] for subject in subjects]
QUERY = ','.join([f"subjectID=q={ID}" for ID in IDs])
subjects_info = rd3.get('solverd_subjectinfo', q=QUERY)


#subjects_dt = dt.Frame(flatten_data(subjects, 'subjectID|id|value'))
#subjects_dt = pd.json_normalize(subjects, 'subjectID|id|value')
#subjects_dt = pd.json_normalize(subjects)

subject = subjects[1]
print(subject)

# map the data to the new model 
emx2_individuals = [] 
emx2_pedigree = []
emx2_datasets = []
emx2_resources = []
id = 0
pid = 0
name = "123"
for subject in subjects:
    new_subject_entry = {}
    new_pedigree_entry = {}

    # map 'subjectID' to 'id' in the individuals table
    new_subject_entry['id'] = subject['subjectID']
    
    # map 'sex1' to 'gender at birth'
    subject_sex1 = subject['sex1']['id']
    if subject_sex1 == 'M':
        new_subject_entry['gender at birth'] = 'assigned male at birth'
    elif subject_sex1 == 'F':
        new_subject_entry['gender at birth'] = 'assigned female at birth'
    else:
        print(f"Value {subject_sex1} cannot be mapped")

    # map 'dateOfBirth' to 'year of birth' and 'month of birth' 
    # DISCUSS 'age group'
    match = [info['dateOfBirth'] for info in subjects_info if 'dateOfBirth' in info and info['subjectID'] == subject['subjectID']]
    new_subject_entry['year of birth'] = "".join(map(str, match))

    # map 'fid' to 'pedigree' 
    new_subject_entry['pedigree'] = subject['fid']
    # to make this work the fid also needs to be added to the pedigree table in emx2.
    if not any (entry['identifier'] == subject['fid'] for entry in emx2_pedigree): # check for uniqueness
        new_pedigree_entry['identifier'] = subject['fid']
        emx2_pedigree.append(new_pedigree_entry)

    # map 'partOfRelease' to 'included in datasets' -- DOES NOT WORK
    # partOfReleaseList = [] # list to gather the releases
    
    # for release in subject['partOfRelease']:
    #     new_dataset_entry = {} # create new entry dict for datasets
    #     new_resource_entry = {} # new entry dict for resources
    #     partOfReleaseList.append(release['id'])
    #     if not any (entry['name'] == release['name'] for entry in emx2_datasets): # check for uniqueness
    #         new_dataset_entry['name'] = release['name']
    #         new_dataset_entry['resource'] = release['id']
    #         new_resource_entry['datasets'] = release['id']
    #         new_resource_entry['id'] = id
    #         id += 1
    #         new_resource_entry['pid'] = pid 
    #         pid += 1
    #         new_resource_entry['name'] = name
    #         name = str(int(name) + 1)
    #         new_resource_entry['type'] = 'Biobank' 
    #         emx2_datasets.append(new_dataset_entry)
    #         emx2_resources.append(new_resource_entry)
    # # add the list to the new entry 
    # #new_subject_entry['included in datasets.resource'] = ','.join(map(str, partOfReleaseList))
    # new_subject_entry['included in datasets.resource'] = partOfReleaseList

    
    # append the new entry to the individuals list
    emx2_individuals.append(new_subject_entry)
    
# write to csv 
pd.DataFrame(emx2_individuals).to_csv('Individuals.csv', index=False)
pd.DataFrame(emx2_pedigree).to_csv('Pedigree.csv', index=False)
#pd.DataFrame(emx2_datasets).to_csv('Datasets.csv', index=False)
#pd.DataFrame(emx2_resources).to_csv('Resources.csv', index=False)

