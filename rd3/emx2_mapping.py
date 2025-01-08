"""
Mapping script for RD3 EMX1 to EMX2
  
"""

from os import environ
from dotenv import load_dotenv
import molgenis.client
#from rd3tools.utils import flatten_data
import pandas as pd
load_dotenv()
from molgenis_emx2_pyclient import Client

# connect to the environment and log in
rd3 = molgenis.client.Session(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# connect to EMX2
with Client(environ['MOLGENIS_EMX2']) as emx2:
    emx2.signin(environ['MOLGENIS_EMX2_USR'], environ['MOLGENIS_EMX2_PWD'])

# retrieve first n family identifiers
families = rd3.get('solverd_subjects', attributes='fid')
fids = []
for row in families:
    if 'fid' in row.keys():
        if row['fid'] not in fids:
            fids.append(row['fid'])

# get the subjects from the first 5 families
QUERY = ','.join([f"fid=q={fid}" for fid in fids[:5]])
# add family ID that has diseases specified, so the code can be checked. 
QUERY = QUERY + ',fid=q=FAM0006076' + ',fid=q=FAM0397080'
#subjects = rd3.get('solverd_subjects', q=QUERY, uploadable=True)
subjects = rd3.get('solverd_subjects', q=QUERY)

#print(fids[:5])

# get the subject IDs - necessary to retrieve the subject info 
IDs = [subject['subjectID'] for subject in subjects]
print(IDs)
# get the subject info for the 5 families
QUERY = ','.join([f"subjectID=q={ID}" for ID in IDs])
subjects_info = rd3.get('solverd_subjectinfo', q=QUERY)
# get the sample info for the 5 families
QUERY = ','.join([f"belongsToSubject=={ID}" for ID in IDs])
samples = rd3.get('solverd_samples', q=QUERY)

# get all subject info (to check if columns are empty)
#subjects_info_tmp = rd3.get('solverd_subjectinfo')


# check if a column is empty
# column_name = 'countryOfBirth'
# if any(column_name in entry for entry in subjects_info_tmp):
#     for entry in subjects_info_tmp:
#         if column_name in entry and entry[column_name]:
#             print(entry[column_name])


#subjects_dt = dt.Frame(flatten_data(subjects, 'subjectID|id|value'))
#subjects_dt = pd.json_normalize(subjects, 'subjectID|id|value')
#subjects_dt = pd.json_normalize(subjects)

subject = subjects[1]
print(subject)

# map the data to the new model 
emx2_individuals = []
emx2_pedigree = []
#emx2_datasets = []
#emx2_resources = []
emx2_pedigree_members = []
emx2_organisations = []
emx2_disease_history = []
emx2_phenotype_observations = []
emx2_clinical_observations = []
emx2_individual_observations = []
emx2_individual_consent = []
id = 0
pid = 0
#name = "123"
for subject in subjects:
    new_individual_entry = {}
    new_pedigree_entry = {}
    new_pedigree_members_entry = {}
    # map 'subjectID' to 'id' in the individuals table
    new_individual_entry['id'] = subject['subjectID']
    
    # map 'sex1' to 'gender at birth' 
    if 'sex1' in subject:
        subject_sex1 = subject['sex1']['id']
        if subject_sex1 == 'M':
            new_individual_entry['gender at birth'] = 'assigned male at birth'
        elif subject_sex1 == 'F':
            new_individual_entry['gender at birth'] = 'assigned female at birth'
        else:
            print(f"Value {subject_sex1} cannot be mapped")

    # map 'dateOfBirth' to 'year of birth'
    # DISCUSS 'age group'
    match = [info['dateOfBirth'] for info in subjects_info if 'dateOfBirth' in info and info['subjectID'] == subject['subjectID']]
    new_individual_entry['year of birth'] = "".join(map(str, match))

    # map 'fid' to 'pedigree' 
    if 'fid' in subject:
        new_individual_entry['pedigree'] = subject['fid']
        # to make this work the fid also needs to be added to the pedigree table in emx2.
        if not any (entry['identifier'] == subject['fid'] for entry in emx2_pedigree): # check for uniqueness
            new_pedigree_entry['identifier'] = subject['fid']
            emx2_pedigree.append(new_pedigree_entry)
    
    # map 'mid' and 'fid' to Pedigree_members table
    if 'fid' in subject and 'clinical_status' in subject:
        if subject['clinical_status']:
            new_pedigree_members_entry = {}
            new_pedigree_members_entry['pedigree'] = subject['fid']
            new_pedigree_members_entry['individual'] = subject['subjectID']
            new_pedigree_members_entry['relative'] = subject['subjectID']
            new_pedigree_members_entry['relation'] = 'Patient'
            new_pedigree_members_entry['affected'] = True
            emx2_pedigree_members.append(new_pedigree_members_entry)
        if 'mid' in subject and subject['clinical_status']:
            new_pedigree_members_entry = {}
            new_pedigree_members_entry['pedigree'] = subject['fid']
            new_pedigree_members_entry['individual'] = subject['mid']['subjectID']
            new_pedigree_members_entry['relative'] = subject['subjectID']
            new_pedigree_members_entry['relation'] = 'Biological Mother'
            new_pedigree_members_entry['affected'] = False
            emx2_pedigree_members.append(new_pedigree_members_entry)
        if 'pid' in subject and subject['clinical_status']:
            new_pedigree_members_entry = {}
            new_pedigree_members_entry['pedigree'] = subject['fid']
            new_pedigree_members_entry['individual'] = subject['pid']['subjectID']
            new_pedigree_members_entry['relative'] = subject['subjectID']
            new_pedigree_members_entry['relation'] = 'Biological Father'
            new_pedigree_members_entry['affected'] = False
            emx2_pedigree_members.append(new_pedigree_members_entry)

    # TO DO: map 'organisation' and 'ERN' to 'affiliated institutions' - How do i get the organisation information? 
    # The ERN info is already in the organisations table, but the organisations aren't. 
    institutions = [] # new list to save the institutions
    resources = [] # new list to save the resources
    # This information is not present in the organisations table so it fails if this is included. 
    #new_organisation_entry = {}
    # if 'organisation' in subject: 
    #     # get the value for every organisation in the list
    #     orgs = [org['value'] for org in subject['organisation']]
    #     institutions.append(orgs)
    #     resources.append('RD3')
    #     for org in subject['organisation']:
    #         if not any (entry['resource'] == org['value'] for entry in emx2_organisations):
    #             new_organisation_entry = {}
    #             new_organisation_entry['resource'] = org['value']
    #             new_organisation_entry['id'] = org['value']
    #             new_organisation_entry['name'] = org['value']
    #             emx2_organisations.append(new_organisation_entry)
    if 'ERN' in subject and subject['ERN']:
        # get the shortname for every ERN in the list 
        ern = [org['shortname'] for org in subject['ERN']]
        institutions.append(ern)
        resources.append('RD3')
        for ern in subject['ERN']:
            if not any (entry['resource'] == ern['shortname'] for entry in emx2_organisations):
                new_organisation_entry = {}
                new_organisation_entry['resource'] = ern['shortname']
                new_organisation_entry['id'] = ern['id']
                new_organisation_entry['name'] = ern['shortname']
                emx2_organisations.append(new_organisation_entry)
    # add the organisation and ERNs (IDs)
    if all(institutions):
        #print(institutions)
        new_individual_entry['affiliated institutions.id'] = ', '.join(item[0] for item in institutions)
    # add the organisations and ERNs (resource)
    if all(resources):
        new_individual_entry['affiliated institutions.resource'] = ','.join(map(str, resources))

    # map 'disease' -- FAILS. I don't understand 'part of clinical observations'. 
    if 'disease' in subject:
        # Disease history
        for disease in subject['disease']:
            new_disease_history_entry = {}
            new_disease_history_entry['disease'] = disease['label']
            new_disease_history_entry['part of clinical observation'] = id
            id += 1
            emx2_disease_history.append(new_disease_history_entry)

        # Phenotype observations
        for phenotype in subject['phenotype']:
            new_phenotype_observation_entry = {}
            new_phenotype_observation_entry['type'] = phenotype['label']
            new_phenotype_observation_entry['part of clinical observation'] = pid
            pid += 1
            emx2_phenotype_observations.append(new_phenotype_observation_entry)

        # Clinical observations
        #new_clinical_observation_entry = {}
        #new_clinical_observation_entry['individual'] = subject['subjectID']
        #emx2_clinical_observations.append(new_clinical_observation_entry)

        # Individual observations
        new_individual_observation_entry = {}
        new_individual_observation_entry['individual'] = subject['subjectID']
        new_individual_observation_entry['diseases.disease'] = [disease['label'] for disease in subject['disease']]
        new_individual_observation_entry['phenotypes.type'] = [phenotype['label'] for phenotype in subject['phenotype']]
        emx2_individual_observations.append(new_individual_observation_entry)


    # map 'partOfRelease' to 'included in datasets' -- DOES NOT WORK: meaning of key 1, 2, 3, in resources? 
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
    # #new_individual_entry['included in datasets.resource'] = ','.join(map(str, partOfReleaseList))
    # new_individual_entry['included in datasets.resource'] = partOfReleaseList

    # map 'consent' (solverd_subjects) to  
    if 'matchMakerPermission' in subject:
        new_individual_consent_entry = {}
        new_individual_consent_entry['identifier'] = subject['subjectID'] + '-matchmaker'
        new_individual_consent_entry['Person consenting'] = subject['subjectID']
        if subject['matchMakerPermission']:
            new_individual_consent_entry['Allow recontacting'] = "Allow use in MatchMaker"
        else:
            new_individual_consent_entry['Allow recontacting'] = "No use in MatchMaker"
        emx2_individual_consent.append(new_individual_consent_entry)
    if 'noIncidentalFindings' in subject: # TO DO: See notes (word doc). Made my own category in (local) ontology. 
        new_individual_consent_entry = {}
        new_individual_consent_entry['identifier'] = subject['subjectID'] + '-reportIncidental'
        new_individual_consent_entry['Person consenting'] = subject['subjectID']
        if subject['noIncidentalFindings']:
            new_individual_consent_entry['Allow recontacting'] = "Report incidental findings back"
        else:
            new_individual_consent_entry['Allow recontacting'] = "No reporting of incidental findings"
        emx2_individual_consent.append(new_individual_consent_entry)
    if 'recontact' in subject:
        new_individual_consent_entry = {}
        new_individual_consent_entry['identifier'] = subject['subjectID'] + '-recontactIncidental'
        new_individual_consent_entry['Person consenting'] = subject['subjectID']
        #print(subject['recontact'])
        if subject['recontact']['label'] == 'Yes':
            new_individual_consent_entry['Allow recontacting'] = "Recontacting for incidental findings"
        if subject['recontact']['label'] == 'No':
            new_individual_consent_entry['Allow recontacting'] = "No recontacting for incidential findings"
        emx2_individual_consent.append(new_individual_consent_entry)    

    # TO DO: map 'solved' (solverd_subjects) to 'is solved' (Clinical observations)
    new_clinical_observation_entry = {}
    if 'solved' in subject:
        new_clinical_observation_entry['individual'] = subject['subjectID']
        new_clinical_observation_entry['is solved'] = subject['solved']
    if 'date_solved' in subject:
        new_clinical_observation_entry['date solved'] = subject['date_solved']
    emx2_clinical_observations.append(new_clinical_observation_entry)

    # map 'comments' (solverd_subjects) to 'comments' (Individuals)
    if 'comments' in subject:
        new_individual_entry['comments'] = subject['comments']

    # TO DO: does not work. The columns are added but this doesn't do anything. 
    # map 'dateRecordCreated' (solverd_subjects) to 'mg_insertedOn' (Individuals)
    new_individual_entry['mg_insertedOn'] = subject['dateRecordCreated']
    # map 'recordCreatedBy' (solverd_subjects) to 'mg_insertedBy' (Individuals)
    new_individual_entry['mg_insertedBy'] = subject['recordCreatedBy']['id']
    # map 'dateRecordUpdated' (solverd_subjects) to 'mg_updatedOn' (Individuals)
    new_individual_entry['mg_updatedOn'] = subject['dateRecordUpdated']
    # map 'wasUpdatedBy' (solverd_subjects) to 'mg_updatedBy' (Individuals)
    new_individual_entry['mg_updatedBy'] = subject['wasUpdatedBy']['id']

    # map 'sex2' (solverd_samples) to 'genotypic sex' (Individuals)
    for sample in samples: 
        if sample['belongsToSubject']['subjectID'] == subject['subjectID']:
            if 'sex2' in sample:
                if sample['sex2']['id'] == 'M':
                    new_individual_entry['genotypic sex'] = 'XY Genotype'
                elif sample['sex2']['id'] == 'F':
                    new_individual_entry['genotypic sex'] = 'XX Genotype'
                else: 
                    print('The genotypic sex could not be mapped.')
                    print(sample['sex2']['id'])
    
    # append the new entry to the individuals list
    emx2_individuals.append(new_individual_entry)

# map samples information 
emx2_biosamples = []
for sample in samples:
    new_biosamples_entry = {}
    # map 'sampleID' (solverd_samples) to 'id' (Biosamples)
    new_biosamples_entry['id'] = sample['sampleID']
    
    # map 'pathologicalState' (solverd_samples) to 'pathological state' (Biosamples)
    if 'pathologicalState' in sample:
        new_biosamples_entry['pathological state'] = sample['pathologicalState']
    else:
        new_biosamples_entry['pathological state'] = 'Unknown'

    # map 'anatomicalLocation' (solverd_samples) to 'anatomical location' (Biosamples)
    if 'anatomicalLocation' in sample:
        new_biosamples_entry['anatomical location'] = sample['anatomicalLocation']
    else: 
        new_biosamples_entry['anatomical location'] = 'Unknown'

    # map 'belongsToSubject' (solverd_samples) to 'collected from individual' (Biosamples)
    new_biosamples_entry['collected from individual'] = sample['belongsToSubject']['subjectID']

    print(len(emx2_individuals))
    # try 
    if 'sex2' in sample:
        print('true')
        new_individual_entry = {}
        new_individual_entry['genotypic sex'] = sample['sex2']['id']
        new_individual_entry['id'] = sample['belongsToSubject']['subjectID']
        emx2_individuals.append(new_individual_entry)
    print(len(emx2_individuals)) 
    
    emx2_biosamples.append(new_biosamples_entry)

    
# write to csv 
pd.DataFrame(emx2_individuals).to_csv('Individuals.csv', index=False)
pd.DataFrame(emx2_pedigree).to_csv('Pedigree.csv', index=False) 
pd.DataFrame(emx2_pedigree_members).to_csv('Pedigree members.csv', index=False)
pd.DataFrame(emx2_individual_consent).to_csv('Individual consent.csv', index=False)
#pd.DataFrame(emx2_organisations).to_csv('Organisations.csv', index=False)
#pd.DataFrame(emx2_disease_history).to_csv('Disease history.csv', index=False)
#pd.DataFrame(emx2_phenotype_observations).to_csv('Phenotype observations.csv', index=False)
#pd.DataFrame(emx2_individual_observations).to_csv('Individual observations.csv', index=False)
pd.DataFrame(emx2_clinical_observations).to_csv('Clinical observations.csv', index=False)
pd.DataFrame(emx2_biosamples).to_csv('Biosamples.csv', index=False)
#pd.DataFrame(emx2_datasets).to_csv('Datasets.csv', index=False)
#pd.DataFrame(emx2_resources).to_csv('Resources.csv', index=False)


print(subject['ERN'])
print([org['value'] for org in subject['organisation']])
print(subject['organisation'])

print([org['label'] for org in subjects[2]['phenotype']])


print(subjects[2]['organisation'])
print(subjects[2])
#print(entry['organis'] == org['value'] for entry in emx2_organisations)

institutions = [['unew-straub'], ['ERN EURO-NMD']]
print(','.join(map(str, institutions)))
print(institutions)
[','.join(map(str, institution)) for institution in institutions]
result = ', '.join(item[0] for item in institutions)
print(result)

subject=subjects[-1]
print([disease['label'] for disease in subject['disease']])


for subject in subjects: 
    if 'matchMakerPermission' in subject:
        print(subject['matchMakerPermission'])

if institutions:
    print('check')

tmp = [[]]
if all(tmp):
    print('test')

subject = subjects[-1]
[org['shortname'] for org in subject['ERN']]
subject['dateRecordCreated']

sample = samples[1]
print(sample['belongsToSubject']['subjectID'])

print(sample)
samples[samples['belongsToSubject']]

['subjectID'] == subject['subjectID']

[sample['sex2'] for sample in samples and 'sex2' in sample and sample['belongsToSubject']['subjectID'] == subject['subjectID']]

