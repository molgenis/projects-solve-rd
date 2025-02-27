"""
Mapping script for RD3 EMX1 to EMX2

"""

from molgenis_emx2_pyclient import Client
from os import environ, path
from dotenv import load_dotenv
import molgenis.client
import pandas as pd
load_dotenv()
import re
import json

# connect to the RD3 EMX1 environment and log in
rd3 = molgenis.client.Session(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# connect to EMX2
# emx2 = Client(
#     'http://localhost:8080/',
#     schema='rd3',
#     token=environ['MOLGENIS_EMX2_TOKEN']
# )

emx2 = Client(
    'https://erdera.molgeniscloud.org',
    schema='RD3v2',
    token=environ['MOLGENIS_EMX2_ERDERA_TOKEN']
)

# schema = 'rd3' # for localhost
schema = 'RD3v2' # for erdera server
emx2.default_schema = schema  # set default schema

##################################################
# This was used to get the subset of data. Based on this subset, this mapping script
# was written. This is no longer used. The mapping was done on all data. 

# # retrieve first n family identifiers
# families = rd3.get('solverd_subjects', attributes='fid', batch_size=10000)
# fids = []
# for row in families:
#     if 'fid' in row.keys():
#         if row['fid'] not in fids:
#             fids.append(row['fid'])

# # get the subjects from the first 5 families (1st row) or 20 random families (2nd row)
# QUERY = ','.join([f"fid=q={fid}" for fid in fids[:5]])
# # QUERY = ','.join([f"fid=q={fid}" for fid in random.sample(fids, 20)])

# # add family ID that has specific variables with data, so the code can be checked.
# QUERY = QUERY + (
#     f',fid=q={environ['FAM_ID_1']}' + 
#     f',fid=q={environ['FAM_ID_2']}' + 
#     f',fid=q={environ['FAM_ID_3']}' + 
#     f',fid=q={environ['FAM_ID_4']}' + 
#     f',fid=q={environ['FAM_ID_5']}' +
#     f',fid=q={environ['FAM_ID_6']}'
# )

# subjects = rd3.get('solverd_subjects', q=QUERY)

# # get the subject IDs to retrieve the subject info
# IDs = [subject['subjectID'] for subject in subjects]
# # get the subject info for the families
# QUERY = ','.join([f"subjectID=q={ID}" for ID in IDs])
# subjects_info = rd3.get('solverd_subjectinfo', q=QUERY)

# # get the samples for the families
# QUERY = ','.join([f"belongsToSubject=={ID}" for ID in IDs])
# samples = rd3.get('solverd_samples', q=QUERY)

# # get the labinfo (experiments) for the families
# sample_ids = [sample['sampleID'] for sample in samples]
# QUERY = ','.join([f'sampleID=={sample_id}' for sample_id in sample_ids])
# labinfos = rd3.get('solverd_labinfo', q=QUERY)

# get the files for the families 
# Retrieving the files data for all IDs at once takes too long. 
# thus, divide the IDs in batches and retrieve the data per batch. 
# this data is collected and written to a file. This file can
# consequently be used for the mapping. 

# files_solveRD = []

# # Divide the IDs into batches to speed up the process
# ID_batches = [IDs[i:i + 2] for i in range(0, len(IDs), 2)]
# # loop through the batches
# for ID_batch in ID_batches:
#     # make a query for only the IDs in this batch
#     QUERY = ','.join([f"subjectID=={ID}" for ID in ID_batch])
#     # retrieve the files for this batch
#     files = rd3.get('solverd_files', q=QUERY, batch_size=10000)
#     # append the files to a list to gather the files for all IDs
#     files_solveRD.append(files)

# # unpack the list - it consists now of lists with dictionaries
# flattened_files = [item for sublist in files_solveRD for item in sublist]
# # create a dataframe from the flattened list
# files_solveRD_df = pd.DataFrame(flattened_files)
# # write to csv
# files_solveRD_df.to_csv('Files_solveRD.csv', index=False)

##################################################
# read the complete datasets

# retrieve subjects and subject information from server - this might be updated
subjects = rd3.get('solverd_subjects', batch_size=5000)
subjects_info = rd3.get('solverd_subjectinfo', batch_size=5000)

# get samples from solve-RD
with open('samples_17022025.json', 'r') as file:
    samples = json.load(file)

# get experiments from solve-RD
with open('experiments_17022025.json', 'r') as file:
    labinfos = json.load(file)

# get experiments from solve-RD
with open('files_11022025.json', 'r') as file:
    files = json.load(file)

##################################################
# First empty the current database because the uploads don't replace but add to the existing tables. 

# truncate the tables
emx2.truncate(table='Phenotype observations', schema=schema)
emx2.truncate(table='Disease history', schema=schema)
emx2.truncate(table='Clinical observations', schema=schema)
emx2.truncate(table='Individual consent', schema=schema)
emx2.truncate(table='Individual observations', schema=schema)
emx2.truncate(table='Pedigree members', schema=schema)
# Delete files in batches
# for batch in range(0, len(files), 1000):
#     emx2.delete_records(table='Files', schema=schema, data=files[batch:batch+1000])
# emx2.truncate(table='Files', schema=schema)
emx2.truncate(table='Sequencing runs', schema=schema)
emx2.truncate(table='Sample preparations', schema=schema)
emx2.truncate(table='Protocol activity', schema=schema)
emx2.truncate(table='Biosamples', schema=schema)
emx2.truncate(table='Individuals', schema=schema)
emx2.truncate(table='Pedigree', schema=schema)

##################################################
# Migrate subjects (solverd_subjects) to the new model

# initialize lists for the tables in EMX2
emx2_individuals = []
emx2_pedigree = []
emx2_pedigree_members = []
emx2_clinical_observations = []
emx2_individual_consent = []
# keep track of the parents' and patients' IDs.
track_mothers = {}
track_fathers = {}
patients = []
# loop through the subjects in RD3
for subject in subjects:
    new_individual_entry = {}

    # map 'subjectID' (solverd_subjects) to 'id' (Individuals)
    new_individual_entry['id'] = subject['subjectID']

    # map 'sex1' (solverd_subjects) to 'gender at birth' (Individuals)
    if 'sex1' in subject:
        subject_sex1 = subject['sex1']['id']
        if subject_sex1 == 'M':
            new_individual_entry['gender at birth'] = 'assigned male at birth'
        elif subject_sex1 == 'F':
            new_individual_entry['gender at birth'] = 'assigned female at birth'
        else:
            print(f"Gender at birth value {subject_sex1} cannot be mapped for individual {subject['subjectID']}")

    # map 'dateOfBirth' (solverd_subjects) to 'year of birth' (Individuals)
    match = [info['dateOfBirth']
             for info in subjects_info if 'dateOfBirth' in info and info['subjectID'] == subject['subjectID']]
    new_individual_entry['year of birth'] = "".join(map(str, match))

    # map 'fid' (solverd_subjects) to 'pedigree' (Individuals) and 'identifier' (Pedigree)
    if 'fid' in subject:
        new_pedigree_entry = {} # initialize new entry
        new_individual_entry['pedigree'] = subject['fid']
        # to make this work the fid also needs to be added to the pedigree table in emx2.
        # check for uniqueness
        if not any(entry['identifier'] == subject['fid'] for entry in emx2_pedigree):
            new_pedigree_entry['identifier'] = subject['fid']
            emx2_pedigree.append(new_pedigree_entry)

    ####
    # Mapping pedigree. 
    # If an individual has either a maternal or paternal ID, this person is a patient.
    # If an individual is a parent and has a parent, a grandparent relationship is determined.
    # If an individual is a patient and has a child, the individual is added multiple times to the 
    #   pedigree members table: both as a patient (with itself as the relative) and as a parent
    #   (with the child as the relative).
    # If an individual has a family ID, but no other information, the person is added to the table
    #   with itself as the relative. This happens later in the code (lines 427 - 441).
    # If individuals have the same mother and father, they are mapped as full siblings. This also
    #   happens later in the code (lines 442 - 467). 
    # If a family only has one member, we assume this individual is a patient. This is done at the end.
    ####

    if 'fid' in subject:
        # Map Patient - if individual has a parent (maternal or paternal), the person is a patient.
        if 'pid' in subject or 'mid' in subject:
            new_pedigree_members_entry = {}
            new_pedigree_members_entry['pedigree'] = subject['fid']
            new_pedigree_members_entry['individual'] = subject['subjectID']
            if 'clinical_status' in subject:
                new_pedigree_members_entry['affected'] = subject['clinical_status']
            new_pedigree_members_entry['relative'] = subject['subjectID']
            new_pedigree_members_entry['relation'] = 'Patient'
            emx2_pedigree_members.append(new_pedigree_members_entry)
            # keep track of the patients
            patients.append(subject['subjectID'])
        # Map Parents - based on MaternalID and PaternalID
        for subj in subjects:
            if subj['subjectID'] == subject['subjectID']:
                continue
            new_pedigree_members_entry = {}
            new_pedigree_members_entry['pedigree'] = subject['fid']
            new_pedigree_members_entry['individual'] = subject['subjectID']
            if 'clinical_status' in subject:
                new_pedigree_members_entry['affected'] = subject['clinical_status']
            # Map Biological Mother
            if 'mid' in subj and subj['mid']['subjectID'] == subject['subjectID']:
                new_pedigree_members_entry['relative'] = subj['subjectID']
                new_pedigree_members_entry['relation'] = "Biological Mother"
                # if 'mid' in subject or 'pid' in subject:
                    # print(f'Individual has a parent and is a parent for ind {subject['subjectID']}')
                # add the child and mother IDs to the dictionary
                track_mothers[subj['subjectID']] = subject['subjectID']
            # Map Biological Father
            elif 'pid' in subj and subj['pid']['subjectID'] == subject['subjectID']:
                new_pedigree_members_entry['relative'] = subj['subjectID']
                new_pedigree_members_entry['relation'] = "Biological Father"
                # add the child and father's IDs to the dictionary
                track_fathers[subj['subjectID']] = subject['subjectID']
                # if 'mid' in subject or 'pid' in subject:
                    # print(f'Individual has a parent and is a parent for ind {subject['subjectID']}')
            if 'relative' in new_pedigree_members_entry: # only append new entry if individual has a parent
                emx2_pedigree_members.append(new_pedigree_members_entry)
        # Map maternal grandparents
        if subject['subjectID'] in patients and subject['subjectID'] in track_mothers.values():
            # Map Biological Maternal Grandmother
            if 'mid' in subject:
                # get the identifier for the grandchild
                grandchild_ids = [child_id for child_id, value in track_mothers.items() if value == subject['subjectID']]
                for grandchild_id in grandchild_ids:
                    new_pedigree_members_entry = {}
                    new_pedigree_members_entry['pedigree'] = subject['fid']
                    new_pedigree_members_entry['individual'] = subject['mid']['subjectID']
                    # if 'clinical_status' in subject:
                    clinical_status = [subject_clin['clinical_status'] for subject_clin in subjects if(
                        subject_clin['subjectID'] == subject['mid']['subjectID']) if (
                            'clinical_status' in subject_clin)]
                    if clinical_status:
                        new_pedigree_members_entry['affected'] = clinical_status[0]
                    new_pedigree_members_entry['relative'] = grandchild_id
                    new_pedigree_members_entry['relation'] = "Biological Maternal Grandmother"
                    emx2_pedigree_members.append(new_pedigree_members_entry)
            # Map Biological Maternal Grandfather
            if 'pid' in subject:
                # get the identifier for the grandchild
                grandchild_ids = [child_id for child_id, value in track_mothers.items() if value == subject['subjectID']]
                for grandchild_id in grandchild_ids:
                    new_pedigree_members_entry = {}
                    new_pedigree_members_entry['pedigree'] = subject['fid']
                    new_pedigree_members_entry['individual'] = subject['pid']['subjectID']
                    # retrieve clinical status of grandparent
                    clinical_status = [subject_clin['clinical_status'] for subject_clin in subjects if(
                        subject_clin['subjectID'] == subject['pid']['subjectID']) if (
                            'clinical_status' in subject_clin)]
                    if clinical_status:
                        new_pedigree_members_entry['affected'] = clinical_status[0]
                    new_pedigree_members_entry['relative'] = grandchild_id
                    new_pedigree_members_entry['relation'] = "Biological Maternal Grandfather"
                    emx2_pedigree_members.append(new_pedigree_members_entry)
        # Map Paternal Grandparents
        if subject['subjectID'] in patients and subject['subjectID'] in track_fathers.values():
            # Map Biological Paternal Grandmother
            if 'mid' in subject:
                # get the identifier for the grandchild
                grandchild_ids = [child_id for child_id, value in track_fathers.items() if value == subject['subjectID']]
                for grandchild_id in grandchild_ids:
                    new_pedigree_members_entry = {}
                    new_pedigree_members_entry['pedigree'] = subject['fid']
                    new_pedigree_members_entry['individual'] = subject['mid']['subjectID']
                    # retrieve clinical status of grandparent
                    clinical_status = [subject_clin['clinical_status'] for subject_clin in subjects if(
                        subject_clin['subjectID'] == subject['mid']['subjectID']) if(
                            'clinical_status' in subject_clin)]
                if clinical_status:
                    new_pedigree_members_entry['affected'] = clinical_status[0]
                new_pedigree_members_entry['relative'] = grandchild_id
                new_pedigree_members_entry['relation'] = "Biological Paternal Grandmother"
                emx2_pedigree_members.append(new_pedigree_members_entry)
            # Map Biological Paternal Grandfather
            if 'pid' in subject: 
                # get the identifier for the grandchild
                grandchild_ids = [child_id for child_id, value in track_fathers.items() if value == subject['subjectID']]
                for grandchild_id in grandchild_ids:
                    new_pedigree_members_entry = {}
                    new_pedigree_members_entry['pedigree'] = subject['fid']
                    new_pedigree_members_entry['individual'] = subject['pid']['subjectID']
                    # retrieve clinical status of grandparent
                    clinical_status = [subject_clin['clinical_status'] for subject_clin in subjects if(
                        subject_clin['subjectID'] == subject['pid']['subjectID']) if(
                            'clinical_status' in subject_clin)]
                    if clinical_status:
                        new_pedigree_members_entry['affected'] = clinical_status[0]
                    new_pedigree_members_entry['relative'] = grandchild_id
                    new_pedigree_members_entry['relation'] = "Biological Paternal Grandfather"
                    emx2_pedigree_members.append(new_pedigree_members_entry)

    # map 'organisation' and 'ERN' (solverd_subjects) to 'affiliated institutions' (Individuals)
    institutions = []  # new list to save the institutions
    resources = []  # new list to save the resources
    new_organisation_entry = {}
    # check if organisations are present for this subject
    if 'organisation' in subject and subject['organisation']:
        # get the value for every organisation in the list
        orgs = [org['value'] for org in subject['organisation']]
        institutions.append(orgs)
        resources.append('RD3')
    # check if there are any ERNs present for this subject
    if 'ERN' in subject and subject['ERN']:
        # get the shortname for every ERN in the list
        ern = [org['shortname'] for org in subject['ERN']]
        institutions.append(ern)
        resources.append('RD3')
    # add the organisation and ERNs (IDs)
    if all(institutions):  # make sure it is not empty
        new_individual_entry['affiliated institutions.id'] = ', '.join(
            item[0] for item in institutions)
    # add the organisations and ERNs (resource)
    if all(resources):  # make sure it is not empty
        new_individual_entry['affiliated institutions.resource'] = ','.join(
            map(str, resources))

    # create clinical and individual observations
    # Clinical observations: map identifier and solved info
    new_clinical_observation_entry = {} # initialize new entry
    new_clinical_observation_entry['individual'] = subject['subjectID']
    if 'solved' in subject:
        new_clinical_observation_entry['is solved'] = subject['solved']
    if 'date_solved' in subject:
        new_clinical_observation_entry['date solved'] = subject['date_solved']
    emx2_clinical_observations.append(new_clinical_observation_entry)
   
    # map to 'included in datasets' (Individuals)
    resourcesList = []  # list to gather resources
    namesList = []  # list to gather the names
    # map 'partOfRelease
    if 'partOfRelease' in subject:
        # loop through the releases of this subject
        for release in subject['partOfRelease']:
            resourcesList.append('RD3')
            namesList.append(release['id'])
    # map 'retracted' (solverd_subjects)
    if 'retracted' in subject and subject['retracted']['id'] == 'Y':
        resourcesList.append("RD3")
        namesList.append('Retracted')
    # map 'includedInDatasets' (solverd_subjects)
    if 'includedInDatasets' in subject:
        for dataset in subject['includedInDatasets']:
            resourcesList.append('RD3')
            namesList.append(dataset['id'])
    # add the lists to the new entry
    new_individual_entry['included in datasets.resource'] = ','.join(
        resourcesList)
    new_individual_entry['included in datasets.name'] = ','.join(
        map(str, namesList))

    # map 'consent' (solverd_subjects) to Individual consent 
    if 'matchMakerPermission' in subject:
        new_individual_consent_entry = {} 
        new_individual_consent_entry['identifier'] = subject['subjectID'] + '-matchmaker'
        new_individual_consent_entry['Person consenting'] = subject['subjectID']
        if subject['matchMakerPermission']:
            new_individual_consent_entry['Allow recontacting'] = "Allow use in MatchMaker"
        else:
            new_individual_consent_entry['Allow recontacting'] = "No use in MatchMaker"
        emx2_individual_consent.append(new_individual_consent_entry)
    if 'noIncidentalFindings' in subject:
        new_individual_consent_entry = {}
        new_individual_consent_entry['identifier'] = subject['subjectID'] + \
            '-reportIncidental'
        new_individual_consent_entry['Person consenting'] = subject['subjectID']
        if subject['noIncidentalFindings']:
            new_individual_consent_entry['Allow recontacting'] = "Report incidental findings back"
        else:
            new_individual_consent_entry['Allow recontacting'] = "No reporting of incidental findings"
        emx2_individual_consent.append(new_individual_consent_entry)
    if 'recontact' in subject:
        new_individual_consent_entry = {}
        new_individual_consent_entry['identifier'] = subject['subjectID'] + \
            '-recontactIncidental'
        new_individual_consent_entry['Person consenting'] = subject['subjectID']
        if subject['recontact']['label'] == 'Yes':
            new_individual_consent_entry['Allow recontacting'] = "Recontacting for incidental findings"
        if subject['recontact']['label'] == 'No':
            new_individual_consent_entry['Allow recontacting'] = "No recontacting for incidential findings"
        emx2_individual_consent.append(new_individual_consent_entry)

    # map 'comments' (solverd_subjects) to 'comments' (Individuals)
    if 'comments' in subject:
        new_individual_entry['comments'] = subject['comments']

    # map 'sex2' (solverd_samples) to 'genotypic sex' (Individuals)
    for sample in samples:
        if 'belongsToSubject' in sample:
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

# Map the individuals that do have a family id but no other information about the relationships
# gather all individuals in the current pedigree members table
# Additionally map sibling relationships. 
individuals_in_pedigree = [member['individual'] for member in emx2_pedigree_members]
# loop through the subjects
for subject in subjects:
    # if the subject has a family ID and is not yet an individual in the list, it needs to be added 
    if 'fid' in subject and subject['subjectID'] not in individuals_in_pedigree:
        new_pedigree_members_entry = {}
        new_pedigree_members_entry['pedigree'] = subject['fid']
        new_pedigree_members_entry['individual'] = subject['subjectID']
        if 'clinical_status' in subject:
            new_pedigree_members_entry['affected'] = subject['clinical_status']
        new_pedigree_members_entry['relative'] = subject['subjectID'] # itself
        emx2_pedigree_members.append(new_pedigree_members_entry)
    # Map Full Sibling relationships
    # first, check if an individual has both parents' IDs and check if the sex is a known category.
    if 'mid' and 'pid' in subject and 'sex1' in subject and subject['sex1']['id'] in ['F', 'M']: 
        # get the parents' IDs
        mother = track_mothers.get(subject['subjectID'])
        father = track_fathers.get(subject['subjectID'])

        # collect the siblings for the individual
        siblings = []
        for child in track_mothers:
            if track_mothers.get(child) == mother and track_fathers.get(child) == father and child != subject['subjectID']:
                siblings.append(child)

        # loop through the siblings and add to the list of pedigree members
        for sibling in siblings:
            new_pedigree_members_entry = {}
            new_pedigree_members_entry['pedigree'] = subject['fid']
            new_pedigree_members_entry['individual'] = subject['subjectID']
            if 'clinical_status' in subject:
                new_pedigree_members_entry['affected'] = subject['clinical_status']
            new_pedigree_members_entry['relative'] = sibling
            if subject['sex1']['id'] == 'F':
                new_pedigree_members_entry['relation'] = 'Full Sister'
            if subject['sex1']['id'] == 'M':
                new_pedigree_members_entry['relation'] = 'Full Brother'
            emx2_pedigree_members.append(new_pedigree_members_entry)
        
# get all family IDs
families = [member['pedigree'] for member in emx2_pedigree_members]
# get the family IDs with only one member 
uniques = [family for family in families if families.count(family) == 1]
# there are four familyIDs that consist of two families,
# these should be left as is. 
for famID in uniques[:]:
    if len(famID.split(',')) > 1:
        uniques.remove(famID) # remove from the list 

# set the individual's relation to Patient in these families
for member in emx2_pedigree_members:
    if member['pedigree'] in uniques:
        member['relation'] = 'Patient'

# to check - unnecessary
# pd.DataFrame(emx2_pedigree_members).to_csv('Pedigree members.csv', index=False)

# save and upload the newly made tables
emx2.save_schema(table="Pedigree", data=emx2_pedigree)
emx2.save_schema(table="Individuals", data=emx2_individuals)
emx2.save_schema(table="Pedigree members", data=emx2_pedigree_members)
emx2.save_schema(table="Clinical observations",
                 data=emx2_clinical_observations)
emx2.save_schema(table="Individual consent",
                 data=emx2_individual_consent)
# obtain the clinical observations table (with automatically generated ID)
clinicalObs = emx2.get(schema=schema, table="Clinical observations")


# again loop through subjects to map phenotype and diseases
# this is done seperately because the IDs first need to be automatically created when uploading clinical observations
emx2_disease_history = []
emx2_phenotype_observations = []
for subject in subjects:
    # mapping 'disease' (solverd_subjects) to Disease history
    if 'disease' in subject and subject['disease']:
        for disease in subject['disease']:
            new_disease_history_entry = {}
            new_disease_history_entry['disease'] = disease['label']
            for obs in clinicalObs:
                if obs['individual'] == subject['subjectID']:
                    new_disease_history_entry['part of clinical observation'] = obs['id']
            emx2_disease_history.append(new_disease_history_entry)
    # mapping 'phenotype' (solverd_subjects) to Phenotype observations (excluded = False)
    if 'phenotype' in subject and subject['phenotype']:
        for phenotype in subject['phenotype']:
            new_phenotype_observation_entry = {}
            new_phenotype_observation_entry['type'] = phenotype['label']
            for obs in clinicalObs:
                if obs['individual'] == subject['subjectID']:
                    new_phenotype_observation_entry['part of clinical observation'] = obs['id']
                    new_phenotype_observation_entry['excluded'] = False
            emx2_phenotype_observations.append(new_phenotype_observation_entry)
    # mapping the 'hasNotPhenotype' (solverd_subjects) to Phenotype observations (excluded = True)
    if 'hasNotPhenotype' in subject:
        for notPhenotype in subject['hasNotPhenotype'] and subject['hasNotPhenotype']:
            new_phenotype_observation_entry = {}
            new_phenotype_observation_entry['type'] = notPhenotype['label']
            for obs in clinicalObs:
                if obs['individual'] == subject['subjectID']:
                    new_phenotype_observation_entry['part of clinical observation'] = obs['id']
                    new_phenotype_observation_entry['excluded'] = True
            emx2_phenotype_observations.append(new_phenotype_observation_entry)
    # map 'ageOfOnset' (solverd_subject_info) to ageOfOnset (Disease history)
    # find the information of the subject with an ageOfOnset. 
    match = [info for info in subjects_info if 'ageOfOnset' in info and info['subjectID'] == subject['subjectID']]
    if match: 
        # make a dict of the IDs and the auto IDs (part of clinical observation)
        tmp = {obs['individual']:obs['id'] for obs in clinicalObs}
        # get the auto ID based on the ID from the individual with an age of onset 
        target_part_of_clinical_obs = tmp.get(match[0]['subjectID'])
        # find this individual in the disease history list based on auto ID. 
        for disease_history_entry in emx2_disease_history:
            if disease_history_entry['part of clinical observation'] == target_part_of_clinical_obs:
                # update this entry in the dict with the age at onset
                disease_history_entry.update({'age group at onset':match[0]['ageOfOnset']['label']})
                
# to check
# pd.DataFrame(emx2_disease_history).to_csv('Disease history.csv', index=False)

# save and upload the disease history and phenotype observation tables
emx2.save_schema(table="Disease history", data=emx2_disease_history)
emx2.save_schema(table="Phenotype observations",
                 data=emx2_phenotype_observations)

#######################################################################
#  Migrate (bio)samples information to the new model

# initialize list to gather the biosamples info for the new model
emx2_biosamples = []
for sample in samples:
    new_biosamples_entry = {}
    # map 'sampleID' (solverd_samples) to 'id' (Biosamples)
    new_biosamples_entry['id'] = sample['sampleID']

    # map 'pathologicalState' (solverd_samples) to 'pathological state' (Biosamples)
    if 'pathologicalState' in sample:
        new_biosamples_entry['pathological state'] = sample['pathologicalState']['value']

    # map 'anatomicalLocation' (solverd_samples) to 'anatomical location' (Biosamples)
    if 'anatomicalLocation' in sample:
        new_biosamples_entry['anatomical location'] = sample['anatomicalLocation']['id']
        if sample['anatomicalLocation']['label'] == 'Other':
            new_biosamples_entry['anatomical location other'] = sample['anatomicalLocationComment']

    # map 'belongsToSubject' (solverd_samples) to 'collected from individual' (Biosamples)
    if 'belongsToSubject' in sample:
        new_biosamples_entry['collected from individual'] = sample['belongsToSubject']['subjectID']

    # map 'sex2' (solverd_samples) to 'genotypic sex' (solverd_subjects)
    if 'sex2' in sample:
        new_individual_entry = {}
        new_individual_entry['genotypic sex'] = sample['sex2']['id']
        new_individual_entry['id'] = sample['belongsToSubject']['subjectID']
        emx2_individuals.append(new_individual_entry)

    # map 'tissueType' (solverd_samples) to 'tissue type' (Biosamples) 
    if 'tissueType' in sample:
       new_biosamples_entry['tissue type'] = sample['tissueType']['id']

    # map 'materialType' (solverd_samples) to 'material type' (Biosamples)
    if 'materialType' in sample:
        types = []
        for type in sample['materialType']:
            types.append(type['label'])
        new_biosamples_entry['material type'] = ",".join(map(str, types))

    # map 'organisation' (solverd_samples) to 'collected at organisation' (Biosamples)
    if 'organisation' in sample:
        new_biosamples_entry['collected at organisation.resource'] = 'RD3'
        new_biosamples_entry['collected at organisation.id'] = sample['organisation']['value']

    # map 'ERN' (solverd_samples) to 'affiliated organisations' (Biosamples)
    if 'ERN' in sample:
        new_biosamples_entry['affiliated organisations.resource'] = 'RD3'
        new_biosamples_entry['affiliated organisations.id'] = sample['ERN']['shortname']

    # map to 'included in datasets' (Biosamples)
    resourcesList = [] # gathers all resources
    namesList = [] # gathers the names
    # map 'retracted' (solverd_samples) to 'included in datasets'
    if 'retracted' in sample and sample['retracted']['id'] == 'Y': 
        resourcesList.append('RD3')
        namesList.append('Retracted')

    # map 'batch' (solverd_samples) to 'included in datasets'
    if 'batch' in sample:
        for batch in sample['batch'].split(","): # split on comma in the case of mulitple batches
            resourcesList.append('RD3')
            namesList.append(batch)

    # map 'partOfRelease' (solverd_samples) to 'included in datasets'
    # gather the resources and releases (names)
    if 'partOfRelease' in sample:
        for release in sample['partOfRelease']:
            resourcesList.append('RD3')
            namesList.append(release['id'])

    # map 'includedInDatasets' (solverd_samples) to 'included in datasets'
    if 'includedInDatasets' in sample:
        for dataset in sample['includedInDatasets']:
            resourcesList.append('RD3')
            namesList.append(dataset['id'])
            
    # add the lists to a new entry 
    new_biosamples_entry['included in datasets.resource'] = ",".join(map(str, resourcesList))
    new_biosamples_entry['included in datasets.name'] = ",".join(map(str, namesList))

    # map 'alternativeIdentifier' (solverd_samples) to 'alternate identifiers' (Biosamples)
    if 'alternativeIdentifier' in sample:
        new_biosamples_entry['alternate ids'] = sample['alternativeIdentifier']

    # map 'flag' (solverd_samples) to 'failed quality control' (Biosamples)
    if 'flag' in sample:
        new_biosamples_entry['failed quality control'] = sample['flag']

    # map 'percentageTumorCells' (solverd_samples) to 'percentage tumor cells' (Biosamples)
    if 'percentageTumorCells' in sample:
        new_biosamples_entry['percentage tumor cells'] = sample['percentageTumorCells']

    # map 'comments' (solverd_samples) to 'comments' (Biosamples)
    if 'comments' in sample:
        new_biosamples_entry['comments'] = sample['comments']

    # append the new biosample entry to the list
    emx2_biosamples.append(new_biosamples_entry)

# to check
# pd.DataFrame(emx2_biosamples).to_csv('Biosamples.csv', index=False)

# save and upload the biosamples table
emx2.save_schema(table="Biosamples", data=emx2_biosamples)

##################################################
# Migrate the Experiments (solverd_labinfo) to the new model

# initialize lists for the data for the new model
emx2_sample_preparation = []
emx2_sequencing_runs = []
# loop through the experiment info
for labinfo in labinfos:
    new_sample_preparation_entry = {}
    new_sequencing_runs_entry = {}

    # map 'experimentID' (solverd_labinfo) to 'identifier' (Sequencing runs and sample preparation) 
    new_sequencing_runs_entry['identifier'] = "seq_" + labinfo['experimentID']
    new_sample_preparation_entry['identifier'] = "sample_" + labinfo['experimentID']

    # map 'sampleID' (solverd_labinfo) to 'input samples' (Protocol parameters)
    if 'sampleID' in labinfo:
        sampleIDs = []
        for sample in labinfo['sampleID']:
            sampleIDs.append(sample['sampleID'])
        new_sequencing_runs_entry['input samples'] = ",".join(map(str, sampleIDs))
        new_sample_preparation_entry['input samples'] = ",".join(map(str, sampleIDs))

    # map 'capture' (solverd_labinfo) to 'library preparation' (Sample preparation)
    if 'capture' in labinfo:
        new_sample_preparation_entry['target enrichment kit'] = labinfo['capture']

    # map 'libraryType' (solverd_labinfo) to 'librarySource' (Sequencing runs)
    # data only contains types Genomic and Transcriptomic
    if 'libraryType' in labinfo:
        # get the library type and convert to lowercase
        libType = labinfo['libraryType']['id'].lower()
        # add the word source as is defined in the ontology
        libType += " source"
        # map
        new_sequencing_runs_entry['library source'] = libType
    
    # map 'library' (solverd_labinfo) to 'libraryLayout' (Sequencing runs)
    # data only contains 1 and 2, 1 is always paired and 2 always single.
    if 'library' in labinfo and labinfo['library']:
        layout = []
        for lib in labinfo['library']:
            if lib['id'] == "1":
                layout.append("PAIRED")
            elif lib['id'] == "2":
                layout.append("SINGLE")
            else:
                print(f"library {lib['id']} could not be mapped.")
        new_sequencing_runs_entry['library layout'] = ",".join(map(str, layout))
    
    # map 'sequencingCentre' (solverd_labinfo) to 'sequencing centre' (Sequencing runs)
    if 'sequencingCentre' in labinfo:
        new_sequencing_runs_entry['sequencing centre.resource'] = 'RD3'
        new_sequencing_runs_entry['sequencing centre.id'] = labinfo['sequencingCentre']['value']

    # map 'sequencer' (solverd_labinfo) to 'platform' and 'platform' (Sequencing runs)
    # make a dictionary to use for mapping the platform to the ontology term
    platform_dict = {
        "Illumina": "Illumina platform",
        "Sequel": "PacBio platform",
        "DNBSEQ": "Complete Genomics platform"
    }
    if 'sequencer' in labinfo:
        # gather first word (before either a space or a dash) - this word is the platform
        pattern = re.compile(r'^.*?(?=-)|^\S+')
        sequencer = labinfo['sequencer']
        match = pattern.match(sequencer)
        if match: 
            platform = match.group()
        new_sequencing_runs_entry['platform'] = platform_dict[platform]

        # also map to 'platform model' (Sequencing runs)
        pattern = re.compile(r'^DNBSEQ-\w{1}\d+$')
        if not pattern.match(labinfo['sequencer']): # don't map all DNBSEQ sequencers
            # differently named in ontology 
            if labinfo['sequencer'] == 'Sequel II': 
                new_sequencing_runs_entry['platform model'] = 'PacBio Sequel II'
            else: # the platform model is as is in the ontology and can thus be mapped
                new_sequencing_runs_entry['platform model'] = labinfo['sequencer'].rstrip() # strip ending whitespace

    # map 'seqType' (solverd_labinfo) to 'library strategy' (Sequencing runs)
    if 'seqType' in labinfo and labinfo['seqType']:
        for seqType in labinfo['seqType']:
            if seqType['label'] == 'ssRNA-seq': # should not be capatalized (ontology)
                new_sequencing_runs_entry['library strategy'] = seqType['label']
            else:
                # capitalize first letter in the strategy (as per ontology)
                new_sequencing_runs_entry['library strategy'] = seqType['label'].title()

    # map 'mean_cov' (solverd_labinfo) to 'mean read depth' (Sequencing runs) 
    if 'mean_cov' in labinfo:
        new_sequencing_runs_entry['mean read depth'] = labinfo['mean_cov']

    # map 'median_cov' (solverd_labinfo) to median read depth' (Sequencing runs)
    if 'median_cov' in labinfo:
        new_sequencing_runs_entry['median read depth'] = labinfo['median_cov']

    # map 'c20' (solverd_labinfo) to 'percentage Tr20' (Sequencing runs)
    if 'c20' in labinfo:
        new_sequencing_runs_entry['percentage Tr20'] = labinfo['c20']

    # map to 'included in datasets' (Sequencing runs and Sample preparations)

    # initialize lists to gather the datasets
    resourcesList = []
    namesList = []
    
    # map 'partOfRelease'
    if 'partOfRelease' in labinfo:
        for release in labinfo['partOfRelease']:
            resourcesList.append('RD3')
            namesList.append(release['id'])

    # map 'retracted' (solverd_labinfo) 
    # check if the individual needs to be retracted from the tables.
    if 'retracted' in labinfo and labinfo['retracted']['id'] == 'Y':
        # add the lists to the new individual entry
        resourcesList.append("RD3")
        namesList.append('Retracted')

    # map 'includedInDatasets' (solverd_labinfo) 
    if 'includedInDatasets' in labinfo:
        for dataset in labinfo['includedInDatasets']:
            resourcesList.append('RD3')
            namesList.append(dataset['id'])

    # add the lists to the Sequencing runs entry
    new_sequencing_runs_entry['included in datasets.resource'] = ','.join(
        resourcesList)
    new_sequencing_runs_entry['included in datasets.name'] = ','.join(
        map(str, namesList))
    # append the lists to the Sample preparation entry
    new_sample_preparation_entry['included in datasets.resource'] = ','.join(
        resourcesList)
    new_sample_preparation_entry['included in datasets.name'] = ','.join(
        map(str, namesList))
    
    # map 'comments' (solverd_labinfo) to 'comments' (Sample preparation and Sequencing runs)
    if 'comments' in labinfo:
        new_sample_preparation_entry['comments'] = labinfo['comments']
        new_sequencing_runs_entry['comments'] = labinfo['comments']

    # append the new entry to the sample preparation list
    if new_sample_preparation_entry: # if not empty
        emx2_sample_preparation.append(new_sample_preparation_entry)
    # # append the new entry to the sequencing runs list
    if new_sequencing_runs_entry: # if not empty
        emx2_sequencing_runs.append(new_sequencing_runs_entry)

# to check 
# pd.DataFrame(emx2_sequencing_runs).to_csv('Sequencing runs.csv', index=False)
# pd.DataFrame(emx2_sample_preparation).to_csv('Sample preparations.csv', index=False)

# save and upload the sample preparation table
emx2.save_schema(table="Sample preparations", data=emx2_sample_preparation)
# # save and upload the sequencing runs table
emx2.save_schema(table="Sequencing runs", data=emx2_sequencing_runs)

##################################################
# Migrate table Files (solverd_files) to the new model

# initialize the emx2 files list
emx2_files = []
# loop through the files
for file in files:
    new_files_entry = {}

    # map 'EGA' (solverd_files) to 'alternative ids' (Files)
    if 'EGA' in file:
        new_files_entry['alternate ids'] = file['EGA']

    # map 'name' (solverd_files) to 'name' (Files)
    if 'name' in file:
        new_files_entry['name'] = file['name']
        
    # map 'name' and 'fenderFilePath' (solverd_files) to 'path' (Files)    
    if 'fenderFilePath' in file: # if fenderFilePath is in the file, combine with name
        new_files_entry['path'] = ",".join(map(str, [file['name'], file['fenderFilePath']]))
    else: # else, only use name
        new_files_entry['path'] = file['name']

    # map 'fileFormat' (solverd_files) to 'format' (Files)
    format_dict = { # dictionary for the categories with different capitalization in old vs. new version
        'CRAI': 'crai',
        'FastQ': 'FASTQ',
        'phenopacket': 'phenopacketJSON'
    }
    # if format is in dictionary, it needs the 'new' name (from the dictionary)
    if file['fileFormat']['label'] in format_dict:
        new_files_entry['format'] = format_dict[file['fileFormat']['label']]
    else: # else, it can directly be mapped
        new_files_entry['format'] = file['fileFormat']['label']

    # map 'md5' (solverd_files) to 'md5 checksum' (Files)
    if 'md5' in file:
        new_files_entry['md5 checksum'] = file['md5']

    # map 'subjectID' (solverd_files) to 'Individuals' (Files)
    if 'subjectID' in file:
        subjectIDs = []
        for subject in file['subjectID']:
            subjectIDs.append(subject['subjectID'])
        new_files_entry['individuals'] = ",".join(map(str, subjectIDs))

    # map 'sampleID' (solverd_files) to 'Biosamples' (Files)
    if 'sampleID' in sample:
        sampleIDs = []
        for sample in file['sampleID']:
            sampleIDs.append(sample['sampleID'])
        new_files_entry['biosamples'] = ",".join(map(str, sampleIDs))

    # map 'experimentID' (solverd_files) to 'generated by protocol' (Files) 
    if 'experimentID' in file:
        new_files_entry['generated by protocol'] = f'sample_{file['experimentID']['experimentID']},seq_{file['experimentID']['experimentID']}'

    # map 'partOfRelease' (solverd_files) to 'included in datasets' (Files)
    if 'partOfRelease' in file:
        partOfReleaseList = []
        resourcesList = []
        for release in file['partOfRelease']:
            partOfReleaseList.append(release['id'])
            resourcesList.append('RD3')
        # append to Sequencing runs
        new_files_entry['included in datasets.resource'] = ','.join(resourcesList)
        new_files_entry['included in datasets.name'] = ','.join(map(str, partOfReleaseList))
        
    # append the new entry to the files list
    if new_files_entry:
        emx2_files.append(new_files_entry)

# TO DO: this does not yet work. I upload the zip file manually in the UI upload. 

# convert to zip - otherwise too big for upload 
pd.DataFrame(emx2_files).to_csv('Files.csv.zip', index=False, compression='gzip')

# save and upload the files table
emx2.save_schema(table='Files', file='Files.csv.zip')

