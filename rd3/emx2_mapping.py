"""
Mapping script for RD3 Solve-RD EMX1 to EMX2

1. Create a new schema with the PATIENT_REGISTRY template
2. Prepare your .env file with the following variables:
    MOLGENIS_PROD_HOST: url with the solve-RD RD3 data
    MOLGENIS_PROD_USR: username to solve-RD RD3 database
    MOLGENIS_PROD_PWD: password to solve-RD RD3 database
    EMX2_URL: url with the newly generated emx2 RD3 database (to which the data will be uploaded)
    EMX2_SCHEMA: the schema name of the RD3 EMX2 instance
    EMX2_TOKEN: token to login to RD3 EMX2
    OUTPUT_PATH_TO_CSVS: string with the path to where the csvs will be placed
    INPUT_PATH_TO_SOLVERD_DATA: input path where the samples, labinfos and files from solve-RD are downloaded - 
        alternatively, these can be retrieved via the EMX1 API, however, this is quite slow. 
3. Run this script 

The data will be written to csv files in the user-specified output folder and uploaded to the server.
"""

from molgenis_emx2_pyclient import Client
from os import environ
from dotenv import load_dotenv
import molgenis.client
import pandas as pd
load_dotenv()
import re
import json
import numpy as np
import zipfile
from zipfile import ZipFile
import asyncio
import os
import logging

# set logging
logging.basicConfig(level='INFO')
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# connect to the RD3 EMX1 environment and log in
rd3 = molgenis.client.Session(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# connect to RD3 EMX2 environment
schema = environ['EMX2_SCHEMA']
emx2 = Client(
    environ['EMX2_URL'],
    schema=schema,
    token=environ['EMX2_TOKEN']
)
emx2.default_schema = schema  # set default schema

output_path = environ['OUTPUT_PATH_TO_CSVS']
input_path = environ['INPUT_PATH_TO_SOLVERD_DATA']

##################################################
# read the datasets from solve-RD RD3

# retrieve subjects and subject information from server
subjects = rd3.get('solverd_subjects', batch_size=5000)
subjects_info = rd3.get('solverd_subjectinfo', batch_size=5000)

# get samples from solve-RD
with open(f'{input_path}samples_17022025.json', 'r') as file:
    samples = json.load(file)

# get experiments from solve-RD
with open(f'{input_path}experiments_17022025.json', 'r') as file:
    labinfos = json.load(file)

# get experiments from solve-RD
with open(f'{input_path}files_11022025.json', 'r') as file:
    files = json.load(file)

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
    subject_id = subject['subjectID']
    
    new_individual_entry = {}

    # map 'subjectID' (solverd_subjects) to 'id' (Individuals)
    new_individual_entry['id'] = subject_id

    # map 'sex1' (solverd_subjects) to 'gender at birth' (Individuals)
    # dictionary of possible gender values 
    gender_dict = {
        'M': 'assigned male at birth',
        'F': 'assigned female at birth',
        'U': 'U',
        'UD': 'UD'
    }

    # get sex info
    sex_info = subject.get('sex1', {})
    sex_id = sex_info.get('id')

    if sex_id in gender_dict:
        new_individual_entry['gender at birth'] = gender_dict[sex_id]
    elif sex_id is not None:
        print(f"Gender at birth value {sex_id} cannot be mapped for individual {subject_id}")

    # map 'dateOfBirth' (solverd_subjects) to 'year of birth' (Individuals)
    match = [info['dateOfBirth']
             for info in subjects_info if 'dateOfBirth' in info and info['subjectID'] == subject_id]
    new_individual_entry['year of birth'] = "".join(map(str, match))

    # map 'fid' (solverd_subjects) to 'pedigree' (Individuals) and 'id' (Pedigree)
    fid = subject.get('fid')
    if fid:
        new_pedigree_entry = {} # initialize new entry
        new_individual_entry['pedigree'] = fid
        # to make this work the fid also needs to be added to the pedigree table in emx2.
        # check for uniqueness

        if not any(entry['id'] == fid for entry in emx2_pedigree):
            new_pedigree_entry['id'] = fid
            emx2_pedigree.append(new_pedigree_entry)

    ###
    # Mapping pedigree. 
    # If an individual has either a maternal or paternal ID, this person is a patient.
    # If an individual is a parent and has a parent, a grandparent relationship is determined.
    # If an individual is a patient and has a child, the individual is added multiple times to the 
    #   pedigree members table: both as a patient (with itself as the relative) and as a parent
    #   (with the child as the relative).
    # If an individual has a family ID, but no other information, the person is added to the table
    #   with itself as the relative. This happens later in the code (lines 355 - 394).
    # If individuals have the same mother and father, they are mapped as full siblings. This also
    #   happens later in the code (lines 355 - 394).
    # If a family only has one member, we assume this individual is a patient. This is done at the end.
    ###

    if fid:
        # Map Patient - if individual has a parent (maternal or paternal), the person is a patient.
        if 'pid' in subject or 'mid' in subject:
            new_pedigree_members_entry = {}
            new_pedigree_members_entry['pedigree'] = fid
            new_pedigree_members_entry['individual'] = subject_id
            if 'clinical_status' in subject:
                new_pedigree_members_entry['affected'] = subject['clinical_status']
            new_pedigree_members_entry['relative'] = subject_id
            new_pedigree_members_entry['relation'] = 'Patient'
            emx2_pedigree_members.append(new_pedigree_members_entry)
            # keep track of the patients
            patients.append(subject['subjectID'])
        # Map Parents - based on MaternalID and PaternalID
        for subj in subjects:
            if subj['subjectID'] == subject_id:
                continue
            new_pedigree_members_entry = {}
            new_pedigree_members_entry['pedigree'] = fid
            new_pedigree_members_entry['individual'] = subject_id
            if 'clinical_status' in subject:
                new_pedigree_members_entry['affected'] = subject['clinical_status']
            # Map Biological Mother
            if 'mid' in subj and subj['mid']['subjectID'] == subject_id:
                new_pedigree_members_entry['relative'] = subj['subjectID']
                new_pedigree_members_entry['relation'] = "Biological Mother"
                # add the child and mother IDs to the dictionary
                track_mothers[subj['subjectID']] = subject_id
            # Map Biological Father
            elif 'pid' in subj and subj['pid']['subjectID'] == subject_id:
                new_pedigree_members_entry['relative'] = subj['subjectID']
                new_pedigree_members_entry['relation'] = "Biological Father"
                # add the child and father's IDs to the dictionary
                track_fathers[subj['subjectID']] = subject_id
            if 'relative' in new_pedigree_members_entry: # only append new entry if individual has a parent
                emx2_pedigree_members.append(new_pedigree_members_entry)
        # Map maternal grandparents
        if subject_id in patients and subject_id in track_mothers.values():
            # Map Biological Maternal Grandmother
            if 'mid' in subject:
                # get the identifier for the grandchild
                grandchild_ids = [child_id for child_id, value in track_mothers.items() if value == subject_id]
                for grandchild_id in grandchild_ids:
                    new_pedigree_members_entry = {}
                    new_pedigree_members_entry['pedigree'] = fid
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
                grandchild_ids = [child_id for child_id, value in track_mothers.items() if value == subject_id]
                for grandchild_id in grandchild_ids:
                    new_pedigree_members_entry = {}
                    new_pedigree_members_entry['pedigree'] = fid
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
        if subject_id in patients and subject_id in track_fathers.values():
            # Map Biological Paternal Grandmother
            if 'mid' in subject:
                # get the identifier for the grandchild
                grandchild_ids = [child_id for child_id, value in track_fathers.items() if value == subject_id]
                for grandchild_id in grandchild_ids:
                    new_pedigree_members_entry = {}
                    new_pedigree_members_entry['pedigree'] = fid
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
                grandchild_ids = [child_id for child_id, value in track_fathers.items() if value == subject_id]
                for grandchild_id in grandchild_ids:
                    new_pedigree_members_entry = {}
                    new_pedigree_members_entry['pedigree'] = fid
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

    # map 'organisation' and 'ERN' (solverd_subjects) to 'affiliated organisations' (Individuals)
    organisations = []  # new list to save the organisations
    new_organisation_entry = {}
    # check if organisations are present for this subject
    if 'organisation' in subject and subject['organisation']:
        # get the value for every organisation in the list
        orgs = [org['value'] for org in subject['organisation']]
        organisations.append(orgs)
    # check if there are any ERNs present for this subject
    if 'ERN' in subject and subject['ERN']:
        # get the shortname for every ERN in the list
        ern = [org['shortname'] for org in subject['ERN']]
        organisations.append(ern)
    # add the organisation and ERNs (IDs)
    if all(organisations):  # make sure it is not empty
        new_individual_entry['affiliated organisations'] = ', '.join(
            item[0] for item in organisations)

    # create clinical observations
    # Clinical observations: map individual id and solved info
    new_clinical_observation_entry = {} # initialize new entry
    new_clinical_observation_entry['individuals'] = subject_id
    new_clinical_observation_entry['is solved'] = subject.get('solved')
    new_clinical_observation_entry['date solved'] = subject.get('date_solved')
    emx2_clinical_observations.append(new_clinical_observation_entry)
   
    # map 'partOfRelease' to 'included in resources' (Individuals)
    namesList = []  # list to gather the names
    # map 'partOfRelease
    if 'partOfRelease' in subject:
        # loop through the releases of this subject
        for release in subject['partOfRelease']:
            namesList.append(release['id'])
    # map 'retracted' (solverd_subjects)
    if 'retracted' in subject and subject['retracted']['id'] == 'Y':
        namesList.append('Retracted')
    # map 'includedInDatasets' (solverd_subjects)
    if 'includedInDatasets' in subject:
        for dataset in subject['includedInDatasets']:
            namesList.append(dataset['id'])
    # add the lists to the new entry
    new_individual_entry['included in resources'] = ','.join(
        map(str, namesList))

    # map 'consent' (solverd_subjects) to Individual consent 
    if 'matchMakerPermission' in subject:
        new_individual_consent_entry = {} 
        new_individual_consent_entry['id'] = subject_id + '-matchmaker'
        new_individual_consent_entry['individuals'] = subject_id
        if subject['matchMakerPermission']:
            new_individual_consent_entry['allow recontacting'] = "Allow use in MatchMaker"
        else:
            new_individual_consent_entry['allow recontacting'] = "No use in MatchMaker"
        emx2_individual_consent.append(new_individual_consent_entry)
    if 'noIncidentalFindings' in subject:
        new_individual_consent_entry = {}
        new_individual_consent_entry['id'] = subject_id + \
            '-reportIncidental'
        new_individual_consent_entry['individuals'] = subject_id
        if subject['noIncidentalFindings']:
            new_individual_consent_entry['allow recontacting'] = "Report incidental findings back"
        else:
            new_individual_consent_entry['allow recontacting'] = "No reporting of incidental findings"
        emx2_individual_consent.append(new_individual_consent_entry)
    if 'recontact' in subject:
        new_individual_consent_entry = {}
        new_individual_consent_entry['id'] = subject_id + \
            '-recontactIncidental'
        new_individual_consent_entry['individuals'] = subject_id
        if subject['recontact']['label'] == 'Yes':
            new_individual_consent_entry['allow recontacting'] = "Recontacting for incidental findings"
        if subject['recontact']['label'] == 'No':
            new_individual_consent_entry['allow recontacting'] = "No recontacting for incidential findings"
        emx2_individual_consent.append(new_individual_consent_entry)

    # map 'comments' (solverd_subjects) to 'comments' (Individuals)
    new_individual_entry['comments'] = subject.get('comments')

    # map 'sex2' (solverd_samples) to 'genotypic sex' (Individuals)
    sex_map = {
        'M': 'XY Genotype',
        'F': 'XX Genotype'
    }

    for sample in samples:
        subject_from_sample = sample.get('belongsToSubject', {})
        sex2 = sample.get('sex2', {})
        if subject_from_sample.get('subjectID') != subject_id:
            continue
        
        sex_id = sex2.get('id')
        if sex_id in sex_map:
            new_individual_entry['genotypic sex'] = sex_map[sex_id]
        elif sex_id is None:
            continue
        else:
            print(f'The genotypic sex {sex_id} could not be mapped.')

    # append the new entry to the individuals list
    emx2_individuals.append(new_individual_entry)

# Map the individuals that do have a family id but no other information about the relationships
# gather all individuals in the current pedigree members table
# Additionally map sibling relationships. 
individuals_in_pedigree = [member['individual'] for member in emx2_pedigree_members]
# loop through the subjects
for subject in subjects:
    subject_id = subject['subjectID']
    # if the subject has a family ID and is not yet an individual in the list, it needs to be added 
    if 'fid' in subject and subject_id not in individuals_in_pedigree:
        new_pedigree_members_entry = {}
        new_pedigree_members_entry['pedigree'] = subject['fid']
        new_pedigree_members_entry['individual'] = subject_id
        new_pedigree_members_entry['affected'] = subject.get('clinical_status')
        new_pedigree_members_entry['relative'] = subject_id # itself
        emx2_pedigree_members.append(new_pedigree_members_entry)
    # Map Full Sibling relationships
    # first, check if an individual has both parents' IDs and check if the sex is a known category.
    if 'mid' and 'pid' in subject and 'sex1' in subject and subject['sex1']['id'] in ['F', 'M']: 
        # get the parents' IDs
        mother = track_mothers.get(subject_id)
        father = track_fathers.get(subject_id)

        # collect the siblings for the individual
        siblings = []
        for child in track_mothers:
            if track_mothers.get(child) == mother and track_fathers.get(child) == father and child != subject_id:
                siblings.append(child)

        # loop through the siblings and add to the list of pedigree members
        for sibling in siblings:
            new_pedigree_members_entry = {}
            new_pedigree_members_entry['pedigree'] = subject['fid']
            new_pedigree_members_entry['individual'] = subject_id
            new_pedigree_members_entry['affected'] = subject.get('clinical_status')
            new_pedigree_members_entry['relative'] = sibling
            if subject['sex1']['id'] == 'F':
                new_pedigree_members_entry['relation'] = 'Full Sister'
            if subject['sex1']['id'] == 'M':
                new_pedigree_members_entry['relation'] = 'Full Brother'
            emx2_pedigree_members.append(new_pedigree_members_entry)

# this step is to explode the cases in the pedigree data where an identifier 
# consists of multiple families, each should have its own row 
emx2_pedigree_df = pd.DataFrame(emx2_pedigree)
emx2_pedigree_df.loc[:,'id'] = emx2_pedigree_df['id'].str.split(',')
emx2_pedigree_df = emx2_pedigree_df.explode('id')
# same for pedigree members
emx2_pedigree_members_df = pd.DataFrame(emx2_pedigree_members)
emx2_pedigree_members_df.loc[:,'pedigree'] = emx2_pedigree_members_df['pedigree'].str.split(',')
emx2_pedigree_members_df = emx2_pedigree_members_df.explode('pedigree')

# get all family IDs
families = emx2_pedigree_members_df.pedigree.to_list()
# get the family IDs with only one member 
uniques = [family for family in families if families.count(family) == 1]

# set the individual's relation to Patient in these families
for _, member in emx2_pedigree_members_df.drop_duplicates().iterrows():
    if member['pedigree'] in uniques and member['relation'] is np.nan:
        member['relation'] = 'Patient'

# save and upload
emx2_pedigree_df.drop_duplicates().to_csv(f'{output_path}Pedigree.csv', index=False)
emx2_pedigree_members_df.drop_duplicates().to_csv(f'{output_path}Pedigree members.csv', index=False)
pd.DataFrame(emx2_individuals).to_csv(f'{output_path}Individuals.csv', index=False)
pd.DataFrame(emx2_individual_consent).to_csv(f'{output_path}Individual consent.csv', index=False)

emx2.save_schema(table="Pedigree", data=emx2_pedigree_df.drop_duplicates())
emx2.save_schema(table="Individuals", data=emx2_individuals)
emx2.save_schema(table="Pedigree members", data=emx2_pedigree_members_df.drop_duplicates())
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
    subject_id = subject['subjectID']
    # mapping 'disease' (solverd_subjects) to Disease history
    if 'disease' in subject and subject['disease']:
        for disease in subject['disease']:
            new_disease_history_entry = {}
            new_disease_history_entry['disease'] = disease['label']
            for obs in clinicalObs:
                if obs['individuals'] == subject_id:
                    new_disease_history_entry['part of clinical observation'] = obs['id']
            emx2_disease_history.append(new_disease_history_entry)
    # mapping 'phenotype' (solverd_subjects) to Phenotype observations (excluded = False)
    if 'phenotype' in subject and subject['phenotype']:
        for phenotype in subject['phenotype']:
            new_phenotype_observation_entry = {}
            new_phenotype_observation_entry['type'] = phenotype['label']
            for obs in clinicalObs:
                if obs['individuals'] == subject_id:
                    new_phenotype_observation_entry['part of clinical observation'] = obs['id']
                    new_phenotype_observation_entry['excluded'] = False
            emx2_phenotype_observations.append(new_phenotype_observation_entry)
    # mapping the 'hasNotPhenotype' (solverd_subjects) to Phenotype observations (excluded = True)
    if 'hasNotPhenotype' in subject:
        for notPhenotype in subject['hasNotPhenotype'] and subject['hasNotPhenotype']:
            new_phenotype_observation_entry = {}
            new_phenotype_observation_entry['type'] = notPhenotype['label']
            for obs in clinicalObs:
                if obs['individuals'] == subject_id:
                    new_phenotype_observation_entry['part of clinical observation'] = obs['id']
                    new_phenotype_observation_entry['excluded'] = True
            emx2_phenotype_observations.append(new_phenotype_observation_entry)
    # map 'ageOfOnset' (solverd_subject_info) to ageOfOnset (Disease history)
    # find the information of the subject with an ageOfOnset. 
    match = [info for info in subjects_info if 'ageOfOnset' in info and info['subjectID'] == subject_id]
    if match: 
        # make a dict of the IDs and the auto IDs (part of clinical observation)
        tmp = {obs['individuals']:obs['id'] for obs in clinicalObs}
        # get the auto ID based on the ID from the individual with an age of onset 
        target_part_of_clinical_obs = tmp.get(match[0]['subjectID'])
        # find this individual in the disease history list based on auto ID. 
        for disease_history_entry in emx2_disease_history:
            if disease_history_entry['part of clinical observation'] == target_part_of_clinical_obs:
                # update this entry in the dict with the age at onset
                disease_history_entry.update({'age group at onset':match[0]['ageOfOnset']['label']})

# save and upload 
pd.DataFrame(emx2_disease_history).to_csv(f'{output_path}Disease history.csv', index=False)
pd.DataFrame(emx2_phenotype_observations).to_csv(f'{output_path}Phenotype obersvations.csv', index=False)

emx2.save_schema(table="Disease history", data=emx2_disease_history)
emx2.save_schema(table="Phenotype observations",
                 data=emx2_phenotype_observations)

#######################################################################
#  Migrate (bio)samples information to the new model, the data is mapped to 
# NGS Sequencing table, which inherits from the Experiments table.

# initialize list to gather the NGS Sequencing info for the new model
ngs_experiments = {}
for sample in samples:
    # get sample ID
    sample_id = sample['sampleID']
    # map 'sampleID' (solverd_samples) to 'sample id' (Experiments)
    ngs_experiments[sample_id] = {'sample id': sample_id}

    # map 'pathologicalState' (solverd_samples) to 'pathological state' (Experiments)
    if 'pathologicalState' in sample:
        ngs_experiments[sample_id]['pathological state'] = sample.get('pathologicalState').get('value')

    # map 'anatomicalLocation' (solverd_samples) to 'anatomical location' (Experiments)
    if 'anatomicalLocation' in sample:
        ngs_experiments[sample_id]['anatomical location'] = sample.get('anatomicalLocation').get('id')
        if sample.get('anatomicalLocation').get('label') == 'Other':
            ngs_experiments[sample_id]['anatomical location other'] = sample.get('anatomicalLocationComment')

    # map 'belongsToSubject' (solverd_samples) to 'individuals' (Experiments)
    if 'belongsToSubject' in sample:
        ngs_experiments[sample_id]['individuals'] = sample.get('belongsToSubject').get('subjectID')

    # map 'tissueType' (solverd_samples) to 'tissue type' (Experiments):
    if 'tissueType' in sample:
        ngs_experiments[sample_id]['tissue type'] = sample['tissueType']['id']

    # map 'materialType' (solverd_samples) to 'sample type' (Experiments):
    if 'materialType' in sample:
        types = []
        for type in sample['materialType']:
            types.append(type['label'])
        ngs_experiments[sample_id]['sample type'] = ",".join(map(str, types))

    # map 'organisation' (solverd_samples) to 'collected at organisation' (Experiments)
    if 'organisation' in sample:
        ngs_experiments[sample_id]['collected at organisation'] = sample['organisation']['value']

    # map 'ERN' (solverd_samples) to 'affiliated organisations' (Experiments)
    if 'ERN' in sample:
        ngs_experiments[sample_id]['affiliated organisations'] = sample['ERN']['shortname']

    # map to 'included in resources' (Experiments)
    namesList = [] # gathers the names
    # map 'retracted' (solverd_samples) to 'included in resources'
    if 'retracted' in sample and sample['retracted']['id'] == 'Y': 
        namesList.append('Retracted')

    # map 'batch' (solverd_samples) to 'included in resources'
    if 'batch' in sample:
        for batch in sample['batch'].split(","): # split on comma in the case of mulitple batches
            namesList.append(batch)

    # map 'partOfRelease' (solverd_samples) to 'included in resources'
    # gather the resources and releases (names)
    if 'partOfRelease' in sample:
        for release in sample['partOfRelease']:
            namesList.append(release['id'])

    # map 'includedInDatasets' (solverd_samples) to 'included in resources'
    if 'includedInDatasets' in sample:
        for dataset in sample['includedInDatasets']:
            namesList.append(dataset['id'])
            
    # add the lists to a new entry 
    ngs_experiments[sample_id]['included in resources'] = ",".join(map(str, namesList))

    # map 'alternativeIdentifier' (solverd_samples) to 'local sample id' (Experiments)
    ngs_experiments[sample_id]['local sample id'] = sample.get('alternativeIdentifier')

    # map 'flag' (solverd_samples) to 'failed quality control' (Experiments)
    ngs_experiments[sample_id]['failed quality control'] = sample.get('flag')

    # map 'percentageTumorCells' (solverd_samples) to 'percentage tumor cells' (Experiments)
    ngs_experiments[sample_id]['percentage tumor cells'] = sample.get('percentageTumorCells')

    # map 'comments' (solverd_samples) to 'comments' (Experiments)
    if 'comments' in sample:
        ngs_experiments[sample_id]['comments'] = f'sample_{sample.get('comments')}'

##################################################
# Migrate the labinfo (solverd_labinfo) to NGS Sequencing table as well

# initialize lists for the data for the new model
emx2_experiments = []
# loop through the experiment info
for labinfo in labinfos:
    # get sample id
    sample_id = None
    if 'sampleID' in labinfo and (len(labinfo['sampleID']) != 0): 
        sample_id = labinfo.get('sampleID')[0].get('sampleID')
    if sample_id is None:
        sample_id = f'noID_{len(ngs_experiments)+1}'
    if sample_id not in ngs_experiments:
        ngs_experiments[sample_id] = {}

    new_experiment_entry = {}

    # map 'experimentID' (solverd_labinfo) to 'id' (Experiments) 
    new_experiment_entry['id'] = labinfo['experimentID']

    # map sample id 
    new_experiment_entry['sample id'] = sample_id

    # map 'capture' (solverd_labinfo) to 'target enrichment kit' (Experiments) 
    new_experiment_entry['target enrichment kit'] = labinfo.get('capture')

    # map 'libraryType' (solverd_labinfo) to 'library source' (Experiments)
    # data only contains types Genomic and Transcriptomic
    if 'libraryType' in labinfo:
        # get the library type and convert to lowercase
        libType = labinfo['libraryType']['id'].lower()
        # add the word source as is defined in the ontology
        libType += " source"
        # map
        new_experiment_entry['library source'] = libType
    
    # map 'library' (solverd_labinfo) to 'library layout' (Experiments)
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
        new_experiment_entry['library layout'] = ",".join(map(str, layout))

    # map 'sequencingCentre' (solverd_labinfo) to 'sequencing centre' (Experiments)
    if 'sequencingCentre' in labinfo:
        new_experiment_entry['sequencing centre'] = labinfo['sequencingCentre']['value']

    # map 'sequencer' (solverd_labinfo) to 'platform' (Experiments)
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
        new_experiment_entry['platform'] = platform_dict[platform]

        # also map to 'platform model' (Experiments)
        pattern = re.compile(r'^DNBSEQ-\w{1}\d+$')
        if not pattern.match(labinfo['sequencer']): # don't map all DNBSEQ sequencers
            # differently named in ontology 
            if labinfo['sequencer'] == 'Sequel II': 
                new_experiment_entry['platform model'] = 'PacBio Sequel II'
            else: # the platform model is as is in the ontology and can thus be mapped
                new_experiment_entry['platform model'] = labinfo['sequencer'].rstrip() # strip ending whitespace

    # map 'seqType' (solverd_labinfo) to 'library strategy' (Experiments)
    if 'seqType' in labinfo and labinfo['seqType']:
        for seqType in labinfo['seqType']:
            if seqType['label'] == 'ssRNA-seq': # should not be capatalized (ontology)
                new_experiment_entry['library strategy'] = seqType['label']
            else:
                # capitalize first letter in the strategy (as per ontology)
                new_experiment_entry['library strategy'] = seqType['label'].title()

    # map 'mean_cov' (solverd_labinfo) to 'mean read depth' (Experiments) 
    new_experiment_entry['mean read depth'] = labinfo.get('mean_cov')

    # map 'median_cov' (solverd_labinfo) to median read depth' (Experiments)
    new_experiment_entry['median read depth'] = labinfo.get('median_cov')

    # map 'c20' (solverd_labinfo) to 'percentage Tr20' (Experiments)
    new_experiment_entry['percentage Tr20'] = labinfo.get('c20')

    # map partOfRelease to 'included in resources' (Experiments)
    # initialize lists to gather the datasets
    namesList = []
    # map 'partOfRelease'
    if 'partOfRelease' in labinfo:
        for release in labinfo['partOfRelease']:
            namesList.append(release['id'])

    # map 'retracted' (solverd_labinfo) 
    # check if the individual needs to be retracted from the tables.
    if 'retracted' in labinfo and labinfo['retracted']['id'] == 'Y':
        # add the lists to the new individual entry
        namesList.append('Retracted')

    # map 'includedInDatasets' (solverd_labinfo) 
    if 'includedInDatasets' in labinfo:
        for dataset in labinfo['includedInDatasets']:
            namesList.append(dataset['id'])

    # update the entry with the sample information (if present)
    if sample_id in ngs_experiments:
        new_experiment_entry.update(ngs_experiments[sample_id])

    # add the resources to the resources from the sample (if present)
    inc_in_resources = new_experiment_entry.get('included in resources')
    if inc_in_resources is not None: 
        sample_resources = [resource for resource in inc_in_resources.split(',')]
        namesList += sample_resources
    new_experiment_entry['included in resources'] = ','.join(
        map(str, set(namesList)))
    
    # map 'comments' (solverd_labinfo) to 'comments' (Experiments)
    comments_sample = new_experiment_entry.get('comments') # not None
    comments_experiment = labinfo.get('comments') 
    if comments_sample is not None and comments_experiment is not None:
        comments_experiment = f'experiment_{comments_experiment}'
        new_experiment_entry['comments'] = f'{comments_experiment},{comments_sample}'
    elif comments_experiment:
        new_experiment_entry['comments'] = f'experiments_{labinfo.get('comments')}'

    emx2_experiments.append(new_experiment_entry)

# add the samples that do not have experiment info
sample_ids_emx2 = [experiment['sample id'] for experiment in emx2_experiments]
for sample_id, sample_info in ngs_experiments.items():
    if sample_id not in sample_ids_emx2:
        emx2_experiments.append(sample_info)

# upload and save 
emx2_experiments_df = pd.DataFrame(emx2_experiments)
emx2_experiments_df.to_csv(f'{output_path}NGS sequencing.csv', index=False)
emx2.save_schema(table='NGS sequencing', data=emx2_experiments_df)

##################################################
# Migrate table Files (solverd_files) to the new model

# first, initialize a file storage location entry
emx2_file_storage_locations = {
    'name': 'gearshift',
    'type': 'Server',
    'organisation': 'UMCG'
    }
# upload
emx2.save_schema(table='File storage location', data=pd.DataFrame([emx2_file_storage_locations]))

# initialize the emx2 files list
emx2_files = []
emx2_file_locations = []
# loop through the files
for file in files:
    new_files_entry = {}
    new_file_location_entry = {}

    # map 'EGA' (solverd_files) to 'alternative ids' (Files)
    new_files_entry['alternate ids'] = file.get('EGA')

    # map 'name' (solverd_files) to 'id' (Files) and to 'file' (File locations)
    new_files_entry['id'] = file.get('name')
    new_file_location_entry['file'] = file.get('name')

    # set storage location
    new_file_location_entry['storage location'] = emx2_file_storage_locations.get('name')

    # set path in File locations
    if 'fenderFilePath' in file: # if fenderFilePath is in the file, combine with name
        new_file_location_entry['path'] = ",".join(map(str, [file['name'], file['fenderFilePath']]))
    else: # else, only use name
        new_file_location_entry['path'] = file['name']

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
    new_files_entry['md5 checksum'] = file.get('md5')

    # map 'subjectID' (solverd_files) to 'individuals' (Files)
    if 'subjectID' in file:
        subjectIDs = []
        for subject in file['subjectID']:
            subjectIDs.append(subject['subjectID'])
        new_files_entry['individuals'] = ",".join(map(str, subjectIDs))

    # map 'experimentID' (solverd_files) to 'produced by experiments' (Files) 
    if 'experimentID' in file:
        new_files_entry['produced by experiment'] = file['experimentID']['experimentID']

    # map 'partOfRelease' (solverd_files) to 'included in resources' (Files)
    if 'partOfRelease' in file:
        partOfReleaseList = []
        for release in file['partOfRelease']:
            partOfReleaseList.append(release['id'])
        # append to Sequencing runs
        new_files_entry['included in resources'] = ','.join(map(str, partOfReleaseList))
        
    # append the new entry to the files list
    if new_files_entry:
        emx2_files.append(new_files_entry)

# convert to zip - otherwise too big for upload 
async def upload_files():
    # write files to csv
    pd.DataFrame(emx2_files).to_csv(f'{output_path}Files.csv', index=False)

    zip_file_name=f'{output_path}files.zip'
    # zip the data
    with ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as my_zip:
        my_zip.write(f'{output_path}Files.csv', 'Files.csv')
    # upload the zipped file with the molgenis schema and the molgenis members
    await emx2.upload_file(schema=schema, file_path=zip_file_name)

    # remove unzipped file again
    os.remove(f'{output_path}Files.csv')

# run the upload of the zipped Files
asyncio.run(upload_files())