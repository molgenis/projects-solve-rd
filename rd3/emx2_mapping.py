"""
Mapping script for RD3 EMX1 to EMX2
  
"""

from molgenis_emx2_pyclient import Client
from os import environ
from dotenv import load_dotenv
import molgenis.client
# from rd3tools.utils import flatten_data
import pandas as pd
load_dotenv()

# connect to the environment and log in
rd3 = molgenis.client.Session(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# connect to emx2
emx2 = Client(
    'http://localhost:8080/',
    schema='rd3',
    token=environ['MOLGENIS_EMX2_TOKEN']
)

schema = 'rd3'
emx2.default_schema = schema  # set default schema

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
# subjects = rd3.get('solverd_subjects', q=QUERY, uploadable=True)
subjects = rd3.get('solverd_subjects', q=QUERY)

# print(fids[:5])

# get the subject IDs - necessary to retrieve the subject info
IDs = [subject['subjectID'] for subject in subjects]
# print(IDs)
# get the subject info for the 5 families
QUERY = ','.join([f"subjectID=q={ID}" for ID in IDs])
subjects_info = rd3.get('solverd_subjectinfo', q=QUERY)
# get the sample info for the 5 families
QUERY = ','.join([f"belongsToSubject=={ID}" for ID in IDs])
samples = rd3.get('solverd_samples', q=QUERY)

# map the data to the new model
emx2_individuals = []
emx2_pedigree = []
emx2_resources = []
emx2_pedigree_members = []
# emx2_organisations = []
emx2_clinical_observations = []
emx2_individual_observations = []
emx2_individual_consent = []
# loop through the subjects in RD3
for subject in subjects:
    new_individual_entry = {}
    new_pedigree_entry = {}
    new_pedigree_members_entry = {}

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
            print(f"Value {subject_sex1} cannot be mapped")

    # map 'dateOfBirth' (solverd_subjects) to 'year of birth' (Individuals)
    match = [info['dateOfBirth']
             for info in subjects_info if 'dateOfBirth' in info and info['subjectID'] == subject['subjectID']]
    new_individual_entry['year of birth'] = "".join(map(str, match))

    # map 'fid' (solverd_subjects) to 'pedigree' (Individuals) and 'identifier' (Pedigree)
    if 'fid' in subject:
        new_individual_entry['pedigree'] = subject['fid']
        # to make this work the fid also needs to be added to the pedigree table in emx2.
        # check for uniqueness
        if not any(entry['identifier'] == subject['fid'] for entry in emx2_pedigree):
            new_pedigree_entry['identifier'] = subject['fid']
            emx2_pedigree.append(new_pedigree_entry)

    # map 'mid' and 'fid'(solverd_subjects) to Pedigree_members
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

    # create clinical and individual observations: only map the individual identifier.
    # Clinical observations
    new_clinical_observation_entry = {}
    new_clinical_observation_entry['individual'] = subject['subjectID']
    if 'solved' in subject:
        new_clinical_observation_entry['is solved'] = subject['solved']
    if 'date_solved' in subject:
        new_clinical_observation_entry['date solved'] = subject['date_solved']
    emx2_clinical_observations.append(new_clinical_observation_entry)
    # Individual observations
    new_individual_observation_entry = {}
    new_individual_observation_entry['individual'] = subject['subjectID']
    emx2_individual_observations.append(new_individual_observation_entry)

    # map 'partOfRelease' to 'included in datasets'
    partOfReleaseList = []  # list to gather the releases
    resourcesList = []  # list to gather resources
    # loop through the releases of this subject
    for release in subject['partOfRelease']:
        partOfReleaseList.append(release['id'])
        resourcesList.append('RD3')
    # check if the individual needs to be retracted from the tables.
    if 'retracted' in subject and subject['retracted']['id'] == 'Y':
        # add the lists to the new individual entry
        resourcesList.append("RD3")
        partOfReleaseList.append('Retracted')
    new_individual_entry['included in datasets.resource'] = ','.join(
        resourcesList)
    new_individual_entry['included in datasets.name'] = ','.join(
        map(str, partOfReleaseList))

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
    # TO DO: See notes (word doc). Made my own category in (local) ontology.
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

# save and upload the newly made tables
emx2.save_schema(table="Pedigree", data=emx2_pedigree)
emx2.save_schema(table="Individuals", data=emx2_individuals)
emx2.save_schema(table="Pedigree members", data=emx2_pedigree_members)
emx2.save_schema(table="Individual observations",
                 data=emx2_individual_observations)
emx2.save_schema(table="Clinical observations",
                 data=emx2_clinical_observations)
# obtain the clinical observations table (with automatically generated ID)
clinicalObs = emx2.get(schema=schema, table="Clinical observations")


# again loop through subjects to map phenotype and diseases
emx2_disease_history = []
emx2_phenotype_observations = []
for subject in subjects:
    # mapping 'disease' (solverd_subjects) to Disease history
    if 'disease' in subject:
        for disease in subject['disease']:
            new_disease_history_entry = {}
            new_disease_history_entry['disease'] = disease['label']
            for obs in clinicalObs:
                if obs['individual'] == subject['subjectID']:
                    new_disease_history_entry['part of clinical observation'] = obs['id']
            emx2_disease_history.append(new_disease_history_entry)
    # mapping 'phenotype' (solverd_subjects) to Phenotype observations (excluded = False)
    if 'phenotype' in subject:
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
        for notPhenotype in subject['hasNotPhenotype']:
            new_phenotype_observation_entry = {}
            new_phenotype_observation_entry['type'] = notPhenotype['label']
            for obs in clinicalObs:
                if obs['individual'] == subject['subjectID']:
                    new_phenotype_observation_entry['part of clinical observation'] = obs['id']
                    new_phenotype_observation_entry['excluded'] = True
            emx2_phenotype_observations.append(new_phenotype_observation_entry)

# save and upload the disease history and phenotype observation tables
emx2.save_schema(table="Disease history", data=emx2_disease_history)
emx2.save_schema(table="Phenotype observations",
                 data=emx2_phenotype_observations)

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

# save and upload the biosamples table
emx2.save_schema(table="Biosamples", data=emx2_biosamples)

# write to csv
pd.DataFrame(emx2_individuals).to_csv('Individuals.csv', index=False)
pd.DataFrame(emx2_pedigree).to_csv('Pedigree.csv', index=False)
pd.DataFrame(emx2_pedigree_members).to_csv('Pedigree members.csv', index=False)
pd.DataFrame(emx2_individual_consent).to_csv(
    'Individual consent.csv', index=False)
# pd.DataFrame(emx2_organisations).to_csv('Organisations.csv', index=False)
pd.DataFrame(emx2_disease_history).to_csv('Disease history.csv', index=False)
# pd.DataFrame(emx2_phenotype_observations).to_csv('Phenotype observations.csv', index=False)
pd.DataFrame(emx2_individual_observations).to_csv(
    'Individual observations.csv', index=False)
pd.DataFrame(emx2_clinical_observations).to_csv(
    'Clinical observations.csv', index=False)
pd.DataFrame(emx2_biosamples).to_csv('Biosamples.csv', index=False)
# pd.DataFrame(emx2_datasets).to_csv('Datasets.csv', index=False)
# pd.DataFrame(emx2_resources).to_csv('Resources.csv', index=False)

# notes
subjects_df = pd.DataFrame(subjects)
subject = subjects_df.loc[subjects_df['subjectID'] == "P0025760"]
subject = subjects_df.loc[subjects_df['subjectID'] == "P0000038"]

if 'organisation' in subject and all([item for item in subject['organisation']]):
    print('This should be printed when there are organisations present')

print(subject['retracted'])
