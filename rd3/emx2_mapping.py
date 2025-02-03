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
import re

# connect to the RD3 EMX1 environment and log in
rd3 = molgenis.client.Session(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# connect to EMX2
emx2 = Client(
    'http://localhost:8080/',
    schema='rd3',
    token=environ['MOLGENIS_EMX2_TOKEN']
)

schema = 'rd3'
emx2.default_schema = schema  # set default schema

# retrieve first n family identifiers
families = rd3.get('solverd_subjects', attributes='fid', batch_size=10000)
fids = []
for row in families:
    if 'fid' in row.keys():
        if row['fid'] not in fids:
            fids.append(row['fid'])

# get the subjects from the first 5 families
QUERY = ','.join([f"fid=q={fid}" for fid in fids[:5]])
# add family ID that has specific variables with data, so the code can be checked.
QUERY = QUERY + (
    f',fid=q={environ['FAM_ID_1']}' + 
    f',fid=q={environ['FAM_ID_2']}' + 
    f',fid=q={environ['FAM_ID_3']}' + 
    f',fid=q={environ['FAM_ID_4']}' + 
    f',fid=q={environ['FAM_ID_5']}' +
    f',fid=q={environ['FAM_ID_6']}'
)
# subjects = rd3.get('solverd_subjects', q=QUERY, uploadable=True)
subjects = rd3.get('solverd_subjects', q=QUERY)

# get the subject IDs - necessary to retrieve the subject info
IDs = [subject['subjectID'] for subject in subjects]
# get the subject info for the families
QUERY = ','.join([f"subjectID=q={ID}" for ID in IDs])
subjects_info = rd3.get('solverd_subjectinfo', q=QUERY)

# get the sample info for the families
QUERY = ','.join([f"belongsToSubject=={ID}" for ID in IDs])
samples = rd3.get('solverd_samples', q=QUERY)

# get the labinfo for the families
sample_ids = [sample['sampleID'] for sample in samples]
QUERY = ','.join([f'sampleID=={sample_id}' for sample_id in sample_ids])
labinfos = rd3.get('solverd_labinfo', q=QUERY)
labinfos_all = rd3.get('solverd_labinfo', batch_size=10000)

# get the files for the families - TO DO: does not work
QUERY = ','.join([f"subjectID=={ID}" for ID in IDs])
files = rd3.get('solverd_files', q=QUERY, batch_size=10000)

##################################################
# Migrate subjects (solverd_subjects) to the new model

# initialize lists for the tables in EMX2
emx2_individuals = []
emx2_pedigree = []
emx2_pedigree_members = []
emx2_clinical_observations = []
emx2_individual_observations = []
emx2_individual_consent = []
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
            print(f"Value {subject_sex1} cannot be mapped")

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

    # create clinical and individual observations
    # Clinical observations: map identifier and solved info
    new_clinical_observation_entry = {} # initialize new entry
    new_clinical_observation_entry['individual'] = subject['subjectID']
    if 'solved' in subject:
        new_clinical_observation_entry['is solved'] = subject['solved']
    if 'date_solved' in subject:
        new_clinical_observation_entry['date solved'] = subject['date_solved']
    emx2_clinical_observations.append(new_clinical_observation_entry)
    # Individual observations: only map the identifier
    new_individual_observation_entry = {} # initialize new entry
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

# retrieve phenotype observations, disease history and clinical observations
phenObs = emx2.get(table='Phenotype observations')
disHist = emx2.get('Disease history')
clinObs = emx2.get('Clinical observations')

# delete phenotype observations, disease history and clinical observations 
# because the upload does not replace but adds to the exisiting tables. 
emx2.delete_records(table='Phenotype observations', data=phenObs)
emx2.delete_records(table='Disease history', data=disHist)
emx2.delete_records(table='Clinical observations', data=clinObs)

# save and upload the newly made tables
emx2.save_schema(table="Pedigree", data=emx2_pedigree)
emx2.save_schema(table="Individuals", data=emx2_individuals)
emx2.save_schema(table="Pedigree members", data=emx2_pedigree_members)
emx2.save_schema(table="Individual observations",
                 data=emx2_individual_observations)
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
pd.DataFrame(emx2_disease_history).to_csv('Disease history.csv', index=False)

# save and upload the disease history and phenotype observation tables
emx2.save_schema(table="Disease history", data=emx2_disease_history)
emx2.save_schema(table="Phenotype observations",
                 data=emx2_phenotype_observations)

#######################################################################
#  Migrate (bio)samples information to the new model

# retrieve and delete Biosamples, this table relies on other tables, so these also need to be removed.
# retrieve the protocol activity, sequencing runs, and sample preparations tables, so they can be truncated 
# seqRuns = emx2.get('Sequencing runs')
# sampPreps = emx2.get('Sample preparations')
# protAct = emx2.get('Protocol activity')
# biosamp = emx2.get(table='Biosamples')

# # delete the tables because the upload does not replace, but adds to the exisiting tables.
# emx2.delete_records(table='Sequencing runs', data=seqRuns) 
# emx2.delete_records(table='Sample preparations', data=sampPreps)
# emx2.delete_records(table='Protocol activity', data=protAct)
# # now, Biosamples can be removed.
# emx2.delete_records(table='Biosamples', data=biosamp)

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
        new_biosamples_entry['anatomical location'] = sample['anatomicalLocation']['label']

    # map 'belongsToSubject' (solverd_samples) to 'collected from individual' (Biosamples)
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
            types.append(type['id'])
        new_biosamples_entry['material type'] = ",".join(map(str, types))

    # map 'organisation' (solverd_samples) to 'collected at organisation' (Biosamples)
    if 'organisation' in sample:
        new_biosamples_entry['collected at organisation.resource'] = 'RD3'
        new_biosamples_entry['collected at organisation.id'] = sample['organisation']['value']

    # map 'ERN' (solverd_samples) to 'affiliated organisations' (Biosamples)
    if 'ERN' in sample:
        new_biosamples_entry['affiliated organisations.resource'] = 'RD3'
        new_biosamples_entry['affiliated organisations.id'] = sample['ERN']['shortname']

    # map 'retracted' (solverd_samples) to 'included in datasets' (Biosamples)
    if 'retracted' in sample and sample['retracted']['id'] == 'Y': 
        new_biosamples_entry['included in datasets.resource'] = 'RD3'
        new_biosamples_entry['included in datasets.name'] = 'Retracted'

    # map 'batch' (solverd_samples) to 'included in datasets' (Biosamples)
    if 'batch' in sample:
        batches = []
        resources = []
        for batch in sample['batch'].split(","): # split on comma in the case of mulitple batches
            batches.append(batch)
            resources.append('RD3')
        new_biosamples_entry['included in datasets.resource'] = ",".join(map(str, resources))
        new_biosamples_entry['included in datasets.name'] = ",".join(map(str, batches))


    # map 'partOfRelease' (solverd_samples) to 'included in datasets' (Biosamples) 
    resources = []
    releases = []
    # gather the resources and releases (names)
    for release in sample['partOfRelease']:
        resources.append('RD3')
        releases.append(release['id'])
    # if this sample already contains the term included in datasets.resource (eg., when the sample also has a batch)
    # then append to this (string) list
    if 'included in datasets.resource' in new_biosamples_entry:
        new_biosamples_entry['included in datasets.resource'] += "," + ",".join(map(str, resources))
        new_biosamples_entry['included in datasets.name'] += "," + ",".join(map(str, releases))
    else: # else, make this new term
        new_biosamples_entry['included in datasets.resource'] = ",".join(map(str, resources))
        new_biosamples_entry['included in datasets.name'] = ",".join(map(str, releases))

    # map 'alternativeIdentifier' (solverd_samples) to 'alternate identifiers' (Biosamples)
    if 'alternativeIdentifier' in sample:
        new_biosamples_entry['alternate ids'] = sample['alternativeIdentifier']

    # map 'flag' (solverd_samples) naar 'failed quality control' (Biosamples)
    if 'flag' in sample:
        new_biosamples_entry['failed quality control'] = sample['flag']

    # map 'comments' (solverd_samples) to 'comments' (Biosamples)
    if 'comments' in sample:
        new_biosamples_entry['comments'] = sample['comments']

    # append the new biosample entry to the list
    emx2_biosamples.append(new_biosamples_entry)

# to check
pd.DataFrame(emx2_biosamples).to_csv('Biosamples.csv', index=False)

# save and upload the biosamples table
emx2.save_schema(table="Biosamples", data=emx2_biosamples)

##################################################
# Migrate the Experiments (solverd_labinfo) to the new model

# retrieve the protocol activity, sequencing runs, and sample preparations tables, so they can be truncated 
# seqRuns = emx2.get('Sequencing runs')
# sampPreps = emx2.get('Sample preparations')
# protAct = emx2.get('Protocol activity')

# # delete the tables because the upload does not replace, but adds to the exisiting tables.
# emx2.delete_records(table='Sequencing runs', data=seqRuns) 
# emx2.delete_records(table='Sample preparations', data=sampPreps)
# emx2.delete_records(table='Protocol activity', data=protAct)

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
    # data only contains 1 and 2, 1 is always single and 2 always paired.
    if 'library' in labinfo and labinfo['library']:
        layout = []
        for lib in labinfo['library']:
            if lib['id'] == "1":
                layout.append("SINGLE")
            elif lib['id'] == "2":
                layout.append("PAIRED")
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
            # does not map to any category in the ontology; print message
            elif labinfo['sequencer'] == 'Sequel IIe' or labinfo['sequencer'] == 'Illumina HiSeq 1500':
                print(f'The following sequencer is not present in the ontology: {labinfo['sequencer']}')
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

    # map 'partOfRelease' (solverd_labinfo) to 'included in datasets' (Sequencing runs and Sample preparations)
    partOfReleaseList = []
    resourcesList = []
    if 'partOfRelease' in labinfo:
        for release in labinfo['partOfRelease']:
            partOfReleaseList.append(release['id'])
            resourcesList.append('RD3')
    # map 'retracted' (solverd_labinfo) to 'included in datasets'
    # check if the individual needs to be retracted from the tables.
    if 'retracted' in labinfo and labinfo['retracted']['id'] == 'Y':
        # add the lists to the new individual entry
        resourcesList.append("RD3")
        partOfReleaseList.append('Retracted')
    # map 'includedInDatasets' (solverd_labinfo) to 'included in datasets' as well
    if 'includedInDatasets' in labinfo:
        for dataset in labinfo['includedInDatasets']:
            partOfReleaseList.append(dataset['id'])
            resourcesList.append('RD3')
    # append to Sequencing runs
    new_sequencing_runs_entry['included in datasets.resource'] = ','.join(
        resourcesList)
    new_sequencing_runs_entry['included in datasets.name'] = ','.join(
        map(str, partOfReleaseList))
    # append to Sample preparation
    new_sample_preparation_entry['included in datasets.resource'] = ','.join(
        resourcesList)
    new_sample_preparation_entry['included in datasets.name'] = ','.join(
        map(str, partOfReleaseList))

    
    # append the new entry to the sample preparation list
    if new_sample_preparation_entry: # if not empty
        emx2_sample_preparation.append(new_sample_preparation_entry)
    # # append the new entry to the sequencing runs list
    if new_sequencing_runs_entry: # if not empty
        emx2_sequencing_runs.append(new_sequencing_runs_entry)

# to check 
pd.DataFrame(emx2_sequencing_runs).to_csv('Sequencing runs.csv', index=False)
pd.DataFrame(emx2_sample_preparation).to_csv('Sample preparations.csv', index=False)

# save and upload the sample preparation table
emx2.save_schema(table="Sample preparations", data=emx2_sample_preparation)
# # save and upload the sequencing runs table
emx2.save_schema(table="Sequencing runs", data=emx2_sequencing_runs)

##################################################
# Migrate table Files (solverd_files) to the new model

emx2_files = []
for file in files: # loop through the files
    new_files_entry = {}

    # map 'EGA' (solverd_files) to 'alterntive ids' (Files)
    if 'EGA' in file:
        new_files_entry['alternative ids'] = file['EGA']
    
    # append the new entry to the files list
    if new_files_entry:
        emx2_files.append(new_files_entry)

# to check
pd.DataFrame(emx2_files).to_csv('Files.csv', index=False)

# save and upload the files table
emx2.save_schema(table="Files", data=emx2_files)

# write to csv - not used anymore 
pd.DataFrame(emx2_individuals).to_csv('Individuals.csv', index=False)
pd.DataFrame(emx2_pedigree).to_csv('Pedigree.csv', index=False)
pd.DataFrame(emx2_pedigree_members).to_csv('Pedigree members.csv', index=False)
pd.DataFrame(emx2_individual_consent).to_csv(
    'Individual consent.csv', index=False)
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
subjects_info_df = pd.DataFrame(subjects_info)

if 'organisation' in subject and all([item for item in subject['organisation']]):
    print('This should be printed when there are organisations present')

print(subject['retracted'])

# look at a specific subject
subject_id=""
subject_id_2=""

subject = subjects_info_df.loc[subjects_df['subjectID'] == subject_id]
print(subject)
print(subject['ageOfOnset'])

subject = subjects_df.loc[subjects_df['subjectID'] == subject_id]
subject['sex1']['id']

subject_info_tmp = rd3.get('solverd_subjectinfo', q=f"subjectID=q={subject_id},subjectID=q={subject_id}")

match = [info for info in subject_info_tmp if 'ageOfOnset' in info and info['subjectID'] == subject_id]

for subj in subject_info_tmp:
    if 'ageOfOnset' in subj and subj['ageOfOnset']:
        print(subj['ageOfOnset']['label'])

samples_tmp = rd3.get('solverd_samples', q=f"belongsToSubject=={subject_id},belongsToSubject=={subject_id_2}")

for sample in samples_tmp:
    if 'alternativeIdentifier' in sample:
        print(sample['alternativeIdentifier'])
        #for b in sample['alternativeIdentifier']:
            #print(b)

samples_tmp = rd3.get('solverd_samples')

for sample in samples_tmp:
    if 'partOfRelease' in sample:
        for release in sample['partOfRelease']:
            print(release['name'])
        #tmp = sample['partOfRelease'].split(",")
        #print(len(tmp))
        #if len(tmp) != 1:
        #    print('true')

labinfos_all_df = pd.DataFrame(labinfos_all)
labinfos_all_df['experimentID'].isnull().any()

labinfos_all_df['sequencer']
for i in labinfos_all_df['sequencingCentre']:
    if not i:
        print("empty")
        print(i)

tmp = labinfos_all_df['sequencer'].dropna()
sequencers = []
for i in tmp:
    sequencers.append(i)

set(sequencers)

labinfos_df = pd.DataFrame(labinfos)
tmp = labinfos_df['sequencer'].dropna()
sequencers = []
for i in tmp:
    sequencers.append(i)

uniques = set(sequencer.rstrip() for sequencer in sequencers)
import re
pattern = re.compile(r'^DNBSEQ-\w{1}\d+$')
filtered_uniques = [item for item in uniques if not pattern.match(item)]
filtered_uniques


samples_all = rd3.get('solverd_samples', batch_size=10000)
samples_all_df = pd.DataFrame(samples_all)

tmp = samples_all_df['tissueType'].dropna()
values = []
for i in tmp:
    #print(i)
    #for x in i:
    values.append(i['id'])

set(values)

import re

# Example list
my_list = ['goal to get', 'goal-to get', 'target one', 'goal after', 'goal-to win']

# Regex pattern to get the first word (before space or dash)
pattern = re.compile(r'^.*?(?=-)|^\S+')

# Loop through each element in the list and apply the regex
for item in my_list:
    match = pattern.match(item)
    if match:
        first_word = match.group()
        print(first_word)
