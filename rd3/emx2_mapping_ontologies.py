"""Migrate RD3 Ontologies to EMX2

For the migration of data from RD3 to EMX2, you will need to migrate the
ontologies and reference datasets first. For values that cannot be
recoded (i.e., diagnoses, HPO terms, etc.), it is better to preserve
the original dataset. Use this script to compile RD3 (EMX1) datasets and
reshape them into the new portal data model.

This includes preparing data for the following portal tables.

- Resources
- Contacts
- Organisations
- Diseases
- Phenotypes

"""

from os import environ
from dotenv import load_dotenv
import molgenis.client
import pandas as pd
from molgenis_emx2_pyclient import Client
load_dotenv()

# connect to the environment and log in
rd3 = molgenis.client.Session(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

emx2 = Client(
    'http://localhost:8080/',
    schema='rd3',
    token=environ['MOLGENIS_EMX2_TOKEN']
)

# ///////////////////////////////////////////////////////////////////////////////

# Manually create meta datasets

# create generic resource for RD3 datasets
# this entry is used to group all records in the RD3 dataset
resources_df = pd.DataFrame([{
    'id': 'RD3',
    'name': 'Solve-RD RD3',
    'type': 'Other type'
}])

emx2.save_schema(table='Resources', data=resources_df)

# Contacts
# In RD3, we used a generic account for database management. We will create
# that here.
contacts_df = pd.DataFrame([
    {
        'resource': 'RD3',
        'role description': 'RD3 bot',
        'first name': 'rd3-bot',
        'last name': 'rd3-bot',
        'statement of consent personal data': False,
        'statement of consent email': False
    }
])

emx2.save_schema(table='Contacts', data=contacts_df)


# ///////////////////////////////////////////////////////////////////////////////

# Migrate organisations
# In RD3, organisations are formatted as "<Organisation_shortname>-<data submitter>"
# It is better to preserve these values rather than mapping them to new values.

# retrieve relevant organisation information from RD3
orgs = rd3.get('solverd_info_organisations')
orgs_df = pd.DataFrame(orgs)[["value", "description"]]

orgs_df = orgs_df.rename(columns={
    'value': 'id',
    'description': 'name'
})

# add link to resource (required in EMX2)
orgs_df['resource'] = resources_df['id'][0]

emx2.save_schema(table="Organisations", data=orgs_df)

# ///////////////////////////////////////////////////////////////////////////////

# Migrate Disease ontology
# We want to preserve the RD3 data rather than recoding values. Therefore, we
# will only import the diseases that were registered in RD3 and that aren't in
# the EMX2 Diseases ontology

# rerieve existing ontology
emx2_diseases = emx2.get(table='Diseases', as_df=True)


# retrieve RD3 Disease ontology to join with diseases used in RD3
rd3_diseases = rd3.get('solverd_lookups_disease', batch_size=10000)
rd3_diseases_df = pd.DataFrame(rd3_diseases, dtype='string')[
    ["id", "label", "ontology", "uri"]]


# retrieve diseases registered in RD3 subjects and flatten
subject_diseases = rd3.get(
    'solverd_subjects',
    attributes='disease',
    batch_size=10000
)

subject_diseases_reduced = [
    row for row in subject_diseases
    if len(row['disease']) > 0
]

diseases_used = []
for row in subject_diseases_reduced:
    for disease in row['disease']:
        diseases_used.append(disease)

# merge the reste of the ontology data to diseases used
disease_df = pd.DataFrame(diseases_used, dtype='string')[['id', 'label']]
disease_df = disease_df.drop_duplicates(subset=['id'])

disease_df = pd.merge(disease_df, rd3_diseases_df, on=["id", "id"], how="left")
del disease_df['label_y']
disease_df = disease_df.rename(columns={
    "label_x": "name",
    'id': 'code',
    'uri': 'ontologyTermURI',
    'ontology': 'codesystem'
})

# check diseases to import to identify unknown terms
disease_df['is_new'] = disease_df['name'].map(
    lambda value: value not in emx2_diseases['name'].to_list()
)

diseases_to_import = disease_df[disease_df['is_new']]

emx2.save_schema(table="Diseases", data=diseases_to_import)


# ///////////////////////////////////////////////////////////////////////////////

# Migrate Phenotypes
# Like the disease ontologies, we will preserve the RD3 data where possible and
# import missing terms in the EMX2 ontology rather than mapping RD3 values.

# retrieve existing HPO ontology
emx2_hpo = emx2.get(table='Phenotypes', as_df=True)


# retrieve the RD3 EMX1 ontology
rd3_hpo = rd3.get('solverd_lookups_phenotype', batch_size=10000)
rd3_hpo_df = pd.DataFrame(
    rd3_hpo, dtype='string'
)[['id', 'label', 'description', 'uri']]


subject_hpo = rd3.get(
    'solverd_subjects',
    attributes='phenotype,hasNotPhenotype',
    batch_size=10000
)

hpos_used = []
for subject in subject_hpo:
    if subject.get('phenotype'):
        for hpo in subject['phenotype']:
            hpos_used.append(hpo)
    if subject.get('hasNotPhenotype'):
        for hpo in subject['hasNotPhenotype']:
            hpos_used.append(hpo)

hpos_used_df = pd.DataFrame(hpos_used)[['id', 'label']]
hpos_used_df_reduced = hpos_used_df.drop_duplicates(subset=['id'])

hpos_merged_df = pd.merge(
    hpos_used_df_reduced,
    rd3_hpo_df,
    on=["id", "id"],
    how="left"
)

del hpos_merged_df['label_y']
hpos_merged_df = hpos_merged_df.rename(
    columns={
        "label_x": "name",
        "description": "definition",
        "uri": "ontologyTermURI",
        "id": "code",
    }
)

hpos_merged_df['codesystem'] = "HPO"

# identify HPO terms that aren't in EMX2 and import
hpos_merged_df['is_new'] = hpos_merged_df['name'].map(
    lambda value: value not in emx2_hpo['name'].to_list())
hpos_to_import = hpos_merged_df[hpos_merged_df['is_new']]


emx2.save_schema(table="Phenotypes", data=hpos_to_import)

# ///////////////////////////////////////////////////////////////////////////////

# Migrate data releases
# SolveRD freezes and patches are now restructured as "Datasets".

releases = rd3.get('solverd_info_datareleases')
releases_used = []
for release in releases:
    releases_used.append({
        'resource': 'RD3',
        'name': release.get('id'),
        'description': release.get('name'),
        'date created': release.get('date'),
        'created by': release['createdBy'][0]['id'] if release.get('createdBy') else None
    })

# add a retracted datasets
releases_used.append({
    'resource': 'RD3',
    'name': 'Retracted'
})

# Migrate batches to datasets as well
batches = rd3.get( # retrieve batches from RD3
    'solverd_samples',
    batch_size=10000,
    attributes= 'batch'
)

# collect all unique batches 
unique_batches = []
for batch in batches:
    if 'batch' in batch:
        # split and strip in the case of multiple batches (eg., 'BGI_1, BGI_3')
        batch_split = [batch.strip() for batch in batch['batch'].split(",")]
        for splitted_batch in batch_split:
            # check if the batch is already in the list 
            if splitted_batch not in unique_batches:
                # if not, add to the list
                unique_batches.append(splitted_batch)

# append each unique batch to the datasets 
for batch in unique_batches:
    releases_used.append({
        'resource': 'RD3',
        'name':batch
    })

# convert to df
releases_df = pd.DataFrame(releases_used)

# save schema
emx2.save_schema(table='Datasets', data=releases_df)

# ///////////////////////////////////////////////////////////////////////////////

# Migrate enrichment kit (capture)

# Gather the capture values of the Experiments table
labinfo = rd3.get('solverd_labinfo',
                  attributes='capture',
                  batch_size=10000)

# Convert to dataframe
labinfo_df = pd.DataFrame(labinfo)

# Get the new ontology 
emx2_seq_enrich_kit = emx2.get(table='SequencingEnrichmentKits', as_df=True)

# Gather the values used for capture in SolveRD
captures = []
for row in labinfo:
    if 'capture' in row:
        captures.append(row['capture'])

# Get the unique values 
unique_captures = list(set(captures))

# Gather only the values that are not already present in the ontology
unique_captures_to_add = [capture for capture in unique_captures if capture not in emx2_seq_enrich_kit]
unique_captures_df = pd.DataFrame({'name': unique_captures_to_add})

# merge the solveRD ontologies with the EMX2 ontologies
emx2_seq_enrich_kit_complete = pd.merge(emx2_seq_enrich_kit, unique_captures_df, on = ['name', 'name'], how='outer')
#tmp = pd.concat([emx2_seq_enrich_kit['name'], unique_captures_series])

# upload the new ontologies to emx2
emx2.save_schema(table="SequencingEnrichmentKits", data=emx2_seq_enrich_kit_complete)

# ///////////////////////////////////////////////////////////////////////////////

# Migrate tissue types not yet present in the ontology

# Gather the tissue types values of the samples table
tissueTypes = rd3.get('solverd_samples',
                  attributes='tissueType',
                  batch_size=10000)

# Convert to dataframe
tissueTypes_df = pd.DataFrame(tissueTypes)

# Get the ontology table
emx2_tissue_types = emx2.get(table='TissueType', as_df=True)

# Gather the values used for tissue type in SolveRD
types = []
for row in tissueTypes:
    if 'tissueType' in row:
        types.append(row['tissueType']['id'])

# Get the unique values 
unique_types = list(set(types))

# Gather only the values that are not already present in the ontology
unique_types_to_add = [tissue_type for tissue_type in unique_types if tissue_type not in emx2_tissue_types['name'].to_list()]
unique_types_df = pd.DataFrame({'name': unique_types_to_add})

# merge the solveRD ontologies with the EMX2 ontologies
emx2_tissue_types_complete = pd.merge(emx2_tissue_types, unique_types_df, on = ['name', 'name'], how='outer')

# upload the new ontology to emx2
emx2.save_schema(table="TissueType", data=emx2_tissue_types_complete)

# ///////////////////////////////////////////////////////////////////////////////

# Upload Gender at birth and Genotypic sex - these were empty.

# upload gender at birth ontology 
emx2.save_schema(table="Gender at birth", file='/Users/w.f.oudijk/Documents/RD3/molgenis-emx2/data/_ontologies/GenderAtBirth.csv')

# upload genotypic sex ontology
emx2.save_schema(table="Genotypic sex", file="/Users/w.f.oudijk/Documents/RD3/molgenis-emx2/data/_ontologies/GenotypicSex.csv")