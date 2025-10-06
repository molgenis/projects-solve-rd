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
- Datasets
- SequencingEnrichmentKits
- TissueType
- SequencingInstrumentModels
- AnatomicalLocation
- Gender at birth
- Genotypic sex

"""

from os import environ
from dotenv import load_dotenv
import molgenis.client
import pandas as pd
from molgenis_emx2_pyclient import Client
load_dotenv()
import re
import zipfile
from zipfile import ZipFile
import asyncio

class map_solveRD_ontologies:
    """This class adds to EMX2 RD3 ontologies with values necessary for solve-RD RD3"""
    def __init__(self):
        # connect to the environment and log in
        self.rd3 = molgenis.client.Session(environ['MOLGENIS_PROD_HOST'])
        self.rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

        self.schema = environ['EMX2_SCHEMA']
        # EMX2 server
        self.emx2 = Client(
            environ['EMX2_URL'],
            schema=self.schema,
            token=environ['EMX2_TOKEN']
        )

        # CatalogueOntologies on EMX2 - includes all ontology tables
        self.emx2_ontologies = Client(
            environ['EMX2_URL'],
            #schema=environ['EMX2_SCHEMA_ONTOLOGIES'],
            schema='CatalogueOntologies',
            token=environ['EMX2_TOKEN']
        )

        self.output_path = environ['OUTPUT_PATH_TO_CSVS']
        self.GitHub_HOST = environ['PATH_TO_RESC_ENDP_AG']


    def map_resources(self): 
        """This function maps the resources and the data releases used in solve-RD to EMX2 RD3, i.e.,
        freezes and patches are now structured as Resources."""
        # this entry is used to group all records in the RD3 dataset
        # disabled
        self.resources_df = pd.DataFrame([{
           'rdf type': 'http://www.w3.org/ns/dcat#DatasetSeries',
            'ldp membership relation': 'https://w3id.org/fdp/fdp-o#metadataCatalog',
            'id': 'EMX2 API',
            'name': 'EMX2 API',
            'type': 'Other type', 
            'cohort type': 'Other type',
            'clinical study type': 'Other', 
            'description': 'The purpose of this is to test EMX2 API calls',
            'keywords': 'Genomics',
            'start year': '2025',
            'status': 'Finalised',
            'number of participants': 5,
            'number of participants with samples': 5,
            'release type': 'Closed dataset' 
        }])

        # get resources from GitHub repo
        self.resources_df = pd.read_csv(f'{self.GitHub_HOST}/Resources.csv')

        releases = self.rd3.get('solverd_info_datareleases')
        releases_used = []
        for release in releases:
            releases_used.append({ 
                'id': release.get('id'),
                'name': release.get('name'),
                'type': 'Other type',
            })

        # add a retracted datasets
        releases_used.append({
            'id': 'Retracted',
            'name': 'Retracted',
            'type': 'Other type'
        })

        # Migrate batches to datasets as well
        batches = self.rd3.get( # retrieve batches from RD3
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
                'id': batch,
                'name':batch,
                'type': 'Other type'
            })

        # Migrate datasets from solve-rd as well
        datasets = self.rd3.get('solverd_info_datasets',
                        attributes='id') # retrieve info from lookup table
        # loop through the datasets and append to the list
        for dataset in datasets:
            releases_used.append({
                'id': dataset['id'],
                'name': dataset['id'],
                'type': 'Other type'
            })

        # convert to df
        self.resources_df = pd.concat([pd.DataFrame(releases_used), self.resources_df])

        self.resources_df['number of participants'] = self.resources_df['number of participants'].astype('Int64')
        self.resources_df['number of participants with samples'] = self.resources_df['number of participants with samples'].astype('Int64')
        self.resources_df['start year'] = self.resources_df['start year'].astype('Int64')
        
        # write to csv
        self.resources_df.to_csv(f'{self.output_path}Resources.csv', index=False) 

    def make_endpoint(self):
        """This function makes an endpoint necessary for the mapping"""
        # disabled
        endpoint = {'id':'main_fdp', 
                    'type': 'https://w3id.org/fdp/fdp-o#MetadataService,http://www.w3.org/ns/dcat#Resource,http://www.w3.org/ns/dcat#DataService,https://w3id.org/fdp/fdp-o#FAIRDataPoint',
                    'name': "MOLGENIS Fair Data Point",
                    'version':'v1.2',
                    'description':'MOLGENIS FDP Endpoint for the catalogue data model',
                    'publisher':'MOLGENIS',
                    'language':'https://www.loc.gov/standards/iso639-2/php/langcodes_name.php?iso_639_1=en',
                    'license':'https://www.gnu.org/licenses/lgpl-3.0.html#license-text',
                    'conformsTo':'https://specs.fairdatapoint.org/fdp-specs-v1.2.html',
                    'metadataCatalog':'EMX2 API',
                    'conformsToFdpSpec':'https://specs.fairdatapoint.org/fdp-specs-v1.2.html'}
        
        # get endpoints from GitHub repo
        endpoint = pd.read_csv(f'{self.GitHub_HOST}/Endpoint.csv')
        endpoint.to_csv(f'{self.output_path}Endpoint.csv', index=False)

        # pd.DataFrame([endpoint]).to_csv(f'{self.output_path}Endpoint.csv', index=False) # to check

    def make_agent(self): 
        """This function makes an agent 'molgenis'"""
        # disabled
        agent = {
            'name': 'MOLGENIS',
            'logo': 'https://molgenis.org/assets/img/logo_green.png',
            'url': 'https://molgenis.org/',
            'mbox': 'support@molgenis.org',
            'mg_draft': 'FALSE'
        }

        # get agents from GitHub repo
        agent = pd.read_csv(f'{self.GitHub_HOST}/Agent.csv')
        agent.to_csv(f'{self.output_pathexit()}Agent.csv', index=False)

        # pd.DataFrame([agent]).to_csv(f'{self.output_path}Agent.csv', index=False) # to check


    async def zip_and_upload(self):
        """This function zips the resources, endpoint and agent tables and uploads them to the server"""
        # get the data to be uploaded        
        self.map_resources()
        self.make_endpoint()
        self.make_agent()
        
        # make a name for the zipped folder
        zip_file_name=f'{self.output_path}archive.zip'
        # zip the data
        with ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as my_zip:
            my_zip.write(f'{self.output_path}Agent.csv', 'Agent.csv')
            my_zip.write(f'{self.output_path}Endpoint.csv', 'Endpoint.csv')
            my_zip.write(f'{self.output_path}Resources.csv', 'Resources.csv')
        # upload the zipped file with the molgenis schema and the molgenis members
        await self.emx2.upload_file(schema=self.schema, file_path=zip_file_name)

    def make_contact(self):
        """This function makes an contact for solve-RD RD3. In RD3, we used a generic account for database management. 
        We will create that here.
        """
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

        self.emx2.save_schema(table='Contacts', data=contacts_df)

    def map_organisations(self):
        """
        In RD3, organisations are formatted as "<Organisation_shortname>-<data submitter>"
        It is better to preserve these values rather than mapping them to new values.
        """

        # retrieve relevant organisation information from RD3
        orgs = self.rd3.get('solverd_info_organisations', attributes='value,description')

        organisations = []

        for org in orgs:
            organisations.append({
                'name': org['value']
            })

        # Migrate ERNs 
        ERNs = self.rd3.get('solverd_info_erns')
        # loop through the ERNs and append to the list
        for ERN_item in ERNs:
            organisations.append({
                'name': ERN_item['shortname'],
            })

        organisations_df = pd.DataFrame(organisations)

        self.emx2_ontologies.save_schema(table="Organisations", data=organisations_df)

    def map_diseases(self):
        """This function adds to the disease ontology to match the possible values of Solve-RD. 
        We want to preserve the RD3 data rather than recoding values. Therefore, we will only import 
        the diseases that were registered in RD3 and that aren't in the EMX2 Diseases ontology"""
        
        # rerieve existing ontology (EMX2)
        emx2_diseases = self.emx2_ontologies.get(table='Diseases', as_df=True)

        # retrieve RD3 Disease ontology (EMX1) to join with diseases used in RD3
        rd3_diseases = self.rd3.get('solverd_lookups_disease', batch_size=10000)
        rd3_diseases_df = pd.DataFrame(rd3_diseases, dtype='string')[
            ["id", "label", "ontology", "uri"]]

        # retrieve diseases registered in RD3 subjects and flatten
        subject_diseases = self.rd3.get(
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

        self.emx2_ontologies.save_schema(table="Diseases", data=diseases_to_import)

    def map_phenotypes(self):
        """This function adds to the phenotypes ontology to match the possible values of Solve-RD."""

        # retrieve existing HPO ontology
        emx2_hpo = self.emx2_ontologies.get(table='Phenotypes', as_df=True)

        # retrieve the RD3 EMX1 ontology
        rd3_hpo = self.rd3.get('solverd_lookups_phenotype', batch_size=10000)
        rd3_hpo_df = pd.DataFrame(
            rd3_hpo, dtype='string'
        )[['id', 'label', 'description', 'uri']]

        # retrieve the terms prevalent in the subjects table
        subject_hpo = self.rd3.get(
            'solverd_subjects',
            attributes='phenotype,hasNotPhenotype',
            batch_size=10000
        )

        # gather the terms in one list (unique)
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

        # merge the used terms with the EMX1 ontology
        hpos_merged_df = pd.merge(
            hpos_used_df_reduced,
            rd3_hpo_df,
            on=["id", "id"],
            how="left"
        )

        # delete unnecessary columns
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

        self.emx2_ontologies.save_schema(table="Phenotypes", data=hpos_to_import)


    def map_enrichment_kit(self): 
        """This function maps the enrichment kits ontology from solve-rd (EMX1) to EMX2 RD3"""
        # Gather the capture values of the Experiments table
        labinfo = self.rd3.get('solverd_labinfo',
                        attributes='capture',
                        batch_size=10000)

        # Get the new ontology 
        emx2_seq_enrich_kit = self.emx2_ontologies.get(table='Sequencing enrichment kits', as_df=True)

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

        # upload the new ontologies to emx2
        self.emx2_ontologies.save_schema(table="Sequencing enrichment kits", data=emx2_seq_enrich_kit_complete)

    def map_seq_instrument_models(self):
        """This function maps the sequencing instrument models used in solv-rd (EMX1) to EMX2 RD3"""
        # Gather the sequencing instrument models from RD3
        insModels = self.rd3.get('solverd_labinfo', attributes='sequencer', batch_size=10000)

        # Gather the emx2 sequencer ontologies
        emx2_sequencers = self.emx2_ontologies.get('Sequencing instrument models', as_df=True)

        # don't migrate all DNBSEQ sequencers
        pattern = re.compile(r'^DNBSEQ-\w{1}\d+$')
        sequencers_to_add = [] # initialize
        for insModel in insModels: # loop through the models used in solve-rd
            if ('sequencer' in insModel # check if not NA 
                and not pattern.match(insModel['sequencer']) # do not map the DNBSEQ sequencers
                and insModel['sequencer'] not in emx2_sequencers['name'] # check if sequencer is already present in ontology from EMX2
                and insModel['sequencer'] != "Sequel II"): # Sequel II is already present in ontology under different name (PacBio Sequel II)
                # gather the sequencers used in solve-RD, except for all exceptions in the if statement
                sequencers_to_add.append(insModel['sequencer'].rstrip()) 
                
        sequencers_to_add = list(set(sequencers_to_add)) # gather the unique sequencers

        # merge the solveRD sequencers with the emx2 ontology
        emx2_sequencers_complete = pd.merge(emx2_sequencers, 
                                            pd.DataFrame({'name': sequencers_to_add}), how='outer')

        # save and upload the ontology with the additional categories 
        self.emx2_ontologies.save_schema(table="Sequencing instrument models", data=emx2_sequencers_complete)

    def map_anatomical_locations(self):
        """This function maps the anatomical locations used in solve-RD to EMX2 RD3"""
        
        # Gather the anatomical locations from RD3
        anatLoc = self.rd3.get('solverd_lookups_anatomicallocation', batch_size=1000)

        anatLocs_used = []
        for row in anatLoc:
            anatLocs_used.append({
                'name': row['id'],
                'label': row['label'],
                'codesystem': row['ontology'],
                'ontologyTermURI': row['uri'],
                'code': int(row['id'])
            })

        anatLocs_used_df = pd.DataFrame(anatLocs_used)

        # Gather the emx2 anatomical locations ontology
        emx2_anatLoc = self.emx2_ontologies.get('Anatomical location', as_df=True)

        # merge the solveRD anatomical locations with the emx2 ontology
        emx2_anatomical_locations_complete = pd.merge(emx2_anatLoc, anatLocs_used_df, on = ['name', 'name'], how='outer')
        emx2_anatomical_locations_complete = pd.DataFrame(emx2_anatomical_locations_complete)

        # drop duplicate columns
        emx2_anatomical_locations_complete = emx2_anatomical_locations_complete.drop(['label_x', 'codesystem_x', 'ontologyTermURI_x', 'code_x'], axis=1)
        # rename to correspond to ontology column names
        emx2_anatomical_locations_complete = emx2_anatomical_locations_complete.rename(
            columns={
                "label_y": "label",
                "ontologyTermURI_y": "ontologyTermURI",
                "codesystem_y": "codesystem",
                "code_y": "code"
            }
        )

        # save and upload the complete ontology 
        self.emx2_ontologies.save_schema(table="Anatomical location", data=emx2_anatomical_locations_complete)

    def map_sex_values(self): 
        """This function adds 'U' and 'UD' sex values to the EMX2 ontology"""
        sex = self.rd3.get('solverd_lookups_sex')

        sex_emx2 = self.emx2_ontologies.get('Gender at birth')

        for gender in sex: 
            if gender['id'] in ['U', 'UD']: 
                sex_emx2.append({
                    'name': gender['id'],
                    'label': gender['label'],
                    'definition': gender['description']
                })
        # upload 
        self.emx2_ontologies.save_schema(table='Gender at birth', data=sex_emx2)

    def map_tissue_type(self): 
        """This function maps the tissue types used in solve-RD to EMX2 ontology"""
        tissue_types_rd3 = self.rd3.get('solverd_lookups_tissueType')
        tissue_types_emx2 = self.emx2_ontologies.get('Tissue type')

        for tissue_type in tissue_types_rd3:
            if tissue_type['id'] not in [tissue_type_emx2['name'] for tissue_type_emx2 in tissue_types_emx2]:
                tissue_types_emx2.append({'name': tissue_type['id']})

        self.emx2_ontologies.save_schema(table='Tissue type', data=tissue_types_emx2)

def main():
    """Run class"""
    map_class = map_solveRD_ontologies()
    asyncio.run(map_class.zip_and_upload())
    # only run the following functions if CatalogueOntologies is new 
    map_class.map_organisations()
    map_class.map_diseases()
    map_class.map_phenotypes()
    map_class.map_enrichment_kit()
    map_class.map_seq_instrument_models()
    map_class.map_anatomical_locations()
    map_class.map_sex_values()
    map_class.map_tissue_type()

if __name__ == "__main__":
    main()